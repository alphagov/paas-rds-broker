package acceptance_test

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/cloudfoundry-incubator/cf-test-helpers/cf"
	cfg "github.com/cloudfoundry-incubator/cf-test-helpers/config"
	"github.com/cloudfoundry-incubator/cf-test-helpers/generator"
	"github.com/cloudfoundry-incubator/cf-test-helpers/helpers"
	"github.com/kelseyhightower/envconfig"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

const (
	DB_CREATE_TIMEOUT = 30 * time.Minute
)

type Config struct {
	DefaultTimeout int `envconfig:"default_timeout" default:"30"`
	CFPushTimeout  int `envconfig:"cf_push_timeout" default:"120"`

	Service          string   `envconfig:"service" required:"true"`
	Plans            []string `envconfig:"plans" required:"true"`
	TestPlan         string   `envconfig:"test_plan" required:"true"`
	TestPlanSnapshot string   `envconfig:"test_plan_snapshot" required:"true"`
	Region           string   `envconfig:"region" required:"true"`
}

var (
	config         Config
	cfConfig       *cfg.Config
	httpClient     *http.Client
	defaultTimeout time.Duration
	cfPushTimeout  time.Duration
)

var _ = Describe("RDS broker", func() {
	err := envconfig.Process("", &config)
	Expect(err).NotTo(HaveOccurred())

	cfConfig = cfg.LoadConfig()

	defaultTimeout = time.Duration(config.DefaultTimeout) * time.Second
	cfPushTimeout = time.Duration(config.CFPushTimeout) * time.Second

	httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfConfig.SkipSSLValidation},
		},
	}

	It("should have registered the service", func() {
		plans := cf.Cf("marketplace").Wait(defaultTimeout)
		Expect(plans).To(Exit(0))
		Expect(plans).To(Say(config.Service))
	})

	It("has the expected plans available", func() {
		plans := cf.Cf("marketplace", "-s", config.Service).Wait(defaultTimeout)
		Expect(plans).To(Exit(0))
		for _, plan := range config.Plans {
			Expect(plans.Out.Contents()).To(ContainSubstring(plan))
		}
	})

	Context("creating a database instance", func() {
		// Avoid creating additional tests in this block because this setup and teardown is
		// slow (several minutes).

		var (
			appName         string
			dbInstanceName  string
			rdsInstanceName string
		)
		BeforeEach(func() {
			appName = generator.PrefixedRandomName("CATS", "APP")
			dbInstanceName = generator.PrefixedRandomName("test", "db")
			Expect(cf.Cf("create-service", config.Service, config.TestPlan, dbInstanceName).Wait(defaultTimeout)).To(Exit(0))

			pollForRDSCreationCompletion(dbInstanceName)

			rdsInstanceName = getRDSInstanceName(dbInstanceName)
			fmt.Fprintf(GinkgoWriter, "Created RDS instance: %s\n", rdsInstanceName)

			Expect(cf.Cf(
				"push", appName,
				"--no-start",
				"-b", cfConfig.GoBuildpackName,
				"-p", "../../example-apps/healthcheck",
				"-f", "../../example-apps/healthcheck/manifest.yml",
				"-d", cfConfig.AppsDomain,
			).Wait(cfPushTimeout)).To(Exit(0))

			Expect(cf.Cf("bind-service", appName, dbInstanceName).Wait(defaultTimeout)).To(Exit(0))

			Expect(cf.Cf("start", appName).Wait(cfPushTimeout)).To(Exit(0))
		})

		AfterEach(func() {
			cf.Cf("delete", appName, "-f").Wait(defaultTimeout)

			Expect(cf.Cf("delete-service", dbInstanceName, "-f").Wait(defaultTimeout)).To(Exit(0))

			// Poll until destruction is complete, otherwise the org cleanup (in AfterSuite) fails.
			pollForRDSDeletionCompletion(dbInstanceName)
		})

		It("can connect to the DB instance from the app", func() {
			By("Sending request to DB Healthcheck app")
			resp, err := httpClient.Get(helpers.AppUri(appName, "/db", cfConfig))
			Expect(err).NotTo(HaveOccurred())
			body, err := ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200), "Got %d response from healthcheck app. Response body:\n%s\n", resp.StatusCode, string(body))

			By("Sending request to DB Healthcheck app without TLS")
			resp, err = httpClient.Get(helpers.AppUri(appName, "/db?ssl=false", cfConfig))
			Expect(err).NotTo(HaveOccurred())
			body, err = ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).NotTo(Equal(200), "Got %d response from healthcheck app. Response body:\n%s\n", resp.StatusCode, string(body))
			Expect(body).To(MatchRegexp("no pg_hba.conf entry for .* SSL off"), "Connection without TLS did not report a TLS error")

			By("Testing permissions after unbind and rebind")
			resp, err = httpClient.Get(helpers.AppUri(appName, "/db/permissions-check?phase=setup", cfConfig))
			Expect(err).NotTo(HaveOccurred())
			body, err = ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200), "Got %d response setting up multi-user test table. Response body:\n%s\n", resp.StatusCode, string(body))

			Expect(cf.Cf("stop", appName).Wait(defaultTimeout)).To(Exit(0))
			Expect(cf.Cf("unbind-service", appName, dbInstanceName).Wait(defaultTimeout)).To(Exit(0))
			Expect(cf.Cf("bind-service", appName, dbInstanceName).Wait(defaultTimeout)).To(Exit(0))
			Expect(cf.Cf("start", appName).Wait(cfPushTimeout)).To(Exit(0))

			resp, err = httpClient.Get(helpers.AppUri(appName, "/db/permissions-check?phase=test", cfConfig))
			Expect(err).NotTo(HaveOccurred())
			body, err = ioutil.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200), "Got %d response testing multi-user permissions. Response body:\n%s\n", resp.StatusCode, string(body))
		})
	})

	Context("final snapshots", func() {

		var (
			dbInstanceName  string
			rdsInstanceName string
		)

		BeforeEach(func() {
			dbInstanceName = generator.PrefixedRandomName("test", "db")
			Expect(cf.Cf("create-service", config.Service, config.TestPlanSnapshot, dbInstanceName).Wait(defaultTimeout)).To(Exit(0))

			pollForRDSCreationCompletion(dbInstanceName)

			rdsInstanceName = getRDSInstanceName(dbInstanceName)
			fmt.Fprintf(GinkgoWriter, "Created RDS instance: %s\n", rdsInstanceName)
		})

		It("should create a final snapshot by default", func() {
			Expect(cf.Cf("delete-service", dbInstanceName, "-f").Wait(defaultTimeout)).To(Exit(0))

			// Poll until destruction is complete, and the snapshot will therefore have been created.
			pollForRDSDeletionCompletion(dbInstanceName)

			rdsClient, err := NewRDSClient(config.Region)
			Expect(err).NotTo(HaveOccurred())
			snapshots, err := rdsClient.GetDBFinalSnapshots(rdsInstanceName)
			fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", rdsInstanceName)
			fmt.Fprint(GinkgoWriter, snapshots)
			Expect(err).NotTo(HaveOccurred())
			Expect(snapshots).Should(ContainSubstring(rdsInstanceName))

			snapshotDeletionOutput, err := rdsClient.deleteDBFinalSnapshot(rdsInstanceName)
			fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", rdsInstanceName)
			fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not create a final snapshot when `skip_final_snapshot` is set to true", func() {
			Expect(cf.Cf("update-service", dbInstanceName, "-c", `{"skip_final_snapshot": "true"}`).Wait(defaultTimeout)).To(Exit(0))
			pollForRDSUpdateCompletion(dbInstanceName)
			Expect(cf.Cf("delete-service", dbInstanceName, "-f").Wait(defaultTimeout)).To(Exit(0))

			// Poll until destruction is complete, and the snapshot would therefore have been created.
			pollForRDSDeletionCompletion(dbInstanceName)

			rdsClient, err := NewRDSClient(config.Region)
			Expect(err).NotTo(HaveOccurred())
			snapshots, err := rdsClient.GetDBFinalSnapshots(rdsInstanceName)
			fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", rdsInstanceName)
			fmt.Fprint(GinkgoWriter, snapshots)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

			snapshotDeletionOutput, err := rdsClient.deleteDBFinalSnapshot(rdsInstanceName)
			fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", rdsInstanceName)
			fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
			Expect(err).To(HaveOccurred())
		})

	})
})

func pollForRDSCreationCompletion(dbInstanceName string) {
	fmt.Fprint(GinkgoWriter, "Polling for RDS creation to complete")
	Eventually(func() *Buffer {
		fmt.Fprint(GinkgoWriter, ".")
		command := quietCf("cf", "service", dbInstanceName).Wait(defaultTimeout)
		Expect(command).To(Exit(0))
		return command.Out
	}, DB_CREATE_TIMEOUT, 15*time.Second).Should(Say("create succeeded"))
	fmt.Fprint(GinkgoWriter, "done\n")
}

func pollForRDSDeletionCompletion(dbInstanceName string) {
	fmt.Fprint(GinkgoWriter, "Polling for RDS destruction to complete")
	Eventually(func() *Buffer {
		fmt.Fprint(GinkgoWriter, ".")
		command := quietCf("cf", "services").Wait(defaultTimeout)
		Expect(command).To(Exit(0))
		return command.Out
	}, DB_CREATE_TIMEOUT, 15*time.Second).ShouldNot(Say(dbInstanceName))
	fmt.Fprint(GinkgoWriter, "done\n")
}

func pollForRDSUpdateCompletion(dbInstanceName string) {
	fmt.Fprint(GinkgoWriter, "Polling for RDS update to complete")
	Eventually(func() *Buffer {
		fmt.Fprint(GinkgoWriter, ".")
		command := quietCf("cf", "service", dbInstanceName).Wait(defaultTimeout)
		Expect(command).To(Exit(0))
		return command.Out
	}, DB_CREATE_TIMEOUT, 15*time.Second).Should(Say("update succeeded"))
	fmt.Fprint(GinkgoWriter, "done\n")
}

func getRDSInstanceName(dbInstanceName string) string {
	serviceOutput := cf.Cf("service", dbInstanceName).Wait(defaultTimeout)
	Expect(serviceOutput).To(Exit(0))
	rxp, _ := regexp.Compile("rdsbroker-([a-z0-9-]+)")
	return string(rxp.Find(serviceOutput.Out.Contents()))
}

// quietCf is an equivelent of cf.Cf that doesn't send the output to
// GinkgoWriter. Used when you don't want the output, even in verbose mode (eg
// when polling the API)
func quietCf(program string, args ...string) *Session {
	command, err := Start(exec.Command(program, args...), nil, nil)
	Expect(err).NotTo(HaveOccurred())
	return command
}

type RDSClient struct {
	region string
	rdssvc *rds.RDS
}

func NewRDSClient(region string) (*RDSClient, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		fmt.Println("Failed to create AWS session,", err)
		return nil, err
	}

	rdssvc := rds.New(sess)
	return &RDSClient{
		region: region,
		rdssvc: rdssvc,
	}, nil
}

func (r *RDSClient) GetDBFinalSnapshots(ID string) (*rds.DescribeDBSnapshotsOutput, error) {
	params := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(ID + "-final-snapshot"),
	}

	resp, err := r.rdssvc.DescribeDBSnapshots(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *RDSClient) deleteDBFinalSnapshot(ID string) (*rds.DeleteDBSnapshotOutput, error) {
	params := &rds.DeleteDBSnapshotInput{
		DBSnapshotIdentifier: aws.String(ID + "-final-snapshot"),
	}

	resp, err := r.rdssvc.DeleteDBSnapshot(params)

	if err != nil {
		return nil, err
	}
	return resp, nil
}
