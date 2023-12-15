package tls_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
	"github.com/alphagov/paas-rds-broker/config"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/phayes/freeport"
)

var (
	rdsBrokerSession *gexec.Session
	brokerAPIClient  *BrokerAPIClient
	brokerName       string
	rdsBrokerPath    string
)

var _ = Describe("TLS Support", func() {
	BeforeEach(func() {
		var err error

		// Give a different Broker Name in each execution, to avoid conflicts
		brokerName = fmt.Sprintf(
			"%s-%s",
			"rdsbroker-integration-test",
			uuid.NewV4().String(),
		)
		rdsBrokerPath, err = gexec.Build("github.com/alphagov/paas-rds-broker")
		Expect(err).ShouldNot(HaveOccurred())

		jsonConfigFilename := "config.json"
		jsonFilePath, err := filepath.Abs(jsonConfigFilename)
		Expect(err).ShouldNot(HaveOccurred())

		jsonData, err := os.ReadFile(jsonFilePath)
		Expect(err).ShouldNot(HaveOccurred())

		var testConfig config.Config
		err = json.Unmarshal(jsonData, &testConfig)
		Expect(err).ShouldNot(HaveOccurred())

		rdsBrokerSession, brokerAPIClient, _ = startNewBroker(&testConfig, brokerName)
	})

	AfterEach(func() {
		rdsBrokerSession.Kill()
	})

	Describe("Services", func() {
		It("returns the proper CatalogResponse", func() {
			_, err := brokerAPIClient.GetCatalog()
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

func startNewBroker(rdsBrokerConfig *config.Config, brokerName string) (*gexec.Session, *BrokerAPIClient, *RDSClient) {
	configFile, err := os.CreateTemp("", "rds-broker")
	Expect(err).ToNot(HaveOccurred())
	defer os.Remove(configFile.Name())

	newRDSBrokerConfig := *rdsBrokerConfig
	// start the broker in a random port
	rdsBrokerPort := freeport.GetPort()
	newRDSBrokerConfig.Port = rdsBrokerPort

	newRDSBrokerConfig.RDSConfig.BrokerName = "tls_broker"

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	Expect(err).NotTo(HaveOccurred())

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(time.Hour),
		IsCA:        true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	Expect(err).NotTo(HaveOccurred())

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes})

	newRDSBrokerConfig.TLS = &config.TLSConfig{
		Certificate: string(certPEM),
		PrivateKey:  string(keyPEM),
	}

	configJSON, err := json.Marshal(&newRDSBrokerConfig)
	Expect(err).ToNot(HaveOccurred())
	Expect(os.WriteFile(configFile.Name(), configJSON, 0644)).To(Succeed())
	Expect(configFile.Close()).To(Succeed())

	command := exec.Command(rdsBrokerPath,
		fmt.Sprintf("-config=%s", configFile.Name()),
	)
	fmt.Printf("\n\n\n----------%s\n%s-----------\n\n", rdsBrokerPath, configFile.Name())
	rdsBrokerSession, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())

	// Wait for it to be listening
	Eventually(rdsBrokerSession, 10*time.Second).Should(And(
		gbytes.Say("rds-broker.start"),
		gbytes.Say(fmt.Sprintf(`{"address":"0.0.0.0:%d","host":"0.0.0.0","port":%d,"tls":true}`, rdsBrokerPort, rdsBrokerPort)),
	))

	rdsBrokerUrl := fmt.Sprintf("https://localhost:%d", rdsBrokerPort)

	brokerAPIClient := NewBrokerAPIClient(rdsBrokerUrl, rdsBrokerConfig.Username, rdsBrokerConfig.Password)
	rdsClient, err := NewRDSClient(rdsBrokerConfig.RDSConfig.Region, rdsBrokerConfig.RDSConfig.DBPrefix)

	Expect(err).ToNot(HaveOccurred())

	return rdsBrokerSession, brokerAPIClient, rdsClient
}
