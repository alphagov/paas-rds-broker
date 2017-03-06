package integration_aws_test

import (
	"fmt"
	"sort"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"

	uuid "github.com/satori/go.uuid"

	. "github.com/alphagov/paas-rds-broker/ci/helpers"
)

const (
	INSTANCE_CREATE_TIMEOUT = 30 * time.Minute
)

var _ = Describe("RDS Broker Daemon", func() {
	BeforeEach(func() {
	})

	AfterEach(func() {
	})

	It("should check the instance credentials", func() {
		Eventually(rdsBrokerSession, 30*time.Second).Should(gbytes.Say("credentials check has ended"))
	})

	var _ = Describe("Services", func() {
		It("returns the proper CatalogResponse", func() {
			var err error

			catalog, err := brokerAPIClient.GetCatalog()
			Expect(err).ToNot(HaveOccurred())

			sort.Sort(ByServiceID(catalog.Services))

			Expect(catalog.Services).To(HaveLen(3))
			service1 := catalog.Services[0]
			service2 := catalog.Services[1]
			service3 := catalog.Services[2]
			Expect(service1.ID).To(Equal("Service-1"))
			Expect(service2.ID).To(Equal("Service-2"))
			Expect(service3.ID).To(Equal("Service-3"))

			Expect(service1.ID).To(Equal("Service-1"))
			Expect(service1.Name).To(Equal("Service 1"))
			Expect(service1.Description).To(Equal("This is the Service 1"))
			Expect(service1.Bindable).To(BeTrue())
			Expect(service1.PlanUpdatable).To(BeTrue())
			Expect(service1.Plans).To(HaveLen(1))
			Expect(service1.Plans[0].ID).To(Equal("Plan-1"))
			Expect(service1.Plans[0].Name).To(Equal("Plan 1"))
			Expect(service1.Plans[0].Description).To(Equal("This is the Plan 1"))
		})
	})

	var _ = Describe("Instance Provision/Update/Deprovision", func() {
		var (
			instanceID string
			serviceID  string
			planID     string
			appGUID    string
			bindingID  string
		)

		BeforeEach(func() {
			instanceID = uuid.NewV4().String()
			serviceID = "Service-1"
			planID = "Plan-1"
			appGUID = uuid.NewV4().String()
			bindingID = uuid.NewV4().String()

			brokerAPIClient.AcceptsIncomplete = true

			code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("succeeded"))
		})

		AfterEach(func() {
			brokerAPIClient.AcceptsIncomplete = true
			code, operation, err := brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("gone"))
		})

		It("can bind to the created service", func() {
			resp, err := brokerAPIClient.DoBindRequest(instanceID, serviceID, planID, appGUID, bindingID)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(201))
		})
	})

	var _ = Describe("Final snapshot enable/disable", func() {
		var (
			instanceID string
			serviceID  string
			planID     string
			appGUID    string
			bindingID  string
		)

		BeforeEach(func() {
			instanceID = uuid.NewV4().String()
			serviceID = "Service-2"
			planID = "Plan-2"
			appGUID = uuid.NewV4().String()
			bindingID = uuid.NewV4().String()
			brokerAPIClient.AcceptsIncomplete = true
		})

		It("should create a final snapshot by default", func() {
			code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("succeeded"))

			code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("gone"))

			snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
			fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshots)
			Expect(err).NotTo(HaveOccurred())
			Expect(snapshots).Should(ContainSubstring(instanceID))

			snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
			fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not create a final snapshot when `skip_final_snapshot` is set at provision time", func() {
			code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, `{"skip_final_snapshot": "true"}`)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("succeeded"))

			code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("gone"))

			snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
			fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshots)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

			snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
			fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
			Expect(err).To(HaveOccurred())
		})

		It("should not create a final snapshot when `skip_final_snapshot` is set via update", func() {
			code, operation, err := brokerAPIClient.ProvisionInstance(instanceID, serviceID, planID, "{}")
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state := pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("succeeded"))

			code, operation, err = brokerAPIClient.UpdateInstance(instanceID, serviceID, planID, planID, `{"skip_final_snapshot": "true"}`)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("succeeded"))

			code, operation, err = brokerAPIClient.DeprovisionInstance(instanceID, serviceID, planID)
			Expect(err).ToNot(HaveOccurred())
			Expect(code).To(Equal(202))
			state = pollForOperationCompletion(instanceID, serviceID, planID, operation)
			Expect(state).To(Equal("gone"))

			snapshots, err := rdsClient.GetDBFinalSnapshots(instanceID)
			fmt.Fprintf(GinkgoWriter, "Final snapshots for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshots)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("DBSnapshotNotFound"))

			snapshotDeletionOutput, err := rdsClient.DeleteDBFinalSnapshot(instanceID)
			fmt.Fprintf(GinkgoWriter, "Snapshot deletion output for %s:\n", instanceID)
			fmt.Fprint(GinkgoWriter, snapshotDeletionOutput)
			Expect(err).To(HaveOccurred())
		})
	})

})

func pollForOperationCompletion(instanceID, serviceID, planID, operation string) string {
	var state string
	var err error

	fmt.Fprint(GinkgoWriter, "Polling for Instance Operation to complete")
	Eventually(
		func() string {
			fmt.Fprint(GinkgoWriter, ".")
			state, err = brokerAPIClient.GetLastOperationState(instanceID, serviceID, planID, operation)
			Expect(err).ToNot(HaveOccurred())
			return state
		},
		INSTANCE_CREATE_TIMEOUT,
		15*time.Second,
	).Should(
		SatisfyAny(
			Equal("succeeded"),
			Equal("failed"),
			Equal("gone"),
		),
	)

	fmt.Fprintf(GinkgoWriter, "done. Final state: %s.\n", state)
	return state
}
