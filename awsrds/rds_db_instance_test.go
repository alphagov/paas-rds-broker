package awsrds_test

import (
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/awsrds"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
)

var _ = Describe("RDS DB Instance", func() {
	var (
		region               string
		partition            string
		dbInstanceIdentifier string
		dbInstanceArn        string
		dbSnapshotArn        string

		awsSession *session.Session

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger

		rdsDBInstance RDSInstance
	)

	const account = "123456789012"

	BeforeEach(func() {
		region = "rds-region"
		partition = "rds-partition"
		dbInstanceIdentifier = "cf-instance-id"
		dbInstanceArn = "arn:" + partition + ":rds:rds-region:" + account + ":db:" + dbInstanceIdentifier
		dbSnapshotArn = "arn:" + partition + ":rds:rds-region:" + account + ":snapshot:" + dbInstanceIdentifier
	})

	JustBeforeEach(func() {
		awsSession = session.New(nil)

		rdssvc = rds.New(awsSession)

		logger = lager.NewLogger("rdsdbinstance_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		rdsDBInstance = NewRDSDBInstance(region, partition, rdssvc, logger)
	})

	var _ = Describe("Describe", func() {
		var (
			describeDBInstance *rds.DBInstance

			receivedDescribeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError          error
		)

		BeforeEach(func() {
			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceArn:        aws.String(dbInstanceArn),
				DBInstanceStatus:     aws.String("available"),
				Engine:               aws.String("test-engine"),
				EngineVersion:        aws.String("1.2.3"),
				DBName:               aws.String("test-dbname"),
				Endpoint: &rds.Endpoint{
					Address: aws.String("dbinstance-endpoint"),
					Port:    aws.Int64(3306),
				},
				MasterUsername:          aws.String("test-master-username"),
				AllocatedStorage:        aws.Int64(100),
				AutoMinorVersionUpgrade: aws.Bool(true),
				BackupRetentionPeriod:   aws.Int64(1),
				CopyTagsToSnapshot:      aws.Bool(true),
				MultiAZ:                 aws.Bool(true),
				PendingModifiedValues: &rds.PendingModifiedValues{
					DBInstanceClass: aws.String("new-instance-class"),
				},
				PubliclyAccessible: aws.Bool(true),
				StorageEncrypted:   aws.Bool(true),
			}
			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ListTagsForResource"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					receivedDescribeDBInstancesInput = r.Params.(*rds.DescribeDBInstancesInput)
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = []*rds.DBInstance{describeDBInstance}
					r.Error = describeDBInstanceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper DB Instance", func() {
			dbInstance, err := rdsDBInstance.Describe(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstance).To(Equal(describeDBInstance))
			Expect(aws.StringValue(receivedDescribeDBInstancesInput.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
		})

		It("returns error if the DB instance does not exist", func() {
			_, err := rdsDBInstance.Describe("unknown")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the expected error", func() {
				_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when describing the DB returns 404", func() {
			BeforeEach(func() {
				awsError := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))
				describeDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
			})

			It("returns the expected error", func() {
				_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
			})
		})
	})

	var _ = Describe("GetResourceTags", func() {
		var (
			listTags []*rds.Tag

			receivedListTagsForResourceInput *rds.ListTagsForResourceInput
			listTagsForResourceError         error
			listTagsForResourceCallCount     int
		)

		BeforeEach(func() {
			listTags = []*rds.Tag{
				{
					Key:   aws.String("key1"),
					Value: aws.String("value1"),
				},
				{
					Key:   aws.String("key2"),
					Value: aws.String("value2"),
				},
				{
					Key:   aws.String("key3"),
					Value: aws.String("value3"),
				},
			}
			listTagsForResourceError = nil
			listTagsForResourceCallCount = 0
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ListTagsForResource"))
				switch r.Operation.Name {
				case "ListTagsForResource":
					listTagsForResourceCallCount = listTagsForResourceCallCount + 1
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					receivedListTagsForResourceInput = r.Params.(*rds.ListTagsForResourceInput)
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = listTags
					r.Error = listTagsForResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the instance tags", func() {
			tags, err := rdsDBInstance.GetResourceTags(dbInstanceArn)
			Expect(err).ToNot(HaveOccurred())
			Expect(tags).To(Equal(listTags))

			Expect(aws.StringValue(receivedListTagsForResourceInput.ResourceName)).To(Equal(dbInstanceArn))
		})

		It("caches the tags from ListTagsForResource unless DescribeRefreshCacheOption is passed", func() {
			tags, err := rdsDBInstance.GetResourceTags(dbInstanceArn)
			Expect(err).ToNot(HaveOccurred())
			Expect(tags).To(Equal(listTags))

			tags, err = rdsDBInstance.GetResourceTags(dbInstanceArn)
			Expect(err).ToNot(HaveOccurred())
			Expect(tags).To(Equal(listTags))

			Expect(listTagsForResourceCallCount).To(Equal(1))

			tags, err = rdsDBInstance.GetResourceTags(dbInstanceArn, DescribeRefreshCacheOption)
			Expect(err).ToNot(HaveOccurred())
			Expect(tags).To(Equal(listTags))

			Expect(listTagsForResourceCallCount).To(Equal(2))
		})
	})

	var _ = Describe("GetTag", func() {
		var (
			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
			expectedTag              string = "true"
		)

		BeforeEach(func() {
			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceStatus:     aws.String("available"),
				Engine:               aws.String("test-engine"),
				EngineVersion:        aws.String("1.2.3"),
				DBName:               aws.String("test-dbname"),
				MasterUsername:       aws.String("test-master-username"),
				AllocatedStorage:     aws.Int64(100),
			}
			describeDBInstances = []*rds.DBInstance{describeDBInstance}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}
			describeDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = describeDBInstances
					r.Error = describeDBInstanceError

				case "ListTagsForResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))

					var (
						tagKey   string = "SkipFinalSnapshot"
						tagValue string = "true"
					)

					data := r.Data.(*rds.ListTagsForResourceOutput)

					data.TagList = []*rds.Tag{
						&rds.Tag{
							Key:   &tagKey,
							Value: &tagValue,
						},
					}
				default:
					Fail(fmt.Sprintf("Unexpected call to AWS RDS API: '%s'", r.Operation.Name))
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper Tag", func() {
			tagValue, err := rdsDBInstance.GetTag(dbInstanceIdentifier, "SkipFinalSnapshot")
			Expect(err).ToNot(HaveOccurred())
			Expect(tagValue).To(Equal(expectedTag))
		})
	})

	var _ = Describe("DescribeByTag", func() {
		var (
			db1, db2, db3             *rds.DBInstance
			db1Tags, db2Tags, db3Tags []*rds.Tag

			listTagsForResourceCallCount int
		)

		BeforeEach(func() {
			db1 = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier + "-1"),
				DBInstanceArn:        aws.String(dbInstanceArn + "-1"),
			}
			db1Tags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Broker Name"),
					Value: aws.String("mybroker"),
				},
			}
			db2 = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier + "-2"),
				DBInstanceArn:        aws.String(dbInstanceArn + "-2"),
			}
			db2Tags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Broker Name"),
					Value: aws.String("mybroker"),
				},
			}
			db3 = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier + "-3"),
				DBInstanceArn:        aws.String(dbInstanceArn + "-3"),
			}
			db3Tags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Broker Name"),
					Value: aws.String("otherbroker"),
				},
			}

			listTagsForResourceCallCount = 0
		})

		JustBeforeEach(func() {
			// Configure RDS api mock. 1 of the instances is not from our broker
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					Expect(r.Params).To(Equal(&rds.DescribeDBInstancesInput{}))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = []*rds.DBInstance{db1, db2, db3}
				case "ListTagsForResource":
					listTagsForResourceCallCount = listTagsForResourceCallCount + 1

					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))

					listTagsForResourceInput := r.Params.(*rds.ListTagsForResourceInput)
					gotARN := *listTagsForResourceInput.ResourceName
					expectedARN := fmt.Sprintf("arn:%s:rds:%s:%s:db:%s", partition, region, account, dbInstanceIdentifier)
					Expect(gotARN).To(HavePrefix(expectedARN))

					data := r.Data.(*rds.ListTagsForResourceOutput)
					switch gotARN {
					case expectedARN + "-1":
						data.TagList = db1Tags
					case expectedARN + "-2":
						data.TagList = db2Tags
					case expectedARN + "-3":
						data.TagList = db3Tags
					}
				default:
					Fail(fmt.Sprintf("Unexpected call to AWS RDS API: '%s'", r.Operation.Name))
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)

		})

		It("returns the expected DB Instances for mybroker", func() {
			dbInstanceDetailsList, err := rdsDBInstance.DescribeByTag("Broker Name", "mybroker")
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))
			Expect(dbInstanceDetailsList[0]).To(Equal(db1))
			Expect(dbInstanceDetailsList[1]).To(Equal(db2))
		})

		It("caches the tags from ListTagsForResource unless DescribeRefreshCacheOption is passed", func() {
			numberOfInstances := 3

			listTagsForResourceCallCount = 0
			dbInstanceDetailsList, err := rdsDBInstance.DescribeByTag("Broker Name", "mybroker")
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(numberOfInstances))

			listTagsForResourceCallCount = 0
			dbInstanceDetailsList, err = rdsDBInstance.DescribeByTag("Broker Name", "mybroker")
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(0))

			listTagsForResourceCallCount = 0
			dbInstanceDetailsList, err = rdsDBInstance.DescribeByTag("Broker Name", "mybroker", DescribeRefreshCacheOption)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(numberOfInstances))
		})
	})

	var _ = Describe("DescribeSnapshots", func() {
		var (
			receivedDescribeDBSnapshotsInput *rds.DescribeDBSnapshotsInput

			describeDBSnapshotsError error

			dbSnapshotOneDayOld   *rds.DBSnapshot
			dbSnapshotTwoDayOld   *rds.DBSnapshot
			dbSnapshotThreeDayOld *rds.DBSnapshot
		)

		BeforeEach(func() {
			dbSnapshotOneDayOld = &rds.DBSnapshot{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBSnapshotIdentifier: aws.String(dbInstanceIdentifier + "-1"),
				DBSnapshotArn:        aws.String(dbSnapshotArn + "-1"),
				SnapshotCreateTime:   aws.Time(time.Now().Add(-1 * 24 * time.Hour)),
			}
			dbSnapshotTwoDayOld = &rds.DBSnapshot{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBSnapshotIdentifier: aws.String(dbInstanceIdentifier + "-2"),
				DBSnapshotArn:        aws.String(dbSnapshotArn + "-2"),
				SnapshotCreateTime:   aws.Time(time.Now().Add(-2 * 24 * time.Hour)),
			}
			dbSnapshotThreeDayOld = &rds.DBSnapshot{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBSnapshotIdentifier: aws.String(dbInstanceIdentifier + "-3"),
				DBSnapshotArn:        aws.String(dbSnapshotArn + "-3"),
				SnapshotCreateTime:   aws.Time(time.Now().Add(-3 * 24 * time.Hour)),
			}
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBSnapshots|ListTagsForResource"))
				switch r.Operation.Name {
				case "DescribeDBSnapshots":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBSnapshotsInput{}))
					receivedDescribeDBSnapshotsInput = r.Params.(*rds.DescribeDBSnapshotsInput)
					data := r.Data.(*rds.DescribeDBSnapshotsOutput)
					data.DBSnapshots = []*rds.DBSnapshot{
						dbSnapshotThreeDayOld,
						dbSnapshotOneDayOld,
						dbSnapshotTwoDayOld,
					}

					r.Error = describeDBSnapshotsError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls the DescribeDBSnapshots endpoint and does not return error", func() {
			_, _ = rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
			_, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(aws.StringValue(receivedDescribeDBSnapshotsInput.DBInstanceIdentifier)).To(Equal(dbInstanceIdentifier))
		})

		It("returns the all the snapshots in order", func() {
			dbSnapshots, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbSnapshots).To(HaveLen(3))
			Expect(dbSnapshots).To(Equal(
				[]*rds.DBSnapshot{
					dbSnapshotOneDayOld,
					dbSnapshotTwoDayOld,
					dbSnapshotThreeDayOld,
				},
			))
		})

		Context("when describing the DB Instance fails", func() {
			BeforeEach(func() {
				describeDBSnapshotsError = awserr.New("code", "message", errors.New("operation failed"))
			})

			It("returns the proper AWS error", func() {
				_, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("code: message"))
			})
		})
	})

	var _ = Describe("Create", func() {
		var (
			createDBInstanceInput *rds.CreateDBInstanceInput

			receivedCreateDBInstanceInput *rds.CreateDBInstanceInput
			createDBInstanceError         error
		)

		BeforeEach(func() {
			createDBInstanceInput = &rds.CreateDBInstanceInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				Engine:                  aws.String("test-engine"),
				AllocatedStorage:        aws.Int64(100),
				AutoMinorVersionUpgrade: aws.Bool(true),
				AvailabilityZone:        aws.String("test-az"),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
				PubliclyAccessible:      aws.Bool(false),
				StorageEncrypted:        aws.Bool(false),
				BackupRetentionPeriod:   aws.Int64(0),
				Tags: []*rds.Tag{
					&rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				},
			}
			createDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.CreateDBInstanceInput{}))
				receivedCreateDBInstanceInput = r.Params.(*rds.CreateDBInstanceInput)
				r.Error = createDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls CreateDBInstance with the same value and does not return error", func() {
			err := rdsDBInstance.Create(createDBInstanceInput)
			Expect(receivedCreateDBInstanceInput).To(Equal(createDBInstanceInput))
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns the error when creating the DB Instance fails", func() {
			createDBInstanceError = errors.New("operation failed")
			err := rdsDBInstance.Create(createDBInstanceInput)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("operation failed"))
		})
	})

	var _ = Describe("Restore", func() {
		var (
			snapshotIdentifier string

			receivedRestoreDBInstanceInput *rds.RestoreDBInstanceFromDBSnapshotInput
			restoreDBInstanceError         error
		)

		BeforeEach(func() {
			snapshotIdentifier = "snapshot-guid"
			restoreDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("RestoreDBInstanceFromDBSnapshot"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.RestoreDBInstanceFromDBSnapshotInput{}))
				receivedRestoreDBInstanceInput = r.Params.(*rds.RestoreDBInstanceFromDBSnapshotInput)
				r.Error = restoreDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			restoreDBInstanceInput := &rds.RestoreDBInstanceFromDBSnapshotInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				DBSnapshotIdentifier:    aws.String(snapshotIdentifier),
				Engine:                  aws.String("test-engine"),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
				PubliclyAccessible:      aws.Bool(false),
			}
			err := rdsDBInstance.Restore(restoreDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedRestoreDBInstanceInput).To(Equal(restoreDBInstanceInput))
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				restoreDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				restoreDBInstanceInput := &rds.RestoreDBInstanceFromDBSnapshotInput{
					DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
					DBSnapshotIdentifier:    aws.String(snapshotIdentifier),
					Engine:                  aws.String("test-engine"),
					AutoMinorVersionUpgrade: aws.Bool(false),
					CopyTagsToSnapshot:      aws.Bool(false),
					MultiAZ:                 aws.Bool(false),
					PubliclyAccessible:      aws.Bool(false),
				}
				err := rdsDBInstance.Restore(restoreDBInstanceInput)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})
	})

	var _ = Describe("Modify", func() {
		var (
			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstanceError error

			modifyDBInstanceError error

			receivedModifyDBInstanceInput *rds.ModifyDBInstanceInput
		)

		BeforeEach(func() {
			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceArn:        aws.String(dbInstanceArn),
				DBInstanceStatus:     aws.String("available"),
				DBSubnetGroup: &rds.DBSubnetGroup{
					DBSubnetGroupName: aws.String("test-subnet-group"),
				},
				Engine:           aws.String("test-engine"),
				EngineVersion:    aws.String("1.2.3"),
				DBName:           aws.String("test-dbname"),
				MasterUsername:   aws.String("test-master-username"),
				AllocatedStorage: aws.Int64(100),
			}
			describeDBInstances = []*rds.DBInstance{describeDBInstance}

			describeDBInstanceError = nil

			modifyDBInstanceError = nil

		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ModifyDBInstance"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = describeDBInstances
					r.Error = describeDBInstanceError
				case "ModifyDBInstance":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ModifyDBInstanceInput{}))
					receivedModifyDBInstanceInput = r.Params.(*rds.ModifyDBInstanceInput)
					data := r.Data.(*rds.ModifyDBInstanceOutput)
					data.DBInstance = &rds.DBInstance{
						DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
						DBInstanceArn:        aws.String(dbInstanceArn),
						DBInstanceStatus:     aws.String("updated"),
					}

					r.Error = modifyDBInstanceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls the ModifyDBInstance and does not return error", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier:     aws.String(dbInstanceIdentifier),
				AllowMajorVersionUpgrade: aws.Bool(false),
			}

			updatedDBInstance, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput).To(Equal(modifyDBInstanceInput))
			Expect(aws.StringValue(updatedDBInstance.DBInstanceStatus)).To(Equal("updated"))
		})

		It("keeps EngineVersion if new major and minor version match", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				EngineVersion:        aws.String("1.2.1"),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput.EngineVersion).To(Equal(aws.String("1.2.3")))
		})

		It("sets EngineVersion if new major version differs", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				EngineVersion:        aws.String("2.2.1"),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput.EngineVersion).To(Equal(aws.String("2.2.1")))
		})

		It("sets EngineVersion if new minor version differs", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				EngineVersion:        aws.String("1.3.1"),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput.EngineVersion).To(Equal(aws.String("1.3.1")))
		})

		It("sets AllowMajorVersionUpgrade to true by default", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier:     aws.String(dbInstanceIdentifier),
				AllowMajorVersionUpgrade: nil,
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput.AllowMajorVersionUpgrade).To(Equal(aws.Bool(true)))
		})

		It("sets AllocatedStorage if new value is bigger", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier:     aws.String(dbInstanceIdentifier),
				AllocatedStorage:         aws.Int64(500),
				AllowMajorVersionUpgrade: aws.Bool(false),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput).To(Equal(modifyDBInstanceInput))
		})

		It("keeps AllocatedStorage if new value is lower", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				AllocatedStorage:     aws.Int64(50),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput).ToNot(Equal(modifyDBInstanceInput))
			Expect(receivedModifyDBInstanceInput.AllocatedStorage).To(BeNil())
		})

		It("does not update SubnetGroup if it is the same", func() {
			modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBSubnetGroupName:    aws.String("test-subnet-group"),
			}

			_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedModifyDBInstanceInput).ToNot(Equal(modifyDBInstanceInput))
			Expect(receivedModifyDBInstanceInput.DBSubnetGroupName).To(BeNil())
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				}

				_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when modifying the DB instance fails", func() {
			BeforeEach(func() {
				modifyDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				modifyDBInstanceInput := &rds.ModifyDBInstanceInput{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				}

				_, err := rdsDBInstance.Modify(modifyDBInstanceInput)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})
	})

	var _ = Describe("AddTagsToResource", func() {
		var (
			listTagsForResourceError       error
			receivedAddTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError         error
		)

		BeforeEach(func() {
			listTagsForResourceError = nil
			addTagsToResourceError = nil
			receivedAddTagsToResourceInput = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("AddTagsToResource|ListTagsForResource"))
				switch r.Operation.Name {
				case "AddTagsToResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.AddTagsToResourceInput{}))
					receivedAddTagsToResourceInput = r.Params.(*rds.AddTagsToResourceInput)
					r.Error = addTagsToResourceError
				case "ListTagsForResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = []*rds.Tag{
						{
							Key:   aws.String("atag"),
							Value: aws.String("foo"),
						},
					}
					r.Error = listTagsForResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls AddTagsToResource when it has new tags", func() {
			newTags := []*rds.Tag{
				{
					Key:   aws.String("newtag"),
					Value: aws.String("bar"),
				},
			}
			err := rdsDBInstance.AddTagsToResource(dbInstanceArn, newTags)
			Expect(err).ToNot(HaveOccurred())
			Expect(receivedAddTagsToResourceInput).ToNot(BeNil())
			Expect(receivedAddTagsToResourceInput.Tags).To(Equal(newTags))
			Expect(aws.StringValue(receivedAddTagsToResourceInput.ResourceName)).To(Equal(dbInstanceArn))
		})
	})

	var _ = Describe("Reboot", func() {
		var (
			rebootDBInstanceError error
		)

		BeforeEach(func() {
			rebootDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("RebootDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.RebootDBInstanceInput{}))
				params := r.Params.(*rds.RebootDBInstanceInput)
				Expect(params.DBInstanceIdentifier).To(Equal(aws.String(dbInstanceIdentifier)))
				r.Error = rebootDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Reboot(&rds.RebootDBInstanceInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			})
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when rebooting the DB instance fails", func() {
			BeforeEach(func() {
				rebootDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Reboot(&rds.RebootDBInstanceInput{
					DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))
					rebootDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Reboot(&rds.RebootDBInstanceInput{
						DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
					})
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("Delete", func() {
		var (
			skipFinalSnapshot         bool
			finalDBSnapshotIdentifier string

			deleteDBInstanceError error
		)

		BeforeEach(func() {
			skipFinalSnapshot = true
			finalDBSnapshotIdentifier = ""
			deleteDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.DeleteDBInstanceInput{}))
				params := r.Params.(*rds.DeleteDBInstanceInput)
				Expect(params.DBInstanceIdentifier).To(Equal(aws.String(dbInstanceIdentifier)))
				if finalDBSnapshotIdentifier != "" {
					Expect(*params.FinalDBSnapshotIdentifier).To(ContainSubstring(finalDBSnapshotIdentifier))
				} else {
					Expect(params.FinalDBSnapshotIdentifier).To(BeNil())
				}
				Expect(params.SkipFinalSnapshot).To(Equal(aws.Bool(skipFinalSnapshot)))
				r.Error = deleteDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when does not skip the final snapshot", func() {
			BeforeEach(func() {
				skipFinalSnapshot = false
				finalDBSnapshotIdentifier = dbInstanceIdentifier + "-final-snapshot"
			})

			It("returns the proper DB Instance", func() {
				err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when deleting the DB instance fails", func() {
			BeforeEach(func() {
				deleteDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))
					deleteDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Delete(dbInstanceIdentifier, skipFinalSnapshot)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("DeleteSnapshots", func() {
		var (
			describeDBSnapshotsInput       *rds.DescribeDBSnapshotsInput
			describeDBSnapshotsError       error
			describeDBSnapshotsRequestDone bool
			describeDBSnapshots            []*rds.DBSnapshot

			listTagsCnt              int
			listTagsARNs             []string
			listTags                 []map[string]string
			listTagsForResourceError []error

			deleteDBSnapshotCnt    int
			deleteDBSnapshotInputs []*rds.DeleteDBSnapshotInput
			deleteDBSnapshotErrors []error
		)

		BeforeEach(func() {
			describeDBSnapshotsInput = &rds.DescribeDBSnapshotsInput{
				SnapshotType: aws.String("manual"),
			}
			describeDBSnapshotsError = nil
			describeDBSnapshotsRequestDone = false
			describeDBSnapshots = []*rds.DBSnapshot{}

			listTagsCnt = 0
			listTagsARNs = []string{}
			listTags = []map[string]string{}
			listTagsForResourceError = []error{}

			deleteDBSnapshotCnt = 0
			deleteDBSnapshotInputs = []*rds.DeleteDBSnapshotInput{}
			deleteDBSnapshotErrors = []error{}
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBSnapshots|ListTagsForResource|DeleteDBSnapshot"))
				switch r.Operation.Name {
				case "DescribeDBSnapshots":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBSnapshotsInput{}))
					Expect(r.Params).To(Equal(describeDBSnapshotsInput))
					data := r.Data.(*rds.DescribeDBSnapshotsOutput)
					data.DBSnapshots = describeDBSnapshots
					r.Error = describeDBSnapshotsError
					describeDBSnapshotsRequestDone = true
				case "ListTagsForResource":
					Expect(len(listTagsARNs)).To(BeNumerically(">", listTagsCnt), "unexpected ListTagsForResource call")
					Expect(len(listTags)).To(BeNumerically(">", listTagsCnt), "unexpected ListTagsForResource call")

					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					input := r.Params.(*rds.ListTagsForResourceInput)
					Expect(aws.StringValue(input.ResourceName)).To(Equal(listTagsARNs[listTagsCnt]))
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = BuilRDSTags(listTags[listTagsCnt])
					if len(listTagsForResourceError) > listTagsCnt {
						r.Error = listTagsForResourceError[listTagsCnt]
					}
					listTagsCnt++
				case "DeleteDBSnapshot":
					Expect(len(deleteDBSnapshotInputs)).To(BeNumerically(">", deleteDBSnapshotCnt), "unexpected DeleteDBSnapshotInput call")

					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DeleteDBSnapshotInput{}))
					Expect(r.Params).To(Equal(deleteDBSnapshotInputs[deleteDBSnapshotCnt]))
					if len(deleteDBSnapshotErrors) > deleteDBSnapshotCnt {
						r.Error = deleteDBSnapshotErrors[deleteDBSnapshotCnt]
					}
					deleteDBSnapshotCnt++
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls the DescribeDBSnapshots endpoint", func() {
			rdsDBInstance.DeleteSnapshots("test-broker", 2)
			Expect(describeDBSnapshotsRequestDone).To(BeTrue())
		})

		It("does not return error", func() {
			err := rdsDBInstance.DeleteSnapshots("test-broker", 2)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when there is a list of snapshots", func() {
			var (
				dbSnapshotOneDayOld             *rds.DBSnapshot
				dbSnapshotTwoDayOld             *rds.DBSnapshot
				dbSnapshotThreeDayOld           *rds.DBSnapshot
				dbSnapshotFourDayOldOtherBroker *rds.DBSnapshot
			)
			BeforeEach(func() {
				// Build DescribeDBSnapshots mock response with 3 instances
				buildDBSnapshotAWSResponse := func(instanceID string, age time.Duration) *rds.DBSnapshot {
					instanceCreateTime := time.Now().Add(-age)
					return &rds.DBSnapshot{
						DBInstanceIdentifier: aws.String(instanceID),
						DBSnapshotIdentifier: aws.String(instanceID),
						DBSnapshotArn:        aws.String(dbSnapshotArn + instanceID),
						SnapshotCreateTime:   aws.Time(instanceCreateTime),
					}
				}

				dbSnapshotOneDayOld = buildDBSnapshotAWSResponse("snapshot-one", 1*24*time.Hour)
				dbSnapshotTwoDayOld = buildDBSnapshotAWSResponse("snapshot-two", 2*24*time.Hour)
				dbSnapshotThreeDayOld = buildDBSnapshotAWSResponse("snapshot-three", 3*24*time.Hour)
				dbSnapshotFourDayOldOtherBroker = buildDBSnapshotAWSResponse("snapshot-four", 4*24*time.Hour)

				describeDBSnapshots = []*rds.DBSnapshot{
					dbSnapshotThreeDayOld,
					dbSnapshotOneDayOld,
					dbSnapshotTwoDayOld,
					dbSnapshotFourDayOldOtherBroker,
				}

				listTagsARNs = []string{
					*dbSnapshotThreeDayOld.DBSnapshotArn,
					*dbSnapshotTwoDayOld.DBSnapshotArn,
					*dbSnapshotFourDayOldOtherBroker.DBSnapshotArn,
				}

				listTags = []map[string]string{
					{TagBrokerName: "test-broker"},
					{TagBrokerName: "test-broker"},
					{TagBrokerName: "other-broker"},
				}

				deleteDBSnapshotInputs = []*rds.DeleteDBSnapshotInput{
					{DBSnapshotIdentifier: dbSnapshotThreeDayOld.DBSnapshotIdentifier},
					{DBSnapshotIdentifier: dbSnapshotTwoDayOld.DBSnapshotIdentifier},
				}

			})

			It("deletes all snapshots older than 1 day which belongs to this broker", func() {
				err := rdsDBInstance.DeleteSnapshots("test-broker", 2)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when deleting a snapshot fails", func() {
				BeforeEach(func() {
					deleteDBSnapshotErrors = []error{awserr.New("code", "message", errors.New("operation failed"))}
				})

				It("returns the proper AWS error", func() {
					err := rdsDBInstance.DeleteSnapshots("test-broker", 2)
					Expect(err).To(MatchError("failed to delete snapshot-three: code: message\ncaused by: operation failed"))
				})
			})

			Context("when gettings the snapshot tags fails", func() {
				BeforeEach(func() {
					listTagsForResourceError = []error{awserr.New("code", "message", errors.New("operation failed"))}
				})

				It("returns the proper AWS error", func() {
					err := rdsDBInstance.DeleteSnapshots("test-broker", 2)
					Expect(err).To(MatchError("failed to list tags for snapshot-three: code: message"))
				})
			})

		})

		Context("when fetching the snapshots fails", func() {
			BeforeEach(func() {
				describeDBSnapshotsError = awserr.New("code", "message", errors.New("operation failed"))
			})

			It("returns the proper AWS error", func() {
				err := rdsDBInstance.DeleteSnapshots("test-broker", 2)
				Expect(err).To(MatchError("failed to fetch snapshot list from AWS API: code: message\ncaused by: operation failed"))
			})
		})

	})

	Describe("GetLatestMinorVersion", func() {
		var (
			engineVersions []*rds.DBEngineVersion
		)

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DescribeDBEngineVersions"))
				data := r.Data.(*rds.DescribeDBEngineVersionsOutput)
				data.DBEngineVersions = engineVersions
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		Context("When no versions are found", func() {
			BeforeEach(func() {
				engineVersions = []*rds.DBEngineVersion{}
			})

			It("returns an error", func() {
				_, err := rdsDBInstance.GetLatestMinorVersion("not-postgres", "5")
				Expect(err).To(MatchError("Did not find a single version for not-postgres/5"))
			})
		})

		Context("When many versions are found", func() {
			BeforeEach(func() {
				engineVersions = []*rds.DBEngineVersion{
					{Engine: aws.String("not-postgres")},
					{Engine: aws.String("definitely-not-postgres")},
				}
			})

			It("returns an error", func() {
				_, err := rdsDBInstance.GetLatestMinorVersion("not-postgres", "5")
				Expect(err).To(MatchError("Did not find a single version for not-postgres/5"))
			})
		})

		Context("When exactly one version is found", func() {
			Context("And there are no upgrade targets", func() {
				BeforeEach(func() {
					engineVersions = []*rds.DBEngineVersion{
						{
							Engine:             aws.String("not-postgres"),
							ValidUpgradeTarget: []*rds.UpgradeTarget{},
						},
					}
				})

				It("returns nil", func() {
					version, err := rdsDBInstance.GetLatestMinorVersion("not-postgres", "5")
					Expect(err).NotTo(HaveOccurred())
					Expect(version).To(BeNil())
				})
			})

			Context("And there are only major upgrade targets", func() {
				BeforeEach(func() {
					engineVersions = []*rds.DBEngineVersion{
						{
							Engine: aws.String("not-postgres"),
							ValidUpgradeTarget: []*rds.UpgradeTarget{
								{IsMajorVersionUpgrade: aws.Bool(true)},
								{IsMajorVersionUpgrade: aws.Bool(true)},
								{IsMajorVersionUpgrade: aws.Bool(true)},
							},
						},
					}
				})

				It("returns nil", func() {
					version, err := rdsDBInstance.GetLatestMinorVersion("not-postgres", "5")
					Expect(err).NotTo(HaveOccurred())
					Expect(version).To(BeNil())
				})
			})

			Context("And there are both major and minor upgrade targets", func() {
				BeforeEach(func() {
					engineVersions = []*rds.DBEngineVersion{
						{
							Engine: aws.String("not-postgres"),
							ValidUpgradeTarget: []*rds.UpgradeTarget{
								{IsMajorVersionUpgrade: aws.Bool(true)},
								{EngineVersion: aws.String("6"), IsMajorVersionUpgrade: aws.Bool(false)},
								{EngineVersion: aws.String("7"), IsMajorVersionUpgrade: aws.Bool(false)},
								{IsMajorVersionUpgrade: aws.Bool(true)},
							},
						},
					}
				})

				It("returns the last minor upgrade target", func() {
					version, err := rdsDBInstance.GetLatestMinorVersion("not-postgres", "5")
					Expect(err).NotTo(HaveOccurred())
					Expect(version).To(Equal(aws.String("7")))
				})
			})
		})
	})

})
