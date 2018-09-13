package awsrds_test

import (
	"errors"
	"fmt"
	"strings"
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
	"github.com/aws/aws-sdk-go/service/sts"
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

		rdsDBInstance          DBInstance
		getCallerIdentityError error
	)

	const account = "123456789012"

	BeforeEach(func() {
		region = "rds-region"
		partition = "rds-partition"
		dbInstanceIdentifier = "cf-instance-id"
		dbInstanceArn = "arn:" + partition + ":rds:rds-region:" + account + ":db:" + dbInstanceIdentifier
		dbSnapshotArn = "arn:" + partition + ":rds:rds-region:" + account + ":snapshot:" + dbInstanceIdentifier
		getCallerIdentityError = nil
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
			listTags                 map[string]string
			listTagsForResourceError error

			properDBInstanceDetails DBInstanceDetails

			describeDBInstance *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error

			listTagsForResourceCallCount int
		)

		BeforeEach(func() {
			listTags = map[string]string{}
			listTagsForResourceError = nil

			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				DBInstanceArn:           aws.String(dbInstanceArn),
				DBInstanceStatus:        aws.String("available"),
				Engine:                  aws.String("test-engine"),
				EngineVersion:           aws.String("1.2.3"),
				DBName:                  aws.String("test-dbname"),
				MasterUsername:          aws.String("test-master-username"),
				AllocatedStorage:        aws.Int64(100),
				AutoMinorVersionUpgrade: aws.Bool(true),
				BackupRetentionPeriod:   aws.Int64(1),
				CopyTagsToSnapshot:      aws.Bool(true),
				MultiAZ:                 aws.Bool(true),
				PubliclyAccessible:      aws.Bool(true),
				StorageEncrypted:        aws.Bool(true),
			}
			describeDBInstancesInput = &rds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}
			describeDBInstanceError = nil

			listTagsForResourceCallCount = 0
		})

		JustBeforeEach(func() {
			properDBInstanceDetails = DBInstanceDetails{
				Identifier:       dbInstanceIdentifier,
				Arn:              dbInstanceArn,
				Status:           "available",
				Engine:           "test-engine",
				EngineVersion:    "1.2.3",
				DBName:           "test-dbname",
				MasterUsername:   "test-master-username",
				AllocatedStorage: int64(100),
				Tags:             listTags,
				AutoMinorVersionUpgrade: true,
				BackupRetentionPeriod:   int64(1),
				CopyTagsToSnapshot:      true,
				MultiAZ:                 true,
				PubliclyAccessible:      true,
				StorageEncrypted:        true,
			}

			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ListTagsForResource"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = []*rds.DBInstance{describeDBInstance}
					r.Error = describeDBInstanceError
				case "ListTagsForResource":
					listTagsForResourceCallCount = listTagsForResourceCallCount + 1
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					input := r.Params.(*rds.ListTagsForResourceInput)
					snapshotArnRegex := "arn:.*:rds:.*:.*:db:" + aws.StringValue(describeDBInstancesInput.DBInstanceIdentifier)
					Expect(aws.StringValue(input.ResourceName)).To(MatchRegexp(snapshotArnRegex))
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = BuilRDSTags(listTags)
					r.Error = listTagsForResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("returns the proper DB Instance", func() {
			dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
		})

		Context("when RDS DB Instance has some tags", func() {
			BeforeEach(func() {
				listTags = map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}
			})
			It("returns the proper DB Instance with the tags", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
			})

			It("caches the tags from ListTagsForResource unless DescribeRefreshCacheOption is passed", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))

				dbInstanceDetails, err = rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))

				Expect(listTagsForResourceCallCount).To(Equal(1))

				dbInstanceDetails, err = rdsDBInstance.Describe(dbInstanceIdentifier, DescribeRefreshCacheOption)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))

				Expect(listTagsForResourceCallCount).To(Equal(2))
			})
		})

		Context("when RDS DB Instance has an Endpoint", func() {
			JustBeforeEach(func() {
				describeDBInstance.Endpoint = &rds.Endpoint{
					Address: aws.String("dbinstance-endpoint"),
					Port:    aws.Int64(3306),
				}
				properDBInstanceDetails.Address = "dbinstance-endpoint"
				properDBInstanceDetails.Port = int64(3306)
			})

			It("returns the proper DB Instance", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
			})
		})

		Context("when RDS DB Instance has pending modifications", func() {
			JustBeforeEach(func() {
				describeDBInstance.PendingModifiedValues = &rds.PendingModifiedValues{
					DBInstanceClass: aws.String("new-instance-class"),
				}
				properDBInstanceDetails.PendingModifications = true
			})

			It("returns the proper DB Instance", func() {
				dbInstanceDetails, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbInstanceDetails).To(Equal(properDBInstanceDetails))
			})
		})

		Context("when the DB instance does not exists", func() {
			JustBeforeEach(func() {
				describeDBInstancesInput = &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("unknown"),
				}
			})

			It("returns the proper error", func() {
				_, err := rdsDBInstance.Describe("unknown")
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					describeDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					describeDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					_, err := rdsDBInstance.Describe(dbInstanceIdentifier)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(ErrDBInstanceDoesNotExist))
				})
			})
		})
	})

	var _ = Describe("GetTag", func() {
		var (
			properDBInstanceDetails DBInstanceDetails

			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error
			expectedTag              string = "true"
		)

		BeforeEach(func() {
			properDBInstanceDetails = DBInstanceDetails{
				Identifier:       dbInstanceIdentifier,
				Status:           "available",
				Engine:           "test-engine",
				EngineVersion:    "1.2.3",
				DBName:           "test-dbname",
				MasterUsername:   "test-master-username",
				AllocatedStorage: int64(100),
				Tags: map[string]string{
					"SkipFinalSnapshot": "true",
				},
			}

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
			expectedDBInstanceDetails []*DBInstanceDetails

			describeDBInstances []*rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput

			describeDBInstanceError error

			listTagsForResourceCallCount int
		)

		BeforeEach(func() {
			// Build DescribeDBInstances mock response with 3 instances
			buildDBInstanceAWSResponse := func(id, suffix string) *rds.DBInstance {
				return &rds.DBInstance{
					DBInstanceIdentifier: aws.String(id + suffix),
					DBInstanceArn:        aws.String(dbInstanceArn + suffix),
					DBInstanceStatus:     aws.String("available"),
					Engine:               aws.String("test-engine"),
					EngineVersion:        aws.String("1.2.3"),
					DBName:               aws.String("test-dbname" + suffix),
					MasterUsername:       aws.String("test-master-username" + suffix),
					AllocatedStorage:     aws.Int64(100),
				}
			}
			describeDBInstances = []*rds.DBInstance{
				buildDBInstanceAWSResponse(dbInstanceIdentifier, "-1"),
				buildDBInstanceAWSResponse(dbInstanceIdentifier, "-2"),
				buildDBInstanceAWSResponse(dbInstanceIdentifier, "-3"),
			}

			describeDBInstancesInput = &rds.DescribeDBInstancesInput{}
			describeDBInstanceError = nil

			// Build expected DB instances from DescribeByTag with only 2 instances
			buildExpectedDBInstanceDetails := func(id, suffix, brokerName string) *DBInstanceDetails {
				return &DBInstanceDetails{
					Identifier:       id + suffix,
					Arn:              dbInstanceArn + suffix,
					Status:           "available",
					Engine:           "test-engine",
					EngineVersion:    "1.2.3",
					DBName:           "test-dbname" + suffix,
					MasterUsername:   "test-master-username" + suffix,
					AllocatedStorage: int64(100),
					Tags: map[string]string{
						"Broker Name": brokerName,
					},
				}
			}
			expectedDBInstanceDetails = []*DBInstanceDetails{
				buildExpectedDBInstanceDetails(dbInstanceIdentifier, "-1", "mybroker"),
				buildExpectedDBInstanceDetails(dbInstanceIdentifier, "-2", "mybroker"),
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
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = describeDBInstances
					r.Error = describeDBInstanceError
				case "ListTagsForResource":
					listTagsForResourceCallCount = listTagsForResourceCallCount + 1

					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))

					listTagsForResourceInput := r.Params.(*rds.ListTagsForResourceInput)
					gotARN := *listTagsForResourceInput.ResourceName
					expectedARN := fmt.Sprintf("arn:%s:rds:%s:%s:db:%s", partition, region, account, dbInstanceIdentifier)
					Expect(gotARN).To(HavePrefix(expectedARN))

					data := r.Data.(*rds.ListTagsForResourceOutput)

					brokerName := "mybroker"
					if strings.HasSuffix(gotARN, "-3") {
						brokerName = "otherbroker"
					}
					data.TagList = []*rds.Tag{
						&rds.Tag{
							Key:   aws.String("Broker Name"),
							Value: aws.String(brokerName),
						},
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
		})

		It("caches the tags from ListTagsForResource unless DescribeRefreshCacheOption is passed", func() {
			dbInstanceDetailsList, err := rdsDBInstance.DescribeByTag("Broker Name", "mybroker")
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(len(describeDBInstances)))

			dbInstanceDetailsList, err = rdsDBInstance.DescribeByTag("Broker Name", "mybroker")
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(len(describeDBInstances)))

			previousCount := listTagsForResourceCallCount

			dbInstanceDetailsList, err = rdsDBInstance.DescribeByTag("Broker Name", "mybroker", DescribeRefreshCacheOption)
			Expect(err).ToNot(HaveOccurred())
			Expect(dbInstanceDetailsList).To(HaveLen(2))

			Expect(listTagsForResourceCallCount).To(Equal(previousCount + len(describeDBInstances)))
		})
	})

	var _ = Describe("DescribeSnapshots", func() {
		var (
			describeDBSnapshotsInput       *rds.DescribeDBSnapshotsInput
			describeDBSnapshotsError       error
			describeDBSnapshotsRequestDone bool
			describeDBSnapshots            []*rds.DBSnapshot

			listTagsForResourceError error
			listTags                 map[string]string
		)

		BeforeEach(func() {
			describeDBSnapshotsInput = &rds.DescribeDBSnapshotsInput{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
			}
			describeDBSnapshotsError = nil
			describeDBSnapshotsRequestDone = false
			describeDBSnapshots = []*rds.DBSnapshot{}
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBSnapshots|ListTagsForResource"))
				switch r.Operation.Name {
				case "DescribeDBSnapshots":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBSnapshotsInput{}))
					Expect(r.Params).To(Equal(describeDBSnapshotsInput))
					data := r.Data.(*rds.DescribeDBSnapshotsOutput)
					data.DBSnapshots = describeDBSnapshots
					r.Error = describeDBSnapshotsError
					describeDBSnapshotsRequestDone = true
				case "ListTagsForResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					input := r.Params.(*rds.ListTagsForResourceInput)
					snapshotArnRegex := "arn:.*:rds:.*:.*:snapshot:" + aws.StringValue(describeDBSnapshotsInput.DBSnapshotIdentifier)
					Expect(aws.StringValue(input.ResourceName)).To(MatchRegexp(snapshotArnRegex))
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = BuilRDSTags(listTags)
					r.Error = listTagsForResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("calls the DescribeDBSnapshots endpoint", func() {
			_, _ = rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
			Expect(describeDBSnapshotsRequestDone).To(BeTrue())
		})

		It("does not return error", func() {
			_, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when there is a list of snapshots", func() {
			var (
				dbSnapshotOneDayOld   *rds.DBSnapshot
				dbSnapshotTwoDayOld   *rds.DBSnapshot
				dbSnapshotThreeDayOld *rds.DBSnapshot
			)
			BeforeEach(func() {
				listTags = map[string]string{
					"Owner":           "Cloud Foundry",
					"Created by":      "AWS RDS Service Broker",
					"Created at":      time.Now().Format(time.RFC822Z),
					"Service ID":      "Service-1",
					"Plan ID":         "Plan-1",
					"Organization ID": "organization-id",
					"Space ID":        "space-id",
				}

				// Build DescribeDBSnapshots mock response with 3 instances
				buildDBSnapshotAWSResponse := func(instanceID string, age time.Duration) *rds.DBSnapshot {
					instanceCreateTime := time.Now().Add(-age)
					suffix := instanceCreateTime.Format("-2006-01-02-15-04")
					return &rds.DBSnapshot{
						DBInstanceIdentifier: aws.String(instanceID),
						DBSnapshotIdentifier: aws.String(instanceID + suffix),
						DBSnapshotArn:        aws.String(dbSnapshotArn + suffix),
						SnapshotCreateTime:   aws.Time(instanceCreateTime),
					}
				}

				dbSnapshotOneDayOld = buildDBSnapshotAWSResponse(dbInstanceIdentifier, 1*24*time.Hour)
				dbSnapshotTwoDayOld = buildDBSnapshotAWSResponse(dbInstanceIdentifier, 2*24*time.Hour)
				dbSnapshotThreeDayOld = buildDBSnapshotAWSResponse(dbInstanceIdentifier, 3*24*time.Hour)

				describeDBSnapshots = []*rds.DBSnapshot{
					dbSnapshotThreeDayOld,
					dbSnapshotOneDayOld,
					dbSnapshotTwoDayOld,
				}
			})

			It("returns the all the snapshots in order", func() {
				dbSnapshotsDetails, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbSnapshotsDetails).To(HaveLen(3))
				Expect(dbSnapshotsDetails[0].Identifier).To(Equal(aws.StringValue(dbSnapshotOneDayOld.DBSnapshotIdentifier)))
				Expect(dbSnapshotsDetails[1].Identifier).To(Equal(aws.StringValue(dbSnapshotTwoDayOld.DBSnapshotIdentifier)))
				Expect(dbSnapshotsDetails[2].Identifier).To(Equal(aws.StringValue(dbSnapshotThreeDayOld.DBSnapshotIdentifier)))
			})

			It("returns the tags for all snapshots", func() {
				dbSnapshotsDetails, err := rdsDBInstance.DescribeSnapshots(dbInstanceIdentifier)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbSnapshotsDetails).To(HaveLen(3))
				Expect(dbSnapshotsDetails[0].Tags).To(Equal(listTags))
				Expect(dbSnapshotsDetails[1].Tags).To(Equal(listTags))
				Expect(dbSnapshotsDetails[2].Tags).To(Equal(listTags))
			})

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
			dbInstanceDetails DBInstanceDetails

			createDBInstanceInput *rds.CreateDBInstanceInput
			createDBInstanceError error
		)

		BeforeEach(func() {
			dbInstanceDetails = DBInstanceDetails{
				Engine: "test-engine",
			}

			createDBInstanceInput = &rds.CreateDBInstanceInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				Engine:                  aws.String("test-engine"),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
				PubliclyAccessible:      aws.Bool(false),
				StorageEncrypted:        aws.Bool(false),
				BackupRetentionPeriod:   aws.Int64(0),
			}
			createDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateDBInstance"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.CreateDBInstanceInput{}))
				Expect(r.Params).To(Equal(createDBInstanceInput))
				r.Error = createDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				dbInstanceDetails.AllocatedStorage = 100
				createDBInstanceInput.AllocatedStorage = aws.Int64(100)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				dbInstanceDetails.AutoMinorVersionUpgrade = true
				createDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				dbInstanceDetails.AvailabilityZone = "test-az"
				createDBInstanceInput.AvailabilityZone = aws.String("test-az")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbInstanceDetails.BackupRetentionPeriod = 7
				createDBInstanceInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CharacterSetName", func() {
			BeforeEach(func() {
				dbInstanceDetails.CharacterSetName = "test-characterset-name"
				createDBInstanceInput.CharacterSetName = aws.String("test-characterset-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				dbInstanceDetails.CopyTagsToSnapshot = true
				createDBInstanceInput.CopyTagsToSnapshot = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBInstanceClass", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBInstanceClass = "db.m3.small"
				createDBInstanceInput.DBInstanceClass = aws.String("db.m3.small")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBName = "test-dbname"
				createDBInstanceInput.DBName = aws.String("test-dbname")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBParameterGroupName = "test-db-parameter-group-name"
				createDBInstanceInput.DBParameterGroupName = aws.String("test-db-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSecurityGroups = []string{"test-db-security-group"}
				createDBInstanceInput.DBSecurityGroups = aws.StringSlice([]string{"test-db-security-group"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSubnetGroupName = "test-db-subnet-group-name"
				createDBInstanceInput.DBSubnetGroupName = aws.String("test-db-subnet-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				dbInstanceDetails.EngineVersion = "1.2.3"
				createDBInstanceInput.EngineVersion = aws.String("1.2.3")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has KmsKeyID", func() {
			BeforeEach(func() {
				dbInstanceDetails.KmsKeyID = "test-kms-key-id"
				createDBInstanceInput.KmsKeyId = aws.String("test-kms-key-id")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUsername", func() {
			BeforeEach(func() {
				dbInstanceDetails.MasterUsername = "test-master-username"
				createDBInstanceInput.MasterUsername = aws.String("test-master-username")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MasterUserPassword", func() {
			BeforeEach(func() {
				dbInstanceDetails.MasterUserPassword = "test-master-user-password"
				createDBInstanceInput.MasterUserPassword = aws.String("test-master-user-password")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				dbInstanceDetails.LicenseModel = "test-license-model"
				createDBInstanceInput.LicenseModel = aws.String("test-license-model")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				dbInstanceDetails.MultiAZ = true
				createDBInstanceInput.MultiAZ = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.OptionGroupName = "test-option-group-name"
				createDBInstanceInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				dbInstanceDetails.Port = 666
				createDBInstanceInput.Port = aws.Int64(666)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredBackupWindow = "test-preferred-backup-window"
				createDBInstanceInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				createDBInstanceInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				dbInstanceDetails.PubliclyAccessible = true
				createDBInstanceInput.PubliclyAccessible = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageEncrypted", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageEncrypted = true
				createDBInstanceInput.StorageEncrypted = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageType = "test-storage-type"
				createDBInstanceInput.StorageType = aws.String("test-storage-type")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				dbInstanceDetails.Iops = 1000
				createDBInstanceInput.Iops = aws.Int64(1000)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbInstanceDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				createDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbInstanceDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
				createDBInstanceInput.Tags = []*rds.Tag{
					&rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				}
			})

			It("does not return error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				createDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Create(dbInstanceIdentifier, dbInstanceDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Restore", func() {
		var (
			snapshotIdentifier string
			dbInstanceDetails  DBInstanceDetails

			restoreDBInstanceInput *rds.RestoreDBInstanceFromDBSnapshotInput
			restoreDBInstanceError error
		)

		BeforeEach(func() {
			snapshotIdentifier = "snapshot-guid"
			dbInstanceDetails = DBInstanceDetails{
				Engine: "test-engine",
			}

			restoreDBInstanceInput = &rds.RestoreDBInstanceFromDBSnapshotInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				DBSnapshotIdentifier:    aws.String(snapshotIdentifier),
				Engine:                  aws.String("test-engine"),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
				PubliclyAccessible:      aws.Bool(false),
			}
			restoreDBInstanceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("RestoreDBInstanceFromDBSnapshot"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.RestoreDBInstanceFromDBSnapshotInput{}))
				Expect(r.Params).To(Equal(restoreDBInstanceInput))
				r.Error = restoreDBInstanceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				dbInstanceDetails.AutoMinorVersionUpgrade = true
				restoreDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has AvailabilityZone", func() {
			BeforeEach(func() {
				dbInstanceDetails.AvailabilityZone = "test-az"
				restoreDBInstanceInput.AvailabilityZone = aws.String("test-az")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				dbInstanceDetails.CopyTagsToSnapshot = true
				restoreDBInstanceInput.CopyTagsToSnapshot = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBInstanceClass", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBInstanceClass = "db.m3.small"
				restoreDBInstanceInput.DBInstanceClass = aws.String("db.m3.small")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBName = "test-dbname"
				restoreDBInstanceInput.DBName = aws.String("test-dbname")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSubnetGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSubnetGroupName = "test-db-subnet-group-name"
				restoreDBInstanceInput.DBSubnetGroupName = aws.String("test-db-subnet-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has LicenseModel", func() {
			BeforeEach(func() {
				dbInstanceDetails.LicenseModel = "test-license-model"
				restoreDBInstanceInput.LicenseModel = aws.String("test-license-model")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				dbInstanceDetails.MultiAZ = true
				restoreDBInstanceInput.MultiAZ = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.OptionGroupName = "test-option-group-name"
				restoreDBInstanceInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Port", func() {
			BeforeEach(func() {
				dbInstanceDetails.Port = 666
				restoreDBInstanceInput.Port = aws.Int64(666)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PubliclyAccessible", func() {
			BeforeEach(func() {
				dbInstanceDetails.PubliclyAccessible = true
				restoreDBInstanceInput.PubliclyAccessible = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageType = "test-storage-type"
				restoreDBInstanceInput.StorageType = aws.String("test-storage-type")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				dbInstanceDetails.Iops = 1000
				restoreDBInstanceInput.Iops = aws.Int64(1000)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbInstanceDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
				restoreDBInstanceInput.Tags = []*rds.Tag{
					&rds.Tag{Key: aws.String("Owner"), Value: aws.String("Cloud Foundry")},
				}
			})

			It("does not return error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when creating the DB Instance fails", func() {
			BeforeEach(func() {
				restoreDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					restoreDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Restore(dbInstanceIdentifier, snapshotIdentifier, dbInstanceDetails)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Modify", func() {
		var (
			listTags                 map[string]string
			listTagsForResourceError error

			dbInstanceDetails DBInstanceDetails
			applyImmediately  bool

			describeDBInstances []*rds.DBInstance
			describeDBInstance  *rds.DBInstance

			describeDBInstancesInput *rds.DescribeDBInstancesInput
			describeDBInstanceError  error

			modifyDBInstanceInput *rds.ModifyDBInstanceInput
			modifyDBInstanceError error

			addTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError error

			getCallerIdentityInput *sts.GetCallerIdentityInput
			getCallerIdentityError error
		)

		BeforeEach(func() {
			listTags = map[string]string{}
			listTagsForResourceError = nil

			dbInstanceDetails = DBInstanceDetails{}
			applyImmediately = false

			describeDBInstance = &rds.DBInstance{
				DBInstanceIdentifier: aws.String(dbInstanceIdentifier),
				DBInstanceArn:        aws.String(dbInstanceArn),
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

			modifyDBInstanceInput = &rds.ModifyDBInstanceInput{
				DBInstanceIdentifier:    aws.String(dbInstanceIdentifier),
				ApplyImmediately:        aws.Bool(applyImmediately),
				AutoMinorVersionUpgrade: aws.Bool(false),
				CopyTagsToSnapshot:      aws.Bool(false),
				MultiAZ:                 aws.Bool(false),
			}
			modifyDBInstanceError = nil

			addTagsToResourceInput = &rds.AddTagsToResourceInput{
				ResourceName: aws.String("arn:" + partition + ":rds:rds-region:" + account + ":db:" + dbInstanceIdentifier),
				Tags: []*rds.Tag{
					&rds.Tag{
						Key:   aws.String("Owner"),
						Value: aws.String("Cloud Foundry"),
					},
				},
			}
			addTagsToResourceError = nil
			getCallerIdentityInput = &sts.GetCallerIdentityInput{}
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(MatchRegexp("DescribeDBInstances|ModifyDBInstance|AddTagsToResource|ListTagsForResource"))
				switch r.Operation.Name {
				case "DescribeDBInstances":
					Expect(r.Operation.Name).To(Equal("DescribeDBInstances"))
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.DescribeDBInstancesInput{}))
					Expect(r.Params).To(Equal(describeDBInstancesInput))
					data := r.Data.(*rds.DescribeDBInstancesOutput)
					data.DBInstances = describeDBInstances
					r.Error = describeDBInstanceError
				case "ModifyDBInstance":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ModifyDBInstanceInput{}))
					Expect(r.Params).To(Equal(modifyDBInstanceInput))
					r.Error = modifyDBInstanceError
				case "AddTagsToResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.AddTagsToResourceInput{}))
					Expect(r.Params).To(Equal(addTagsToResourceInput))
					r.Error = addTagsToResourceError
				case "ListTagsForResource":
					Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
					input := r.Params.(*rds.ListTagsForResourceInput)
					snapshotArnRegex := "arn:.*:rds:.*:.*:db:" + aws.StringValue(describeDBInstancesInput.DBInstanceIdentifier)
					Expect(aws.StringValue(input.ResourceName)).To(MatchRegexp(snapshotArnRegex))
					data := r.Data.(*rds.ListTagsForResourceOutput)
					data.TagList = BuilRDSTags(listTags)
					r.Error = listTagsForResourceError
				}
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when apply immediately is set to true", func() {
			BeforeEach(func() {
				applyImmediately = true
				modifyDBInstanceInput.ApplyImmediately = aws.Bool(true)
			})

			It("returns the proper DB Instance", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when is a different DB engine", func() {
			BeforeEach(func() {
				dbInstanceDetails.Engine = "new-engine"
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("Migrating the RDS DB Instance engine from 'test-engine' to 'new-engine' is not supported"))
			})
		})

		Context("when has AllocatedStorage", func() {
			BeforeEach(func() {
				dbInstanceDetails.AllocatedStorage = 500
				modifyDBInstanceInput.AllocatedStorage = aws.Int64(500)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("and new value is less than old value", func() {
				BeforeEach(func() {
					dbInstanceDetails.AllocatedStorage = 50
					modifyDBInstanceInput.AllocatedStorage = aws.Int64(100)
				})

				It("picks up the old value", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has AutoMinorVersionUpgrade", func() {
			BeforeEach(func() {
				dbInstanceDetails.AutoMinorVersionUpgrade = true
				modifyDBInstanceInput.AutoMinorVersionUpgrade = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has BackupRetentionPeriod", func() {
			BeforeEach(func() {
				dbInstanceDetails.BackupRetentionPeriod = 7
				modifyDBInstanceInput.BackupRetentionPeriod = aws.Int64(7)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has CopyTagsToSnapshot", func() {
			BeforeEach(func() {
				dbInstanceDetails.CopyTagsToSnapshot = true
				modifyDBInstanceInput.CopyTagsToSnapshot = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBInstanceClass", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBInstanceClass = "db.m3.small"
				modifyDBInstanceInput.DBInstanceClass = aws.String("db.m3.small")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBParameterGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBParameterGroupName = "test-db-parameter-group-name"
				modifyDBInstanceInput.DBParameterGroupName = aws.String("test-db-parameter-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has DBSecurityGroups", func() {
			BeforeEach(func() {
				dbInstanceDetails.DBSecurityGroups = []string{"test-db-security-group"}
				modifyDBInstanceInput.DBSecurityGroups = aws.StringSlice([]string{"test-db-security-group"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has EngineVersion", func() {
			BeforeEach(func() {
				dbInstanceDetails.EngineVersion = "1.2.4"
				modifyDBInstanceInput.EngineVersion = aws.String("1.2.4")
				modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(false)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("and is a major version upgrade", func() {
				BeforeEach(func() {
					dbInstanceDetails.EngineVersion = "1.3.3"
					modifyDBInstanceInput.EngineVersion = aws.String("1.3.3")
					modifyDBInstanceInput.AllowMajorVersionUpgrade = aws.Bool(true)
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when has MultiAZ", func() {
			BeforeEach(func() {
				dbInstanceDetails.MultiAZ = true
				modifyDBInstanceInput.MultiAZ = aws.Bool(true)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has OptionGroupName", func() {
			BeforeEach(func() {
				dbInstanceDetails.OptionGroupName = "test-option-group-name"
				modifyDBInstanceInput.OptionGroupName = aws.String("test-option-group-name")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredBackupWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredBackupWindow = "test-preferred-backup-window"
				modifyDBInstanceInput.PreferredBackupWindow = aws.String("test-preferred-backup-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has PreferredMaintenanceWindow", func() {
			BeforeEach(func() {
				dbInstanceDetails.PreferredMaintenanceWindow = "test-preferred-maintenance-window"
				modifyDBInstanceInput.PreferredMaintenanceWindow = aws.String("test-preferred-maintenance-window")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has StorageType", func() {
			BeforeEach(func() {
				dbInstanceDetails.StorageType = "test-storage-type"
				modifyDBInstanceInput.StorageType = aws.String("test-storage-type")
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Iops", func() {
			BeforeEach(func() {
				dbInstanceDetails.Iops = 1000
				modifyDBInstanceInput.Iops = aws.Int64(1000)
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has VpcSecurityGroupIds", func() {
			BeforeEach(func() {
				dbInstanceDetails.VpcSecurityGroupIds = []string{"test-vpc-security-group-ids"}
				modifyDBInstanceInput.VpcSecurityGroupIds = aws.StringSlice([]string{"test-vpc-security-group-ids"})
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when has Tags", func() {
			BeforeEach(func() {
				dbInstanceDetails.Tags = map[string]string{"Owner": "Cloud Foundry"}
			})

			It("does not return error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when adding tags to resource fails", func() {
				BeforeEach(func() {
					addTagsToResourceError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("when getting user arn fails", func() {
				BeforeEach(func() {
					getCallerIdentityError = errors.New("operation failed")
				})

				It("does not return error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		Context("when describing the DB instance fails", func() {
			BeforeEach(func() {
				describeDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})

		Context("when modifying the DB instance fails", func() {
			BeforeEach(func() {
				modifyDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					modifyDBInstanceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Modify(dbInstanceIdentifier, dbInstanceDetails, applyImmediately)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
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
			err := rdsDBInstance.Reboot(dbInstanceIdentifier)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when rebooting the DB instance fails", func() {
			BeforeEach(func() {
				rebootDBInstanceError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := rdsDBInstance.Reboot(dbInstanceIdentifier)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is a 404 error", func() {
				BeforeEach(func() {
					awsError := awserr.New("code", "message", errors.New("operation failed"))
					rebootDBInstanceError = awserr.NewRequestFailure(awsError, 404, "request-id")
				})

				It("returns the proper error", func() {
					err := rdsDBInstance.Reboot(dbInstanceIdentifier)
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
					awsError := awserr.New("code", "message", errors.New("operation failed"))
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

})
