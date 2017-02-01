package awsrds_test

import (
	"errors"

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

var _ = Describe("RDS Utils", func() {
	var (
		awsSession *session.Session

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		stssvc  *sts.STS
		stsCall func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger
	)

	BeforeEach(func() {
		awsSession = session.New(nil)

		rdssvc = rds.New(awsSession)
		stssvc = sts.New(awsSession)

		logger = lager.NewLogger("rdsservice_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)
	})

	var _ = Describe("UserAccount", func() {
		var (
			getCallerIdentityInput *sts.GetCallerIdentityInput
			getCallerIdentityError error
		)
		const account = "123456789012"

		BeforeEach(func() {
			getCallerIdentityError = nil
			getCallerIdentityInput = &sts.GetCallerIdentityInput{}
		})

		JustBeforeEach(func() {
			stssvc.Handlers.Clear()

			stsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("GetCallerIdentity"))
				Expect(r.Params).To(Equal(getCallerIdentityInput))
				data := r.Data.(*sts.GetCallerIdentityOutput)
				data.Account = aws.String(account)
				r.Error = getCallerIdentityError
			}
			stssvc.Handlers.Send.PushBack(stsCall)
		})

		It("returns the User Account", func() {
			userAccount, err := UserAccount(stssvc)
			Expect(err).ToNot(HaveOccurred())
			Expect(userAccount).To(Equal(account))
		})

		Context("when getting user fails", func() {
			BeforeEach(func() {
				getCallerIdentityError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := UserAccount(stssvc)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})
		})
	})

	var _ = Describe("BuilRDSTags", func() {
		var (
			tags          map[string]string
			properRDSTags []*rds.Tag
		)

		BeforeEach(func() {
			tags = map[string]string{"Owner": "Cloud Foundry"}
			properRDSTags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Owner"),
					Value: aws.String("Cloud Foundry"),
				},
			}
		})

		It("returns the proper RDS Tags", func() {
			rdsTags := BuilRDSTags(tags)
			Expect(rdsTags).To(Equal(properRDSTags))
		})
	})

	var _ = Describe("AddTagsToResource", func() {
		var (
			resourceARN string
			rdsTags     []*rds.Tag

			addTagsToResourceInput *rds.AddTagsToResourceInput
			addTagsToResourceError error
		)

		BeforeEach(func() {
			resourceARN = "arn:aws:rds:rds-region:account:db:identifier"
			rdsTags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Owner"),
					Value: aws.String("Cloud Foundry"),
				},
			}

			addTagsToResourceInput = &rds.AddTagsToResourceInput{
				ResourceName: aws.String(resourceARN),
				Tags:         rdsTags,
			}
			addTagsToResourceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("AddTagsToResource"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.AddTagsToResourceInput{}))
				Expect(r.Params).To(Equal(addTagsToResourceInput))
				r.Error = addTagsToResourceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := AddTagsToResource(resourceARN, rdsTags, rdssvc, logger)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when adding tags to a resource fails", func() {
			BeforeEach(func() {
				addTagsToResourceError = errors.New("operation failed")
			})

			It("return error the proper error", func() {
				err := AddTagsToResource(resourceARN, rdsTags, rdssvc, logger)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					addTagsToResourceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := AddTagsToResource(resourceARN, rdsTags, rdssvc, logger)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})
})
