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
)

var _ = Describe("RDS Utils", func() {
	var (
		awsSession *session.Session

		rdssvc  *rds.RDS
		rdsCall func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger
	)

	BeforeEach(func() {
		awsSession, _ = session.NewSession(nil)

		rdssvc = rds.New(awsSession)

		logger = lager.NewLogger("rdsservice_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)
	})

	var _ = Describe("BuildRDSTags", func() {
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
			rdsTags := BuildRDSTags(tags)
			Expect(rdsTags).To(Equal(properRDSTags))
		})
	})

	var _ = Describe("ListTagsForResource", func() {
		var (
			resourceARN     string
			expectedRdsTags []*rds.Tag

			listTagsForResourceInput *rds.ListTagsForResourceInput
			listTagsForResourceError error
		)

		BeforeEach(func() {
			resourceARN = "arn:aws:rds:rds-region:account:db:identifier"
			expectedRdsTags = []*rds.Tag{
				&rds.Tag{
					Key:   aws.String("Owner"),
					Value: aws.String("Cloud Foundry"),
				},
			}

			listTagsForResourceInput = &rds.ListTagsForResourceInput{
				ResourceName: aws.String(resourceARN),
			}
			listTagsForResourceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("ListTagsForResource"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.ListTagsForResourceInput{}))
				Expect(r.Params).To(Equal(listTagsForResourceInput))
				data := r.Data.(*rds.ListTagsForResourceOutput)
				data.TagList = expectedRdsTags
				r.Error = listTagsForResourceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			_, err := ListTagsForResource(resourceARN, rdssvc, logger)
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns the expected tags", func() {
			rdsTags, err := ListTagsForResource(resourceARN, rdssvc, logger)
			Expect(err).ToNot(HaveOccurred())
			Expect(rdsTags).To(Equal(expectedRdsTags))
		})

		Context("when adding tags to a resource fails", func() {
			BeforeEach(func() {
				listTagsForResourceError = errors.New("operation failed")
			})

			It("return error the proper error", func() {
				_, err := ListTagsForResource(resourceARN, rdssvc, logger)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					listTagsForResourceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := ListTagsForResource(resourceARN, rdssvc, logger)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("RemoveTagsFromResource", func() {
		var (
			resourceARN string
			rdsTagKeys  []*string

			removeTagsFromResourceInput *rds.RemoveTagsFromResourceInput
			removeTagsFromResourceError error
		)

		BeforeEach(func() {
			resourceARN = "arn:aws:rds:rds-region:account:db:identifier"
			tagKey := "atag"
			rdsTagKeys = []*string{
				&tagKey,
			}

			removeTagsFromResourceInput = &rds.RemoveTagsFromResourceInput{
				ResourceName: aws.String(resourceARN),
				TagKeys:      rdsTagKeys,
			}
			removeTagsFromResourceError = nil
		})

		JustBeforeEach(func() {
			rdssvc.Handlers.Clear()

			rdsCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("RemoveTagsFromResource"))
				Expect(r.Params).To(BeAssignableToTypeOf(&rds.RemoveTagsFromResourceInput{}))
				Expect(r.Params).To(Equal(removeTagsFromResourceInput))
				r.Error = removeTagsFromResourceError
			}
			rdssvc.Handlers.Send.PushBack(rdsCall)
		})

		It("does not return error", func() {
			err := RemoveTagsFromResource(resourceARN, rdsTagKeys, rdssvc, logger)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when removing tags from a resource fails", func() {
			BeforeEach(func() {
				removeTagsFromResourceError = errors.New("operation failed")
			})

			It("return error the proper error", func() {
				err := RemoveTagsFromResource(resourceARN, rdsTagKeys, rdssvc, logger)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					removeTagsFromResourceError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := RemoveTagsFromResource(resourceARN, rdsTagKeys, rdssvc, logger)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

})
