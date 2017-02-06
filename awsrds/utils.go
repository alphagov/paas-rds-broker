package awsrds

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pivotal-golang/lager"
)

func UserAccount(stssvc *sts.STS) (string, error) {
	getCallerIdentityInput := &sts.GetCallerIdentityInput{}
	getCallerIdentityOutput, err := stssvc.GetCallerIdentity(getCallerIdentityInput)
	if err != nil {
		return "", err
	}

	return *getCallerIdentityOutput.Account, nil

}

func BuilRDSTags(tags map[string]string) []*rds.Tag {
	var rdsTags []*rds.Tag

	for key, value := range tags {
		rdsTags = append(rdsTags, &rds.Tag{Key: aws.String(key), Value: aws.String(value)})
	}

	return rdsTags
}

func RDSTagsValues(rdsTags []*rds.Tag) map[string]string {
	tags := map[string]string{}

	for _, t := range rdsTags {
		tags[aws.StringValue(t.Key)] = aws.StringValue(t.Value)
	}

	return tags
}

func AddTagsToResource(resourceARN string, tags []*rds.Tag, rdssvc *rds.RDS, logger lager.Logger) error {
	addTagsToResourceInput := &rds.AddTagsToResourceInput{
		ResourceName: aws.String(resourceARN),
		Tags:         tags,
	}

	logger.Debug("add-tags-to-resource", lager.Data{"input": addTagsToResourceInput})

	addTagsToResourceOutput, err := rdssvc.AddTagsToResource(addTagsToResourceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	logger.Debug("add-tags-to-resource", lager.Data{"output": addTagsToResourceOutput})

	return nil
}

func ListTagsForResource(resourceARN string, rdssvc *rds.RDS, logger lager.Logger) ([]*rds.Tag, error) {
	listTagsForResourceInput := &rds.ListTagsForResourceInput{
		ResourceName: aws.String(resourceARN),
	}

	logger.Debug("list-tags-for-resource", lager.Data{"input": listTagsForResourceInput})

	listTagsForResourceOutput, err := rdssvc.ListTagsForResource(listTagsForResourceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return listTagsForResourceOutput.TagList, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return listTagsForResourceOutput.TagList, err
	}

	logger.Debug("list-tags-for-resource", lager.Data{"output": listTagsForResourceOutput})

	return listTagsForResourceOutput.TagList, nil
}
