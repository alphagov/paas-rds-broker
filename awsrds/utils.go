package awsrds

import (
	"errors"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
)

func BuildRDSTags(tags map[string]string) []*rds.Tag {
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

func ListTagsForResource(resourceARN string, rdssvc *rds.RDS, logger lager.Logger) ([]*rds.Tag, error) {
	listTagsForResourceInput := &rds.ListTagsForResourceInput{
		ResourceName: aws.String(resourceARN),
	}

	logger.Debug("list-tags-for-resource", lager.Data{"input": listTagsForResourceInput})

	listTagsForResourceOutput, err := rdssvc.ListTagsForResource(listTagsForResourceInput)
	if err != nil {
		return listTagsForResourceOutput.TagList, HandleAWSError(err, logger)
	}

	logger.Debug("list-tags-for-resource", lager.Data{"output": listTagsForResourceOutput})

	return listTagsForResourceOutput.TagList, nil
}

func RemoveTagsFromResource(resourceARN string, tagKeys []*string, rdssvc *rds.RDS, logger lager.Logger) error {
	removeTagsFromResourceInput := &rds.RemoveTagsFromResourceInput{
		ResourceName: aws.String(resourceARN),
		TagKeys:      tagKeys,
	}

	logger.Debug("remove-tags-from-resource", lager.Data{"input": removeTagsFromResourceInput})

	removeTagsFromResourceOutput, err := rdssvc.RemoveTagsFromResource(removeTagsFromResourceInput)
	if err != nil {
		return HandleAWSError(err, logger)
	}

	logger.Debug("remove-tags-from-resource", lager.Data{"output": removeTagsFromResourceOutput})

	return nil
}

func HandleAWSError(err error, logger lager.Logger) error {
	logger.Error("aws-rds-error", err)
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == rds.ErrCodeDBInstanceNotFoundFault {
			return ErrDBInstanceDoesNotExist
		}
		if awsErr.Code() == "InvalidParameterCombination" {
			return NewError(
				errors.New(awsErr.Code() + ": " + awsErr.Message()),
				ErrCodeInvalidParameterCombination,
			)
		}
		return NewError(errors.New(awsErr.Code() + ": " + awsErr.Message()), "")
	}
	return err
}

func GetDBPort(endpoint *rds.Endpoint) int64 {
	if endpoint == nil {
		return 0
	} else {
		return aws.Int64Value(endpoint.Port)
	}
}

func GetDBAddress(endpoint *rds.Endpoint) (dbAddress string) {
	if endpoint == nil {
		return ""
	} else {
		return aws.StringValue(endpoint.Address)
	}
}
