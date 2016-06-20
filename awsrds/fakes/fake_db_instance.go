package fakes

import (
	"github.com/alphagov/paas-rds-broker/awsrds"
)

type FakeDBInstance struct {
	DescribeCalled            bool
	DescribeID                string
	DescribeDBInstanceDetails awsrds.DBInstanceDetails
	DescribeError             error

	DescribeByTagCalled            bool
	DescribeByTagKey               string
	DescribeByTagValue             string
	DescribeByTagDBInstanceDetails []*awsrds.DBInstanceDetails
	DescribeByTagError             error

	CreateCalled            bool
	CreateID                string
	CreateDBInstanceDetails awsrds.DBInstanceDetails
	CreateError             error

	ModifyCalled            bool
	ModifyID                string
	ModifyDBInstanceDetails awsrds.DBInstanceDetails
	ModifyApplyImmediately  bool
	ModifyError             error

	DeleteCalled            bool
	DeleteID                string
	DeleteSkipFinalSnapshot bool
	DeleteError             error
}

func (f *FakeDBInstance) Describe(ID string) (awsrds.DBInstanceDetails, error) {
	f.DescribeCalled = true
	f.DescribeID = ID

	return f.DescribeDBInstanceDetails, f.DescribeError
}

func (f *FakeDBInstance) DescribeByTag(tagKey, tagValue string) ([]*awsrds.DBInstanceDetails, error) {
	f.DescribeByTagCalled = true
	f.DescribeByTagKey = tagKey
	f.DescribeByTagValue = tagValue

	return f.DescribeByTagDBInstanceDetails, f.DescribeByTagError
}

func (f *FakeDBInstance) Create(ID string, dbInstanceDetails awsrds.DBInstanceDetails) error {
	f.CreateCalled = true
	f.CreateID = ID
	f.CreateDBInstanceDetails = dbInstanceDetails

	return f.CreateError
}

func (f *FakeDBInstance) Modify(ID string, dbInstanceDetails awsrds.DBInstanceDetails, applyImmediately bool) error {
	f.ModifyCalled = true
	f.ModifyID = ID
	f.ModifyDBInstanceDetails = dbInstanceDetails
	f.ModifyApplyImmediately = applyImmediately

	return f.ModifyError
}

func (f *FakeDBInstance) Delete(ID string, skipFinalSnapshot bool) error {
	f.DeleteCalled = true
	f.DeleteID = ID
	f.DeleteSkipFinalSnapshot = skipFinalSnapshot

	return f.DeleteError
}
