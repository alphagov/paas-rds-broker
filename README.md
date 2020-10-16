# AWS RDS Service Broker [![Build Status](https://travis-ci.org/alphagov/paas-rds-broker.png)](https://travis-ci.org/alphagov/paas-rds-broker)


This is a fork of [AWS RDS Service Broker](https://github.com/cf-platform-eng/rds-broker) written by Pivotal Software Inc.

We have changed all source code includes to point to this repository.

This is a [Cloud Foundry Service Broker](https://docs.cloudfoundry.org/services/overview.html) for [Amazon Relational Database Service (RDS)](https://aws.amazon.com/rds/) supporting [MariaDB](https://aws.amazon.com/rds/mariadb/), [MySQL](https://aws.amazon.com/rds/mysql/) and [PostgreSQL](https://aws.amazon.com/rds/postgresql/) RDS Databases.

More details can be found at this [Pivotal P.O.V Blog post](http://blog.pivotal.io/pivotal-cloud-foundry/products/a-look-at-cloud-foundrys-service-broker-updates).

## Installation

### Locally

Using the standard `go install` (you must have [Go](https://golang.org/) already installed in your local machine):

```
$ go install github.com/alphagov/paas-rds-broker
$ rds-broker -config=<path-to-your-config-file>
```

### BOSH

This broker can be deployed using the [AWS Service Broker BOSH Release](https://github.com/cf-platform-eng/aws-broker-boshrelease).

## Configuration

Refer to the [Configuration](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md) instructions for details about configuring this broker.

This broker gets the AWS credentials from the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. It requires a user with some [IAM](https://aws.amazon.com/iam/) & [RDS](https://aws.amazon.com/rds/) permissions. Refer to the [iam_policy.json](https://github.com/alphagov/paas-rds-broker/blob/master/iam_policy.json) file to check what actions the user must be allowed to perform.

## Usage

### Managing Service Broker

Configure and deploy the broker using one of the above methods. Then:

1. Check that your Cloud Foundry installation supports [Service Broker API Version v2.6 or greater](https://docs.cloudfoundry.org/services/api.html#changelog)
2. [Register the broker](https://docs.cloudfoundry.org/services/managing-service-brokers.html#register-broker) within your Cloud Foundry installation;
3. [Make Services and Plans public](https://docs.cloudfoundry.org/services/access-control.html#enable-access);
4. Depending on your Cloud Foundry settings, you migh also need to create/bind an [Application Security Group](https://docs.cloudfoundry.org/adminguide/app-sec-groups.html) to allow access to the RDS DB Instances.

### Integrating Service Instances with Applications

Application Developers can start to consume the services using the standard [CF CLI commands](https://docs.cloudfoundry.org/devguide/services/managing-services.html).

Depending on the [broker configuration](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#rds-broker-configuration), Application Depevelopers can send arbitrary parameters on certain broker calls:

#### Provision

Provision calls support the following optional [arbitrary parameters](https://docs.cloudfoundry.org/devguide/services/managing-services.html#arbitrary-params-create):

| Option                         | Type     | Description
|:-------------------------------|:---------|:-----------
| `backup_retention_period`      | Integer  | The number of days that Amazon RDS should retain automatic backups of the DB instance (between `0` and `35`) (*)
| `character_set_name`           | String   | For supported engines, indicates that the DB instance should be associated with the specified CharacterSet (*)
| `dbname`                       | String   | The name of the Database to be provisioned. If it does not exists, the broker will create it, otherwise, it will reuse the existing one. If this parameter is not set, the broker will use a random Database name
| `preferred_backup_window`      | String   | The daily time range during which automated backups are created if automated backups are enabled (*)
| `preferred_maintenance_window` | String   | The weekly time range during which system maintenance can occur (*)
| `enable_extensions`           | []String | The names of the extensions which should be enabled. Supported extensions are specified by the plan, and the supplied list is combined with the set of default extensions defined by the plan. If this parameter isn't provided, the plan's default extensions will be enabled. (*\*)

(\*) Refer to the [Amazon Relational Database Service Documentation](https://aws.amazon.com/documentation/rds/) for more details about how to set these properties

(\*\*) Postgres only

#### Update

Update calls support the following optional [arbitrary parameters](https://docs.cloudfoundry.org/devguide/services/managing-services.html#arbitrary-params-update):

| Option                           | Type     | Description
|:-------------------------------  |:-------  |:-----------
| `apply_at_maintenance_window`    | Boolean  | Specifies whether the modifications in this request and any pending modifications are asynchronously applied as soon as possible (default) or if they should be queued until the Preferred Maintenance Window setting for the DB instance (*)
| `backup_retention_period`        | Integer  | The number of days that Amazon RDS should retain automatic backups of the DB instance (between `0` and `35`) (*)
| `preferred_backup_window`        | String   | The daily time range during which automated backups are created if automated backups are enabled (*)
| `preferred_maintenance_window`   | String   | The weekly time range during which system maintenance can occur (*)
| `preferred_maintenance_window`   | String   | The weekly time range during which system maintenance can occur (*)
| `reboot`                         | Boolean  | Reboot the instance immediately. Any other parameter or change in the updated would be ignored. See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RebootInstance.html for details.
| `force_failover`                 | Boolean  | For HA failover during reboot. Only valid when used with `reboot` and for HA plans. See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RebootInstance.html for detais.
| `update_minor_version_to_latest` | Boolean  | Attempts to update the database to the latest available minor version supported by RDS as per the `rds:DescribeDBEngineVersions` API
| `enable_extensions`              | []String | The names of the extensions which should be enabled. Supported extensions are specified by the plan, and the supplied list is combined with the set of default extensions defined by the plan. (*\*)
| `disable_extensions`             | []String | The names of the extensions which should be disabled. Supported extensions are specified by the plan, and default extensions cannot be disabled. (*\*)

(*) Refer to the [Amazon Relational Database Service Documentation](https://aws.amazon.com/documentation/rds/) for more details about how to set these properties

(\*\*) Postgres only

#### Reboot

Reboot is performed by passing the custom parameter `{ "reboot": true }` in an update. Pass `{ "reboot": true, "force_failover": true }` to force failover in a HA instance.

It will not update the instance, so the new plan or custom parameters would be ignored.

### Housekeeping tasks

The broker runs a number of housekeeping tasks. These need to be enabled on exactly one instance in your deployment by setting `run_housekeeping` to `true` in the config file.

#### Delete old RDS snapshots

The housekeeping task will delete old RDS snapshots which were created by this broker. It will search for snapshots older than `keep_snapshots_for_days` and with the matching `Broker Name` tag (config: `rds_config.broker_name`).

## Running tests

There are two forms of tests for the broker, the unit tests and the integration tests. The unit tests are run automatically by travis, but because the integration tests actually use the AWS RDS API they must be run manually or by an agent with AWS credentials.

### Running the unit tests

To run the tests of this broker, you need a Postgres and MySQL running locally, without SSL. The connection details can be configured using environment variables - see the `sqlengine` test suites for more information.

To run the unit tests:

```bash
make unit
```

This handles the starting of Docker containers running MySQL and Postgres. It will also re-run the SQL engine tests against Postgres 10.5.

The Docker containers are removed after a successful test run. If you need to remove them you can use:

```bash
make stop_dbs
```

### Running the integration tests

These tests must be run from within an AWS environment as they will attempt to connect directly to the RDS instance to verify it. They will create and delete some supporting resources:

- [DB Subnet Group](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.WorkingWithRDSInstanceinaVPC.html#USER_VPC.Subnets) for the RDS instances, based on all of the subnets in the VPC of the instance that the tests run on
- [EC2 Security Group](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_SecurityGroups.html) for the test to connect to RDS instances, based on the subnet of the instance that the tests run on

They need IAM permissions for the above, everything listed in [`iam_policy.json`](iam_policy.json), and the ability to delete RDS snapshots.

You can run them with:
```
make integration
```

## Master and Binding Credentials

The RDS Broker generates and uses two sets of credentials: master and binding credentials.

![Credentials generation flow](https://raw.githubusercontent.com/alphagov/paas-rds-broker/master/docs/rdsbroker_credentials_flow.png)

[Source](https://drive.google.com/file/d/163K2r7dx3KlheNInTy829y_qdjGNO-GI/view?usp=sharing)

### Master credentials

The master credentials are generated when a DB instance is created and are used to create a database connection
whenever an admin level access of the database only used by RDS-broker is required, including binding (equivalent of
creating a new DB user), unbinding (equivalent of deleting a DB user), routing DB metrics, changing user passwords and
rotating master credentials. It consists of:

* _masterUsername_ - A random alphanumeric field generated at the point of instance creation.
* _masterPassword_ - An SHA256 hashed alphanumeric field, generated based on instance id and a secret.

### Binding credentials

Binding allows an app to get access to a DB with limited user credentials.
Whenever an app is deployed and configured to bind to a PaaS DB, a new set of credentials is generated and used to
create a DB user, binding an app to the DB service, allowing access to the DB.
Binding credentials consist of:

* _username_ - this is an SHA256 hashed alphanumeric field, generated based on binding id (username)
* _password_ - a random alphanumeric field
* _Note on usernameold_ - we have recently changed our hashing algorithm from MD5 to SHA256. This function is to
support the legacy binding credentials that are still using MD5 as hashing algorithm. When dropping a user (DropUser),
we generate username (generateUsername) with the new hashing algorithm (SHA256), if there is no match, we try to use
the old hashing algorithm (generateUsernameOld)

It is noted that masterPassword and binding username are deterministically generated by hashing the binding ID and
secrets. Hence, if we change the hashing algorithm, it will have effect on both the master credentials and binding and
unbinding of the instances, and may cause downtime if it is not handled properly.

### Extensions and parameter groups

Certain Postgres extensions (such as `pg_stat_statements`) require [shared preload libraries](https://www.postgresql.org/docs/9.5/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-PRELOAD)
to be enabled. In RDS, preload libraries are specified as a parameter in the parameter group, and are enabled for all
databases in that group.

The RDS broker will create new parameter groups to support new combinations of extensions as necessary, or otherwise
add new databases to existing groups with the right extensions.

Enabling or disabling extensions like `pg_stat_statements` that require shared preload libraries via an update call will apply a new parameter group. As a result, it also requires `"reboot": true` to be specified.

## Contributing

In the spirit of [free software](http://www.fsf.org/licensing/essays/free-sw.html), **everyone** is encouraged to help improve this project.

Here are some ways *you* can contribute:

* by using alpha, beta, and prerelease versions
* by reporting bugs
* by suggesting new features
* by writing or editing documentation
* by writing specifications
* by writing code (**no patch is too small**: fix typos, add comments, clean up inconsistent whitespace)
* by refactoring code
* by closing [issues](https://github.com/alphagov/paas-rds-broker/issues)
* by reviewing patches

### Submitting an Issue

We use the [GitHub issue tracker](https://github.com/alphagov/paas-rds-broker/issues) to track bugs and features. Before submitting a bug report or feature request, check to make sure it hasn't already been submitted. You can indicate support for an existing issue by voting it up. When submitting a bug report, please include a [Gist](http://gist.github.com/) that includes a stack trace and any details that may be necessary to reproduce the bug, including your Golang version and operating system. Ideally, a bug report should include a pull request with failing specs.

### Submitting a Pull Request

1. Fork the project.
2. Create a topic branch.
3. Implement your feature or bug fix.
4. Commit and push your changes.
5. Submit a pull request.

## Copyright

Copyright (c) 2015 Pivotal Software Inc. See [LICENSE](https://github.com/alphagov/paas-rds-broker/blob/master/LICENSE) for details.
