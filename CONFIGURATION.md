# Configuration

A sample configuration can be found at [config-sample.json](https://github.com/alphagov/paas-rds-broker/blob/master/config-sample.json).

## General Configuration

| Option                 | Required | Type    | Description
|:-----------------------|:--------:|:--------|:-----------
| port                   | N        | Integer | The TCP port to listen on. Defaults to 3000 if unspecified.
| log_level              | Y        | String  | Broker Log Level (DEBUG, INFO, ERROR, FATAL)
| username               | Y        | String  | Broker Auth Username
| password               | Y        | String  | Broker Auth Password
| run_housekeeping       | N        | Boolean | Whether to run housekeeping tasks (including master password rotation, and snapshot cleanups). This should be set to true on exactly one instance in your deployment.
| cron_schedule          | Y        | String  | Schedule for cron jobs. A crontab-like expression with seconds precision (e.g. '0 0 * * * *' or '@hourly'), with fields: 'second minute hour dom month dow'
| keep_snapshots_for_days| Y        | Integer | Number of days to keep old RDS snapshots for
| rds_config             | Y        | Hash    | [RDS Broker configuration](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#rds-broker-configuration)

## RDS Broker Configuration

| Option                         | Required | Type    | Description
|:-------------------------------|:--------:|:------- |:-----------
| region                         | Y        | String  | RDS Region
| db_prefix                      | Y        | String  | Prefix to add to RDS DB Identifiers
| allow_user_provision_parameters| N        | Boolean | Allow users to send arbitrary parameters on provision calls (defaults to `false`)
| allow_user_update_parameters   | N        | Boolean | Allow users to send arbitrary parameters on update calls (defaults to `false`)
| allow_user_bind_parameters     | N        | Boolean | Allow users to send arbitrary parameters on bind calls (defaults to `false`)
| catalog                        | Y        | Hash    | [RDS Broker catalog](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#rds-broker-catalog)
| master_password_seed           | Y        | String  | Seed to generate DB instances master passwords
| broker_name                    | Y        | String  | RDS broker name used to tag instances for identification

### Note
When the seed is changed and the broker restarted, the instances master passwords will be updated.

## RDS Broker catalog

Please refer to the [Catalog Documentation](https://docs.cloudfoundry.org/services/api.html#catalog-mgmt) for more details about these properties.

### Catalog

| Option   | Required | Type      | Description
|:---------|:--------:|:--------- |:-----------
| services | N        | []Service | A list of [Services](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#service)

### Service

| Option                        | Required | Type          | Description
|:------------------------------|:--------:|:------------- |:-----------
| id                            | Y        | String        | An identifier used to correlate this service in future requests to the catalog
| name                          | Y        | String        | The CLI-friendly name of the service that will appear in the catalog. All lowercase, no spaces
| description                   | Y        | String        | A short description of the service that will appear in the catalog
| tags                          | N        | []String      | A list of service tags
| metadata.displayName          | N        | String        | The name of the service to be displayed in graphical clients
| metadata.imageUrl             | N        | String        | The URL to an image
| metadata.longDescription      | N        | String        | Long description
| metadata.providerDisplayName  | N        | String        | The name of the upstream entity providing the actual service
| metadata.documentationUrl     | N        | String        | Link to documentation page for service
| metadata.supportUrl           | N        | String        | Link to support for the service
| requires                      | N        | []String      | A list of permissions that the user would have to give the service, if they provision it (only `syslog_drain` is supported)
| plan_updateable               | N        | Boolean       | Whether the service supports upgrade/downgrade for some plans
| plans                         | N        | []ServicePlan | A list of [Plans](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#service-plan) for this service
| dashboard_client.id           | N        | String        | The id of the Oauth2 client that the service intends to use
| dashboard_client.secret       | N        | String        | A secret for the dashboard client
| dashboard_client.redirect_uri | N        | String        | A domain for the service dashboard that will be whitelisted by the UAA to enable SSO

### Service Plan

| Option               | Required | Type          | Description
|:---------------------|:--------:|:------------- |:-----------
| id                   | Y        | String        | An identifier used to correlate this plan in future requests to the catalog
| name                 | Y        | String        | The CLI-friendly name of the plan that will appear in the catalog. All lowercase, no spaces
| description          | Y        | String        | A short description of the plan that will appear in the catalog
| metadata.bullets     | N        | []String      | Features of this plan, to be displayed in a bulleted-list
| metadata.costs       | N        | Cost Object   | An array-of-objects that describes the costs of a service, in what currency, and the unit of measure
| metadata.displayName | N        | String        | Name of the plan to be display in graphical clients
| free                 | N        | Boolean       | This field allows the plan to be limited by the non_basic_services_allowed field in a Cloud Foundry Quota
| rds_properties       | Y        | RDSProperties | [RDS Properties](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md#rds-properties)

## RDS Properties

Please refer to the [Amazon Relational Database Service Documentation](https://aws.amazon.com/documentation/rds/) for more details about these properties.

| Option                          | Required | Type      | Description
|:--------------------------------|:--------:|:--------- |:-----------
| allocated_storage               | Y        | Integer   | The amount of storage (in gigabytes) to be initially allocated for the database instances (between `5` and `6144`)
| auto_minor_version_upgrade      | N        | Boolean   | Enable or disable automatic upgrades to new minor versions as they are released (defaults to `false`)
| availability_zone               | N        | String    | The Availability Zone that database instances will be created in
| backup_retention_period         | N        | Integer   | The number of days that Amazon RDS should retain automatic backups of DB instances (between `0` and `35`)
| character_set_name              | N        | String    | For supported engines, indicates that DB instances should be associated with the specified CharacterSet
| copy_tags_to_snapshot           | N        | Boolean   | Enable or disable copying all tags from DB instances to snapshots
| db_instance_class               | Y        | String    | The name of the DB Instance Class
| db_security_groups              | N        | []String  | The security group(s) names that have rules authorizing connections from applications that need to access the data stored in the DB instance
| db_subnet_group_name            | N        | String    | The DB subnet group name that defines which subnets and IP ranges the DB instance can use in the VPC
| engine                          | Y        | String    | The name of the Database Engine (only `mariadb`, `mysql` and `postgres` are supported)
| engine_version                  | Y        | String    | The version number of the Database Engine
| iops                            | N        | Integer   | The amount of Provisioned IOPS to be initially allocated for DB instances when using `io1` storage type
| kms_key_id                      | N        | String    | The KMS key identifier for encrypted DB instances
| license_model                   | N        | String    | License model information for DB instances (`license-included`, `bring-your-own-license`, `general-public-license`)
| multi_az                        | N        | Boolean   | Enable or disable Multi-AZ deployment for high availability DB Instances
| option_group_name               | N        | String    | The DB option group name that enables any optional functionality you want the DB instances to support
| port                            | N        | Integer   | The TCP/IP port DB instances will use for application connections
| preferred_backup_window         | N        | String    | The daily time range during which automated backups are created if automated backups are enabled
| preferred_maintenance_window    | N        | String    | The weekly time range during which system maintenance can occur
| publicly_accessible             | N        | Boolean   | Specify if DB instances will be publicly accessible
| skip_final_snapshot             | N        | Boolean   | Determines whether a final DB snapshot is created before the DB instances are deleted
| storage_encrypted               | N        | Boolean   | Specifies whether DB instances are encrypted
| storage_type                    | N        | String    | The storage type to be associated with DB instances (`standard`, `gp2`, `io1`)
| vpc_security_group_ids          | N        | []String  | VPC security group(s) IDs that have rules authorizing connections from applications that need to access the data stored in DB instances
| allowed_extensions              | Y        | []String  | The set of Postgres extensions which can be enabled  
| default_extensions              | Y        | []String  | The set of Postgres extensions which are enabled by default. Each of these must also be in the `allowed_extensions` list.
