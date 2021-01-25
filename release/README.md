# AWS Service Broker BOSH Release

This is a [BOSH](http://bosh.io/) release for [AWS RDS Service Broker](https://github.com/alphagov/paas-rds-broker)

This is a fork of [AWS Service Broker BOSH Release](https://github.com/cf-platform-eng/aws-broker-boshrelease) written by Pivotal Software Inc.

We have removed all references to [AWS CloudFormation Service Broker](https://github.com/cf-platform-eng/cloudformation-broker) and [AWS SQS Service Broker](https://github.com/cf-platform-eng/sqs-broker) that were included in original repository. 


## Disclaimer

This is NOT presently a production ready AWS Service Broker BOSH release. This is a work in progress. It is suitable for experimentation and may not become supported in the future.

## Usage

### AWS credentials and permissions

This BOSH release requires an AWS user with some advanced privileges. Instead of using your primary AWS admin user, it is recommended to create a new AWS user (with the proper access keys), create an specific policy based on the [iam_policy.json](iam_policy.json) file, and then attach the policy to the user.

### Using BOSH

#### Upload the BOSH release

To use this BOSH release, first upload it to your BOSH:

```
bosh target BOSH_HOST
git clone https://github.com/alphagov/paas-rds-broker-boshrelease.git
cd aws-broker-boshrelease
bosh upload release releases/aws-broker/aws-broker-2.yml
```

#### Create a BOSH deployment manifest

Now create a deployment file (using the files in the [examples](examples/) directory as a starting point).

#### Deploy using the BOSH deployment manifest

Using the previous created deployment manifest, now we can deploy it:

```
bosh deployment path/to/deployment.yml
bosh -n deploy
```

Refer to the broker's documentation for more details about the required properties:
* [AWS RDS Service Broker Documentation](https://github.com/alphagov/paas-rds-broker/blob/master/CONFIGURATION.md)

### Using [Pivotal Ops Manager](https://network.pivotal.io/products/ops-manager)

#### Pivotal tile

You can deploy the AWS Service Broker using [Pivotal Ops Manager](https://network.pivotal.io/products/ops-manager). If you want modify the configuration (services, plans, ...) you will need to build your custom Pivotal tile, otherwise, just download the already existing Pivotal tile.

##### Build the Pivotal tile

Update the [handcraft.yml](metadata_parts/handcraft.yml) file with your modifications. Then, build the Pivotal tile:

```
git clone https://github.com/alphagov/paas-rds-broker-boshrelease.git
cd paas-rds-broker-boshrelease
bundle install
bundle exec vara build-pivotal .
```

##### Download the Pivotal tile

Download the [p-aws-broker-0.1.1.0.pivotal](https://storage.googleapis.com/pivotal/p-aws-broker-0.1.1.0.pivotal) file to your workstation.

#### Upload the Pivotal tile

Upload the Pivotal tile `p-aws-broker-0.1.1.0.pivotal` to your Pivotal Ops Manager, configure it, and deploy.

## Contributing

Please note that this is a fork of [AWS RDS Service Broker](https://github.com/alphagov/paas-rds-broker). If you want to contribute please consider creating a PR for upstream repository.

In the spirit of [free software](http://www.fsf.org/licensing/essays/free-sw.html), **everyone** is encouraged to help improve this project.

Here are some ways *you* can contribute:

* by using alpha, beta, and prerelease versions
* by reporting bugs
* by suggesting new features
* by writing or editing documentation
* by writing specifications
* by writing code (**no patch is too small**: fix typos, add comments, clean up inconsistent whitespace)
* by refactoring code
* by closing [issues](https://github.com/alphagov/paas-rds-broker-boshrelease/issues)
* by reviewing patches

### Submitting an Issue
We use the [GitHub issue tracker](https://github.com/alphagov/paas-rds-broker-boshrelease/issues) to track bugs and features. Before submitting a bug report or feature request, check to make sure it hasn't already been submitted. You can indicate support for an existing issue by voting it up. When submitting a bug report, please include a
[Gist](http://gist.github.com/) that includes a stack trace and any details that may be necessary to reproduce the bug,. Ideally, a bug report should include a pull request with failing specs.

### Submitting a Pull Request

1. Fork the project.
2. Create a topic branch.
3. Implement your feature or bug fix.
4. Commit and push your changes.
5. Submit a pull request.

## Copyright

Copyright (c) 2015 Pivotal Software, Inc & 2016 [Government Digital Service](https://www.gov.uk/government/organisations/government-digital-service). See [LICENSE](LICENSE) for details.
