#!/bin/bash
export AWS_DEFAULT_REGION=eu-west-1

set -euo pipefail

clean_instances() {
  echo "Current instances"
  aws rds describe-db-instances | \
    jq -r '
      .DBInstances[] |
        select(.DBInstanceIdentifier | startswith("build-test-")) |
        "\(.DBInstanceIdentifier) \(.DBInstanceStatus)"
    '


  db_instances_to_delete="$(
    aws rds describe-db-instances | \
      jq -r '
        .DBInstances[] |
          select(.DBInstanceStatus != "deleting") |
          select(.DBInstanceIdentifier | startswith("build-test-")) |
          .DBInstanceIdentifier
      '
  )"

  if [ "${db_instances_to_delete}" != "" ]; then
    echo "${db_instances_to_delete}" |
      xargs -n 1 -I {} \
        aws rds delete-db-instance \
          --skip-final-snapshot \
          --db-instance-identifier {} | \
          jq -r '"Set to delete: \(.DBInstance.DBInstanceIdentifier) \(.DBInstance.DBInstanceStatus)"'
  fi
}

clean_subnet_groups() {
  echo "Current subnet groups"
  aws rds describe-db-subnet-groups | \
    jq -r '
      .DBSubnetGroups[] |
        select(.DBSubnetGroupName | startswith("build-test-")) |
        "\(.DBSubnetGroupName) \(.SubnetGroupStatus)"
    '

  db_subnetgroups_to_delete="$(
    aws rds describe-db-subnet-groups | \
      jq -r '
        .DBSubnetGroups[] |
          select(.DBSubnetGroupName | startswith("build-test-")) |
          .DBSubnetGroupName
      '
  )"

  if [ "$db_subnetgroups_to_delete" != "" ]; then
    echo "${db_subnetgroups_to_delete}" |
      xargs -n 1 -I {} \
        aws rds delete-db-subnet-group \
          --db-subnet-group-name {}
  fi
}

clean_security_groups() {
  echo "Current security groups"
  aws ec2 describe-security-groups | \
    jq -r '
      .SecurityGroups[] |
        select(.GroupName | startswith("build-test-")) |
        "\(.GroupName) \(.GroupId)"
    '

  sg_to_delete="$(
    aws ec2 describe-security-groups | \
      jq -r '
        .SecurityGroups[] |
          select(.GroupName | startswith("build-test-")) |
          .GroupId
      '
  )"

  if [ "$sg_to_delete" != "" ]; then
    echo "${sg_to_delete}" |
      xargs -n 1 -I {} \
        aws ec2 delete-security-group \
        --group-id {}
  fi
}

clean_parameter_groups() {
  echo "Current db parameter groups"
  aws rds describe-db-parameter-groups | \
    jq -r '
      .DBParameterGroups[] |
        select(.DBParameterGroupName | startswith("build-test-")) |
        "\(.DBParameterGroupName) \(.Description)"
    '

  db_parametergroups_to_delete="$(
    aws rds describe-db-parameter-groups | \
      jq -r '
        .DBParameterGroups[] |
          select(.DBParameterGroupName | startswith("build-test-")) |
          .DBParameterGroupName
      '
  )"

  if [ "$db_parametergroups_to_delete" != "" ]; then
    echo "${db_parametergroups_to_delete}" |
      xargs -n 1 -I {} \
        aws rds delete-db-parameter-group \
          --db-parameter-group-name {}
  fi
}

clean_instances
clean_subnet_groups
clean_security_groups
clean_parameter_groups # added
