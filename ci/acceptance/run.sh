#!/bin/bash

set -e
set -u

# Set defaults
TIMEOUT=${TIMEOUT:-600}
INTERVAL=${INTERVAL:-5}
MEMORY_LIMIT=${MEMORY_LIMIT:-"120M"}

check_service() {
  elapsed=${TIMEOUT}
  until [ $elapsed -le 0 ]; do
    status=$(cf service ${SERVICE_INSTANCE_NAME})
    if echo ${status} | grep "Status: create succeeded"; then
      return 0
    elif echo ${status} | grep "Status: create failed"; then
      return 1
    fi
    let elapsed-=${INTERVAL}
    sleep ${INTERVAL}
  done
  return 1
}

teardown() {
  cf delete -f ${APP_NAME}
  cf delete-service -f ${SERVICE_INSTANCE_NAME}
}
trap teardown EXIT

cf api ${CF_API_URL}
cf auth ${CF_USERNAME} ${CF_PASSWORD}
cf target -o ${CF_ORGANIZATION} -s ${CF_SPACE}

cf create-service ${SERVICE_NAME} ${PLAN_NAME} ${SERVICE_INSTANCE_NAME}
if ! check_service; then
  echo "Failed to create service ${SERVICE_NAME}"
  exit 1
fi

path=$(cd $(dirname $0); pwd -P)
cf push ${APP_NAME} -p ${path} -m ${MEMORY_LIMIT} --no-start

cf set-env ${APP_NAME} SERVICE_NAME ${SERVICE_NAME}
cf bind-service ${APP_NAME} ${SERVICE_INSTANCE_NAME}
cf start ${APP_NAME}

url=$(cf app ${APP_NAME} | grep "urls: " | awk '{print $2}')
status=$(curl -w "%{http_code}" "https://${url}/${URL_PATH}")
if [ "${status}" != "200" ]; then
  echo "Unexpected status code ${status}"
  cf logs ${APP_NAME} --recent
  exit 1
fi

cf unbind-service ${APP_NAME} ${SERVICE_INSTANCE_NAME}
