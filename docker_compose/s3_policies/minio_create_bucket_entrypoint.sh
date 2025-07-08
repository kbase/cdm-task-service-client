#!/bin/bash

printf "\n*** starting minio bucket & user setup ***\n"

mc alias set minio $MINIO_URL $MINIO_USER $MINIO_PWD

# make cts-logs bucket
if ! mc ls minio/cts-logs 2>/dev/null; then
  mc mb minio/cts-logs && echo 'Bucket cts-logs created'
else
  echo 'bucket cts-logs already exists'
fi

# make cts-data bucket
if ! mc ls minio/cts-data 2>/dev/null; then
  mc mb minio/cts-data && echo 'Bucket cts-data created'
else
  echo 'bucket cts-data already exists'
fi

# create policies
mc admin policy create minio cdm-task-service-read-write-policy /s3_policies/cdm-task-service-read-write-policy.json

# make CTS user
mc admin user add minio $MINIO_CTS_USER $MINIO_CTS_PWD
mc admin policy attach minio cdm-task-service-read-write-policy --user=$MINIO_CTS_USER
echo 'CTS user and policy set'
