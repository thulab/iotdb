#!/bin/bash
set -e
SCRIPT_DIR=$(dirname $(readlink -e $0))
. ${SCRIPT_DIR}/image

docker build -t ${IMAGE} \
  --build-arg buildtime="$(date +"%Y-%m-%d %T")"  \
  --build-arg owner=${IMAGE_OWNER} \
  --build-arg env_para="$(cat $SCRIPT_DIR/env-list.yaml)" \
  ${SCRIPT_DIR}/../

echo -e "\n$IMAGE"
docker inspect --format "buildtime={{.Config.Labels.buildtime}} owner={{.Config.Labels.owner}}" $IMAGE
echo "env_para="
docker inspect --format "{{.Config.Labels.env_para}}" $IMAGE

