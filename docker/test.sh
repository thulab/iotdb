#!/bin/bash
SCRIPT_DIR=$(dirname $(readlink -e $0))
. ${SCRIPT_DIR}/image

if [ $# -lt 1 ]; then
    echo "Usage: $0 <env_file>"
    exit -1
fi

set -ex
ENV_FILE=$1
shift
echo $*
