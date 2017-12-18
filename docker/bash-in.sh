#!/bin/bash
SCRIPT_DIR=$(dirname $(readlink -e $0))
. $SCRIPT_DIR/image

set -x
docker run -it --rm --entrypoint=/bin/bash $IMAGE
