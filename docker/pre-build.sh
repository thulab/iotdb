#!/bin/bash
SCRIPT_DIR=$(dirname $(readlink -e $0))
. $SCRIPT_DIR/image

set -x
echo "do something here before docker build"
