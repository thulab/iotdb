#!/bin/sh

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

TSFILE_CONF=${TSFILE_HOME}/conf
TSFILE_LOGS=${TSFILE_HOME}/logs


if [ -f "$TSFILE_CONF/tsfile-env.sh" ]; then
    . "$TSFILE_CONF/tsfile-env.sh"
else
    echo "can't find $TSFILE_CONF/tsfile-env.sh"
fi


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

CLASSPATH=""
for f in ${TSFILE_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=cn.edu.thu.tsfiledb.service.Daemon

"$JAVA" -DTSFILE_HOME=${TSFILE_HOME} -Dlogback.configurationFile=${TSFILE_CONF}/logback.xml $TSFILEDB_DERBY_OPTS $TSFILEDB_JMX_OPTS -Dname=tsfiledb\.IoTDB -cp "$CLASSPATH" "$MAIN_CLASS"


exit $?