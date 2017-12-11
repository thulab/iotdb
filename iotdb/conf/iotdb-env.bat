@echo off

set LOCAL_JMX=no

set JMX_PORT=31999

if "%LOCAL_JMX%" == "yes" (
		set IOTDB_JMX_OPTS="-Diotdb.jmx.local.port=%JMX_PORT%" "-Dcom.sun.management.jmxremote.authenticate=false" "-Dcom.sun.management.jmxremote.ssl=false"
	) else (
		set IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote" "-Dcom.sun.management.jmxremote.authenticate=false"  "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.port=%JMX_PORT%"
	)
set IOTDB_DERBY_OPTS= "-Dderby.stream.error.field=cn.edu.tsinghua.iotdb.auth.dao.DerbyUtil.DEV_NULL"

set IOTDB_HEAP_OPTS=-Xmx1G -Xms1G