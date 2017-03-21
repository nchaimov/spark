#!/bin/bash

if [ -z "$JAVA_HOME" ] ; then
    export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
fi
export PATH="$JAVA_HOME/bin:$PATH"

MASTER=$1
HOST=$(hostname)
if [ "$HOST" == "$MASTER" ]; then
        $SPARK_HOME/sbin/start-master.sh
fi

$SPARK_HOME/sbin/start-slave.sh spark://$MASTER:7077 -c $SPARK_WORKER_CORES

tail -f /dev/null #wait forever
