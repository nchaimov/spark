#!/bin/bash

export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
export PATH="$JAVA_HOME/bin:$PATH"

MASTER=$1
HOST=$(hostname)
if [ "$HOST" == "$MASTER" ]; then
        $SPARK_HOME/sbin/start-master.sh
fi

$SPARK_HOME/sbin/start-slave.sh spark://$MASTER:7077 -c 32

tail -f /dev/null #wait forever
