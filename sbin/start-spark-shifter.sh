#!/bin/bash

#module load java
export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
export PATH="$JAVA_HOME/bin:$PATH"

MASTER=$1
NUM_CORES=$2
HOST=$(hostname)
if [ "$HOST" == "$MASTER" ]; then
        $SPARK_HOME/sbin/start-master.sh
fi

$SPARK_HOME/sbin/start-slave.sh spark://$MASTER:7077 -c $NUM_CORES 

tail -f /dev/null #wait forever
