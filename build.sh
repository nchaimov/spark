#!/bin/bash 
./build/mvn -Pyarn -Phadoop-2.4 -Phive -Phive-thriftserver -Dhadoop-version=2.4.1-INSTRUMENT-SNAPSHOT -DskipTests=true -Dmaven.javadoc.skip=true package install
