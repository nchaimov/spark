#!/bin/bash
#PBS -N SparkPageRank
#PBS -V
#PBS -q regular
#PBS -A m2018
#PBS -l mppwidth=240
#PBS -l walltime=00:55:00
#PBS -j oe
#PBS -m abe
#PBS -o SparkPageRank.$PBS_JOBID

cd $PBS_O_WORKDIR

export BDAS_PATH=/scratch1/scratchdirs/nchaimov/bdas-inst
export SCRIPTS_PATH=$BDAS_PATH/BDAS-Conf-Startup/scripts
export SPARK_PATH=$BDAS_PATH/spark

source $BDAS_PATH/env.sh

echo "Starting YARN for Spark..."
$SCRIPTS_PATH/start_spark_yarn $PBS_O_WORKDIR/run_pagerank_iters.sh
echo "YARN shut down"

