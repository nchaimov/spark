#!/bin/bash

export EXECUTOR_CORES=24
export EXECUTOR_MEMORY="32G"
export EDGE_PARTITIONS=256
export PAGERANK_ITERATIONS=10
export PART_STRATEGY=EdgePartition2D

echo -e "Executor cores:\t$EXECUTOR_CORES"
echo -e "Executor memory:\t$EXECUTOR_MEMORY"
echo -e "Edge partitions:\t$EDGE_PARTITIONS"
echo -e "PageRank iterations:\t$PAGERANK_ITERATIONS"
echo -e "Partition strategy:\t$PART_STRATEGY"

# Wait to make sure yarn has started
sleep 2

# run_pagerank(num_executors)
function run_pagerank {
    echo "*** Running PageRank with num-executors = $1"

    $SPARK_PATH/bin/spark-submit --master yarn-client --num-executors $1 \
        --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEMORY \
        target/scala-2.10/livejournal-page-rank_2.10-1.0.jar $PBS_O_WORKDIR/soc-LiveJournal1.txt \
        --numEPart=$EDGE_PARTITIONS --numIter=$PAGERANK_ITERATIONS --partStrategy=$PART_STRATEGY

    echo "*** Completed PageRank with num-executors = $1"
}

for i in `seq 1 10`;
do
    run_pagerank $i
done    


