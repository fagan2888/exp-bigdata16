inputPath="hdfs://master:9000/pagerank/livej-adj-8m"
outputPath="hdfs://master:9000/pagerank/flink-out"

hadoop fs -rm -r $outputPath
$FLINK_HOME/bin/flink run \
-c husky.pagerank \
../target/flink-experiment-0.9.1.jar \
$inputPath \
$outputPath \
20
