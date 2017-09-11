inputPath="hdfs://master:9000/sssp/livej-adj-8m"
outputPath="hdfs://master:9000/sssp/flink-out"

hadoop fs -rm -r $outputPath
$FLINK_HOME/bin/flink run \
-c husky.ssspDelta \
../target/flink-experiment-0.9.1.jar \
$inputPath \
$outputPath \
0
