inputPath="hdfs://master:9000/wordcount/wordcount_198g"
outputPath="hdfs://master:9000/wordcount/flink-out"

hadoop fs -rm -r $outputPath
$FLINK_HOME/bin/flink run \
-c husky.wordcount \
../target/flink-experiment-0.9.1.jar \
$inputPath \
$outputPath
