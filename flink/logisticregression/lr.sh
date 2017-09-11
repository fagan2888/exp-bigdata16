inputPath="hdfs://master:9000/lr/in"
outputPath="hdfs://master:9000/lr/flink-out"

hadoop fs -rm -r $outputPath
$FLINK_HOME/bin/flink run \
-c zzxx.BcLogisticRegression \
../target/flink-experiment-0.9.1.jar \
$inputPath \
$outputPath \
0.01 \
20
