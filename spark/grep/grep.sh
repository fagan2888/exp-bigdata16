inputPath="hdfs://master:9000/terasort/terasort-18g"
outputPath="hdfs://master:9000/grep/flink-out"

hadoop fs -rm -r $outputPath
$SPARK_HOME/bin/spark-submit \
--class grep \
--master spark://master:7077 \
--driver-memory 10G \
--driver-cores 4 \
--executor-memory 10G \
--executor-cores 6 \
../target/scala-2.10/experimental-track_2.10-1.0.jar \
$inputPath \
$outputPath \
012
