inputPath="hdfs://master:9000/wordcount/wordcount_162g"
outputPath="hdfs://master:9000/wordcount/spark-out"

hadoop fs -rm -r $outputPath
$SPARK_HOME/bin/spark-submit \
--class wordcount \
--master spark://master:8088 \
--conf spark.shuffle.spill=false \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-memory 10G \
--driver-cores 4 \
--executor-memory 10G \
--executor-cores 6 \
../target/scala-2.10/experimental-track_2.10-1.0.jar \
$inputPath \
$outputPath 
