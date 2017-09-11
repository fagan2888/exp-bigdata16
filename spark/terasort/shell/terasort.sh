inputPath="hdfs://master:9000/terasort/terasort-18g"
outputPath="hdfs://master:9000/terasort/spark-out"
hadoop fs -rm -r $outputPath

$SPARK_HOME/bin/spark-submit \
--class TeraSort \
--master spark://master:7077 \
--conf spark.shuffle.spill=false \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-memory 10g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
../target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar \
$inputPath \
$outputPath
