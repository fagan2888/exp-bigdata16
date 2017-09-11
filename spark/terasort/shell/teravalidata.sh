inputPath="hdfs://master:9000/terasort/spark-out"
hadoop fs -rm -r $outputPath

$SPARK_HOME/bin/spark-submit \
--class TeraValidate \
--master spark://master:7077 \
--driver-memory 10g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
../target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar \
$inputPath \
$outputPath 
