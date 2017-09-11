outputPath="hdfs://master:9000/terasort/terasort-18g"
hadoop dfs -rm $outputPath
$SPARK_HOME/bin/spark-submit \
--class BlockTeraGen \
--master spark://master:7077 \
--driver-memory 10g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
../target/spark-terasort-1.0-SNAPSHOT-jar-with-dependencies.jar \
18g $outputPath \
60000000
