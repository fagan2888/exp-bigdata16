$SPARK_HOME/bin/spark-shell --master spark://master:7077 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.shuffle.spill=false \
--driver-memory 10g \
--driver-cores 4 \
--executor-cores 6 \
--executor-memory 10g < ./sssp-adj.scala 
