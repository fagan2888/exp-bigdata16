$SPARK_HOME/bin/spark-shell --master spark://master:7077 \
--driver-memory 10g \
--driver-cores 4 \
--executor-cores 6 \
--executor-memory 10g < ./als.scala
