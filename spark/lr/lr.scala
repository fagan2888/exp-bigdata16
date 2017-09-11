import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val inputPath = "hdfs://master:9000/datasets/ml/HIGGS"
val outputPath = "hdfs://master:9000/lr/spark-out"

// load
val data = MLUtils.loadLibSVMFile(sc, inputPath)

val training = data;
val test = data;

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Get evaluation metrics.
val metrics = new MulticlassMetrics(predictionAndLabels)
val precision = metrics.precision
println("Precision = " + precision)

// Save and load model

model.save(sc, outputPath)
