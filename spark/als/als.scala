import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import scala.math._

val inputPath="hdfs://master:9000/datasets/ml/netflix"
val rank = 20
val numIteration = 4 
val lambda = 0.1

// load
val data = sc.textFile(inputPath)
val ratings = data.map(_.split('\t') match {
  // case Array(user, item, rate, t) =>
  case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
})


// Build the recommendation model using ALS
val model = ALS.train(ratings, rank , numIteration, lambda) 
// Evaluate the model on rating data
var usersProducts = ratings.map { case Rating(user, product, rate) =>
  (user, product)
}
val predictions = 
  model.predict(usersProducts).map { case Rating(user, product, rate) => 
    ((user, product), rate)
  }
val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
  ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
  var err = (r1 - r2)
  err * err
}.mean()
val RMSE = sqrt(MSE)
println("Mean Squared Error = " + RMSE)
