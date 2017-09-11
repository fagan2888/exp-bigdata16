import org.apache.spark._

object wordcount{
  def main(args: Array[String]){
    if( args.length < 2){
      System.out.println("format: wordcount inputPath outputPath")
      return 
    }
    val inputPath = args(0);
    val outputPath = args(1);

    val start = System.currentTimeMillis;

    val sc = new SparkContext( new SparkConf().setAppName("spark wordcount"));
    
    //loading
    val lines = sc.textFile(inputPath);
   // lines.count
   // val endLoading = System.currentTimeMillis
   // System.out.println("L time: " + (endLoading - start) /  1000.0 + " seconds");

    //computing
    // val result = lines.flatMap(_.split("\\W+")).map( (_,1) ).reduceByKey(_+_);
     val flatmap1 = lines.flatMap(_.split("\\W+"));
   // flatmap1.count
    //val F = System.currentTimeMillis
   // System.out.println("L&F time: " + (F - start) /  1000.0 + " seconds");
    
    val map1 = flatmap1.map( (_,1) );
     //map1.count
    //val M = System.currentTimeMillis
    //System.out.println("L&F&M time: " + (M - start) /  1000.0 + " seconds");
    val result = map1.reduceByKey(_+_);
    result.count
    //val result = lines.flatMap(_.split("\\W+").map( (_,1) )).reduceByKey(_+_);

    //val endComputing = System.currentTimeMillis
    // System.out.println("finish count and Computing time: " + (endComputing - endLoading ) /  1000.0 + " seconds");
    // System.out.println("L&F&M&R time: " + (endComputing - start) /  1000.0 + " seconds");

    //test
    // result.count
    // val endSecondCount= System.currentTimeMillis
    // System.out.println("finish count and Loading + Computing time: " + (endSecondCount - endComputing ) /  1000.0 + " seconds");

    //dumping
  //     result.saveAsTextFile(outputPath);
    //
    val end = System.currentTimeMillis
   // System.out.println("dumping time: " + (endDumping - endComputing) /  1000.0 + " seconds");
   System.out.println("total time: " + (end - start) /  1000.0 + " seconds");
  }
}

//spark wordcount streaming example
/*
object wordcount{

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[4]").setAppName("streaming wordcount")
    val ssc = new StreamingContext( conf, Seconds(5))
    val lines = ssc.socketTextStream(args(0), args(1).toInt );

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map( word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)

    wordCounts.print();

    ssc.start();
    ssc.awaitTermination()
  }

}
*/
