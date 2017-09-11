import org.apache.spark._
import scala.Tuple2
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

object grep{
  def main(args: Array[String]){
    if( args.length < 3){
      System.out.println("format: grep inputPath outputPath pattern")
      return 
    }
    val inputPath = args(0);
    val outputPath = args(1);
    val pattern = args(2);

    val start = System.currentTimeMillis;

    //loading
      //sc for Text serialization
    val sparkConf = new SparkConf().setAppName("spark grep");
    sparkConf.registerKryoClasses( Array(classOf[Text])  )
    val sc = new SparkContext( sparkConf );
    // val sc = new SparkContext( new SparkConf().setAppName("spark grep") );
    val lines= sc.newAPIHadoopFile[Text, Text, HadoopTeraInputFormat](inputPath);
   // val lines= sc.textFile(args(0));
   // val match : String = args(2);
    //val numMatch = input.filter(line=>line.contains(match)).count();
    //println("%s lines in %s contains %s".format(numMatch,args(0),match));
    // val job = new Job( new Configuration() );
    // FileInputFormat.setInputPaths(job, new Path(inputPath) )
    // job.setInputFormatClass( classOf[HadoopTeraInputFormat] )
    // val lines= sc.newAPIHadoopRDD( job.getConfiguration(), classOf[HadoopTeraInputFormat], classOf[Text], classOf[Text])
    
    // lines.count;
    // val endLoading = System.currentTimeMillis
    // System.out.println("loading time: " + (endLoading - start) /  1000.0 + " seconds");

    //computing
     val result = lines.filter(_.toString.contains(pattern));

     //result.count;
     val numResult=result.count;
    // val endComputing = System.currentTimeMillis
    // System.out.println("computing time: " + (endComputing - endLoading) /  1000.0 + " seconds");

    //dumping
     result.saveAsNewAPIHadoopFile[HadoopTeraOutputFormat](outputPath);
    val numRecords = lines.count
    System.out.println("number of input records: " +numResult+ numRecords);

    val endDumping = System.currentTimeMillis
    // System.out.println("dumping time: " + (endDumping - endComputing) /  1000.0 + " seconds");

    System.out.println("total time: " + (endDumping - start) /  1000.0 + " seconds");
  }
}
