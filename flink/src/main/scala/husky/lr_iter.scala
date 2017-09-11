package zzxx
import org.apache.flink.api.scala._
import org.apache.flink.api.common.io._
import org.apache.flink.ml.math._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions._
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

object BcLogisticRegression {

  case class TraingPoints (
    value : Int,
    vector : SparseVector 
  )

  class Train extends RichFlatMapFunction[TraingPoints, (Int, Float)] {
    var curw = new Array[Float](100) 

    override def open(config: Configuration):Unit = {
      //I got a list here
      for (i <- getRuntimeContext().getBroadcastVariable[(Int, Float)]("w")) {
        curw(i._1) = i._2
      }  
    }

    def flatMap(point: TraingPoints, collect: Collector[(Int, Float)]):Unit = {
      val dot = point.vector.dot(curw).toFloat
      val mul = (point.value - 1 / (1 + math.exp(-dot))).toFloat * 0.1 
      for (i <- point.vector.data)
        collect.collect((i._1, i._2 * mul.toFloat))
        //point.vector.data.map{case (idx, v) => (idx, v * mul)}
    }
  }

  case class SparseVector (data : Array[(Int, Float)]) {
    def dot(w: Array[Float]):Float = {
      data.map{case (idx, v) => v * w(idx)}.reduce((a, b) => a + b)
    }
  }


    def main(args: Array[String]) {
      if (args.length != 4 && args.length != 5 ) {
        System.out.println("Usage: flink_lr inputPath outputPath alpha num_iter [vectorSize]")
        return
      }
      val env = ExecutionEnvironment.getExecutionEnvironment
      val inputPath = args(0)
      val outputPath = args(1)
      val alpha = args(2).toFloat
      val num_iter = args(3).toInt
      val txt = env.readTextFile(inputPath)
      //read input, the following is data format
      //V P1:V1 P2:V2 P3:V3 ...
      //V & V1 & V2 & V3 ... is Int and is either 0 or 1
      //P1 & P2 & P3 ... is positive Int
      val points = txt.map{ line => {
        val dat = line.split("\\s")
        val vec = dat.slice(1, dat.size).map {
          v => {
            val pair = v.split(":")
            (pair(0).toInt, pair(1).toFloat)
          }
        }
        TraingPoints(dat(0).toInt, SparseVector(vec))
      }}
      points.count()
      val vecSize = if (args.length == 5) args(4).toInt else {
        val maxIdx = points.flatMap {
          p => p.vector.data.map{case (a, b) => a}
        }.reduce((a, b) => if (a > b) a else b)
        maxIdx.collect()(0) + 1
      }
      var init = new Array[(Int, Float)](vecSize)
      for (i <- 0 to vecSize - 1)
        init(i) = (i, 0.f)
        val w = env.fromCollection(init)
        val startTime = System.currentTimeMillis();
        val finalw = w.iterate(num_iter) {
          curw => {
            val grads = points.flatMap(new Train()).withBroadcastSet(curw, "w")
            curw.union(grads).groupBy(0).sum(1)
          }
        }
        finalw.count()
        //env.execute("LR")
        val stopTime = System.currentTimeMillis();
        finalw.writeAsText(outputPath)
        System.out.println("Program Run Time: " + ((stopTime - startTime) / 1000) + "s.");
    }
}
