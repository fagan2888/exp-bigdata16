package husky
import org.apache.flink.api.scala._
import org.apache.flink.api.common.io._
import org.apache.flink.util._
import org.apache.flink.api.java.aggregation.Aggregations.SUM

import java.lang.System;

object pagerank{
  case class Vertex( id: Int, rank: Float){
    override def toString(): String = {
      return (id + "\t" + rank)
    }
  }
  case class AdjList( id: Int, adjList: Array[Int])

  def main(args: Array[String]){

    var startTime = System.currentTimeMillis();
    if(args.length < 3){
      System.out.println("pagerank inputPath outputPath iteration")
      return 
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val iteration = args(2).toInt
    var resetProb = 0.15.toFloat
    if( args.length > 4) resetProb = args(3).toFloat

    val env = ExecutionEnvironment.getExecutionEnvironment

    // load
    val textFile = env.readTextFile(inputPath)

    // compute
    val vertices: DataSet[Vertex] = textFile.map( line => {
      val id = line.split("\t")(0).toInt
      Vertex(id, 0.15.toFloat)
    })

    val adjLists: DataSet[AdjList] = textFile.map( line => {
      val idAndList = line.split("\t")
      val id = idAndList(0).toInt
      val nbs = idAndList(1).split(" ")
      val adjList = nbs.slice(1, nbs.size).map( nbId => nbId.toInt)
      AdjList(id, adjList)
    })

    val finalRanks = vertices.iterate(iteration){
      currentRank => 
        val newRank =currentRank.join(adjLists).where("id").equalTo("id"){
          (vertex, nbs, out: Collector[ Vertex ]) => {
            val partialValue = 
              vertex.rank / nbs.adjList.length
            for( nbId <- nbs.adjList ){
              out.collect(Vertex( nbId, partialValue))
            }
          }
        }
          .groupBy("id").aggregate(SUM, "rank")
          .map(vertex => Vertex(vertex.id, resetProb + (1 - resetProb) * vertex.rank))

          newRank

    }

    // dump
    finalRanks.writeAsText(outputPath)

    env.execute("flink pagerank")
  }
}
