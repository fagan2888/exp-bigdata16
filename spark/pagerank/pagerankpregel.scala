import org.apache.spark._
import scala.reflect.ClassTag
import org.apache.spark.graphx._

def runPageRank[VD: ClassTag, ED: ClassTag](
  graph: Graph[VD, ED], maxIter:Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    val pagerankGraph: Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees){
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      .mapTriplets( e => 1.0 / e.srcAttr)
      .mapVertices( (id, deg) => 0.0 )
      .cache()

      def compute(id: VertexId, attr: Double, msgSum: Double) : Double = {
        resetProb + ( 1.0 - resetProb ) * msgSum
      }

      def sendMessages(edge: EdgeTriplet[ Double, Double]) = {
        Iterator( (edge.dstId, edge.srcAttr * edge.attr) );
      }
      def combiner( a: Double,b: Double): Double = a + b

      // Pregel(pagerankGraph, 0.0, maxIter, activeDirection = EdgeDirection.Out)(compute, sendMessages, combiner)
      Pregel(pagerankGraph, 0.0, maxIter, activeDirection = EdgeDirection.Out)(compute, sendMessages, combiner)
}

val maxIter = 20;
val resetProb = 0.15;
val inputPath = "hdfs://master:9000/datasets/graph/livej-snap-adj";
val outputPath = "hdfs://master:9000/pagerank/spark-out";

val textRDD = sc.textFile(inputPath)
val edge = textRDD.flatMap(line => { 
  val numbers = line.split("\t"); 
  val id = numbers(0).toLong; 
  val nbs = numbers(1).split(" "); 
  nbs.slice(1, nbs.size).map(nb => Edge(id, nb.toLong, 0.toDouble))
})
val graph = Graph.fromEdges[Double, Double](edge, 0)

val result = runPageRank(graph, maxIter, resetProb)
result.vertices.saveAsTextFile(outputPath)
