import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val inputPath = "hdfs://master:9000/datasets/graph/twitter-adj";
val outputPath= "hdfs://master:9000/sssp/spark-out";
val sourceId: VertexId = 0; // The ultimate source

//load
val startTime = System.currentTimeMillis
val textRDD = sc.textFile( inputPath )

//build graph
val edge = textRDD.flatMap(
  line => { 

    val indexOfTab = line.indexOf("\t");
    val id = Integer.parseInt( line.substring(0, indexOfTab) );
    val adjList = line.substring(indexOfTab + 1).split(" ");
    val nNbs = Integer.parseInt(adjList(0));
    adjList.slice(1, adjList.size).map(
        nb => {
            Edge(id, nb.toInt, 1.toFloat)
        }
    ) 
  }
)

val graph = Graph.fromEdges[Float, Float](edge, 0.toFloat)
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.toFloat else Float.MaxValue);

// compute
val sssp = initialGraph.pregel(Float.MaxValue)(
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr != Float.MaxValue && triplet.attr != Float.MaxValue && triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) // Merge Message
)

//dump
sssp.vertices.saveAsTextFile(outputPath);
