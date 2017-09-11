package husky
import org.apache.flink.api.scala._
import org.apache.flink.util._

object ssspDelta{
  def main(args: Array[String]){
    if( args.length < 2){
      System.out.println("sssp inputPath outputPath sourceId")
      return 
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val sourceId = args(2).toLong
    var maxIterations = 10000
    if(args.length > 3) maxIterations = args(3).toInt

    var env = ExecutionEnvironment.getExecutionEnvironment

    val adjLists = env.readTextFile(inputPath)

    val vertices: DataSet[(Long, Double)]= adjLists.map( line => {
      var id = line.split("\t")(0).toLong
      if( id == sourceId ) (id, 0.toDouble)
      else (id, Double.PositiveInfinity)
    })

    val edges = adjLists.flatMap( line => {
      val idAndList = line.split("\t")
      val id = idAndList(0).toLong
      val nbs = idAndList(1).split(" ")
      nbs.slice(1, nbs.size).map( nbId => (id, nbId.toLong))
    })

    val verticesWithDistance = vertices.iterateDelta(vertices, maxIterations, Array(0)) {
      (s, ws) =>

        // apply the step logic: join with the edges
        val allNeighbors = ws.filter(_._2 != Double.PositiveInfinity)
                             .join(edges).where(0).equalTo(0) { (vertex, edge) =>
                                (edge._2, vertex._2 + 1.0)
                              }

        // select the minimum neighbor
        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Long, Double)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }
        // iter = iter + 1
        (updatedComponents, updatedComponents)
    }
    verticesWithDistance.writeAsText(outputPath)
    env.execute("flink ssspDelta")
  }
}
