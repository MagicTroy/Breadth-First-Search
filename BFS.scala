import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexId, Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.compat.Platform._

/**
  * Created by troy on 3/14/16.
  *
  * @version 1.0
  *
  * @author Sixun Ouyang
  **/
object BFS{

  def main(args: Array[String]): Unit = {
    println("Start main")
    val total_time: Double= currentTime
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster("local").setAppName("closenessCentrality")

    val sc = new SparkContext(conf)
    val path: String = "/home/troy/Clustering Coeffiency/data/s10.csv"

    val graph = loadData(path, sc)
    calculate_bfs(graph)
    val time_cost: Double = currentTime
    println("total time cost: " + (time_cost - total_time) / 1000)
  }

  /**
    * load and convert data into a desier format
    * */
  def loadData(path: String, sc: SparkContext): Graph[Double, Double] = {
    /**load from file*/
    val raw: RDD[Edge[Double]] = sc.textFile(path).map{ s =>
      val parts = s.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, 1.0)
    }.distinct
    val convert : RDD[Edge[Double]] = raw.filter{ s =>
      s.srcId != s.dstId
    }

    /**build graph*/
    val raw_graph : Graph[Double, Double] =
      Graph.fromEdges(convert, 0.0)
    raw_graph.cache()
    raw_graph
  }

  def calculate_bfs(graph: Graph[Double, Double]) = {
    val vertices_array:Array[(VertexId, Double)] = graph.vertices.collect()
    val map_bfs: Map[Long, Array[(Long, Double)]] = (vertices_array.map(_._1), vertices_array.map{x => shortest_path(createInputGraph(x._1, graph)).vertices.collect()}).zipped.map(_ -> _).toMap
  }

  def createInputGraph(x:VertexId, graph: Graph[Double, Double]): Graph[Double, Double] ={
    graph.mapVertices((id, _) => if (id == x) 0.0 else Double.PositiveInfinity)
  }
  
  /** calculate sssp
 *
    *  @param graph
    *  @return result graph
    */
  def shortest_path(graph: Graph[Double, Double]): Graph[Double,Double] = {

    val sssp = graph.pregel(Double.PositiveInfinity)(

      (id, dist, newDist) => math.min(dist, newDist),

      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      (a, b) => math.min(a, b)
    )
    sssp
  }
}
