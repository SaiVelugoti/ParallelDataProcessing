package HW2.RSJoinDataSet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object ReduceSide {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.col

  val session: SparkSession = SparkSession.builder().appName("RS Spark DSet").config("spark.master", "local").getOrCreate()
  import session.implicits._

  session.conf.set("spark.sql.crossJoin.enabled", "true")

  def main(args: Array[String]) {
    val maxValue = 7
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nHW2.RSJoinDataSet.ReduceSide <input dir> <output dir>")
      System.exit(1)
    }
    
    // Read input file as DataSet and set column names as "Follower" and "User"
    val edgesDS = 
      session.read.option("inferSchema", "true").csv(args(0)).toDF("Follower", "User")
    
    // Filter Edges that are less than the mav value
    val filteredEdges = 
      edgesDS.filter($"_c0" < maxValue && $"_c1" < maxValue)
    
    // create two copies of filtered edges dataset to self join
    val Edges1 = filteredEdges.toDF("Follower1", "User1")
    val Edges2 = filteredEdges.toDF("Follower2", "User2")
    
    // Join edges to get Path2 : User from Edges1 to be Follower in Edges2
    // Save the path in Path2 dataset
    val Path2 = 
      Edges1.join(Edges2, ($"User1" === $"Follower2") && ($"Follower1" !== $"User2"))
      .select($"Follower1", $"User1", $"User2")
    
    // Join Path2 edges with originally filtered edges to find the closed triangle
    // Save the three nodes/vertices of each possible triangle to triangleDS
    val triangleDS = 
      Path2.join(filteredEdges, ($"User2" === $"Follower") && ($"Follower1" === $"User"))
      .select($"Follower1", $"User1", $"User2")
    
    triangleDS.show()
    // Number of triangles = (numberOfRecords in traingleDS)/3
    logger.info("Number of Triangles: "+ triangleDS.count()/3)
    session.stop()

  }
}