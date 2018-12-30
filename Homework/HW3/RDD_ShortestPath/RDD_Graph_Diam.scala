package graph_diam

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import scala.collection.mutable.ListBuffer

object RDD_Graph_Diam {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\npr.PageRankRDD <k-value> <output-dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank RDD").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path("IterationPageRank"), true)
      hdfs.delete(new org.apache.hadoop.fs.Path("finalPageRanks"), true)
    } catch { case _: Throwable => {} }

    val source = "3"
    val edgesFile = sc.textFile(args(0), 2)

    val edgesRDD = edgesFile.map { line =>
      val pair = line.split(",")
      (pair(0), pair(1))
    }

    val graphRDD = edgesRDD.groupBy(_._1)
      .map {
        case (from, to) =>
          if (from.equals(source)) (from, (to.map(_._2), "T"))
          else (from, (to.map(_._2), "F"))
      }

    var distanceRDD = graphRDD.mapValues {
      case (v) =>
        if (v._2.compareTo("T") == 0) 0.toDouble
        else Double.PositiveInfinity
    }

    var oldDistRDD = distanceRDD
    var complete = false
    var count = 0.toDouble

    while (!complete) {
      distanceRDD = graphRDD.join(oldDistRDD).flatMap(e => extractVertices(e)).reduceByKey(Math.min(_, _))
      count = oldDistRDD.join(distanceRDD).map(e => ((e._2._1 != e._2._2), 1)).filter(_._1 == true).count()
      if (count > 0.0) {
        complete = true
        oldDistRDD = distanceRDD
      } else {
        complete = false
      }
    }

  }

  def extractVertices(e: (String, ((Iterable[String], String), Double))): Iterable[(String, Double)] = {
    val from = e._1
    val dist = e._2._2
    var list_of_edges = new ListBuffer[String]()
    list_of_edges += from;
    for (vertex <- e._2._1._1) {
      list_of_edges += vertex
    }
    val ret = list_of_edges.map { v =>
      if (v.equals(from)) (v, dist)
      else (v, dist + 1)
    }
    return ret
  }
}