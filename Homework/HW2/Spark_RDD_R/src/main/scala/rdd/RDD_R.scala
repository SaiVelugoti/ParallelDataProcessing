package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset

object RDD_R {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nrdd.RDD_R <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Follower").setMaster("local")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))
    
    // Read lines from input file and split each line with comma as delimiter
    // Consider second value from the split i.e, the user who is being followed as key 
    // and generate the output as (user, 1)
    // reduceByKey function performs the required aggregate operation
    val counts = textFile.map{line => val pairs = line.split(",")
                          (pairs(1), 1)}.reduceByKey(_ + _)
                          
    println("\n-----Debug String-----")
    println(counts.toDebugString)
    
    counts.saveAsTextFile(args(1))
    
  }
}