package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset

object RDD_G {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nrdd.RDD_G <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Follower").setMaster("local")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))
    
    // Read lines from input file and split each line with comma as delimiter
    // Consider second value from the split i.e, the user who is being followed as key 
    // and generate the output as (user, 1)
    // groupByKey: groups the users being followed and then it is aggregated using sum
    val counts = textFile.map{line => val pairs = line.split(",")
                          (pairs(1), 1)}.groupByKey()
                          .map{case(user, followers) => (user, followers.sum)}
                          
    println("\n-----Debug String-----")
    println(counts.toDebugString)
    
    counts.saveAsTextFile(args(1))
    
  }
}