package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset

object RDD_A {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nrdd.RDD_A <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Follower").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. 
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }

    val textFile = sc.textFile(args(0))

    // Read lines from input file and split each line with comma as delimiter
    // Consider second value from the split i.e, the user who is being followed as key 
    // and generate the output as (user, 1)
    // aggregateByKey: performs the required aggregate operation
    val counts = textFile.map{line => val pairs = line.split(",")
                          (pairs(1), 1)}.
                          aggregateByKey(0)((key, value) => 
                            key + value, (value1, value2) => value1 + value2)

    println("\n-----Debug String-----")
    println(counts.toDebugString)

    counts.saveAsTextFile(args(1))

  }
}