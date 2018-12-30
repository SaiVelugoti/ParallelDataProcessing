package dset

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset


object DSETBackup {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.col
  
  val session: SparkSession = SparkSession.builder().appName("DSETApp").config("spark.master", "local").getOrCreate()
  import session.implicits._
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ndset.DSETImpl <input dir> <output dir>")
      System.exit(1)
    }
    
    //    val conf = new SparkConf().setAppName("Combine DSET")
//    val conf = new SparkConf().setAppName("Combine DSET").setMaster("local")
//    val sc = new SparkContext(conf)
    
    val textFile = session.sparkContext.textFile(args(0))
    val ds = session.createDataset(textFile)
    ds.show()
    
    
    //    output.writeStream. .save(args(1))
//    output.saveAsTextFile(args(1))
//    logger.info(ds.groupBy("_c1").count().show())

    //   ----Alternate Dataset creation
    //    val textFile = session.sparkContext.textFile(args(0))
    //    val ds = session.createDataset(textFile)
    //    ds.show()
    //    ds.saveAsTextFile(args(1))
    
    
//val sqlContext = new SQLContext(sc)
//val pointsTrainDf =  sqlContext.createDataFrame(textFile)
//val pointsTrainDs = pointsTrainDf.as[Edges]

    //    val sqlContext = new SQLContext(sc)
    //    val df = sqlContext.read
    //    .format("com.databricks.spark.csv")
    //    .option("header", "true") // Use first line of all files as header
    //    .option("inferSchema", "true") // Automatically infer data types
    //    .load("cars.csv")
    //    
    //    var session = SparkSession.builder().appName("DSETApp").master("local").getOrCreate()
    //    Dataset < Row > dataset = session.read().option("inferSchema", "true").csv(args(0)).toDF("Follower", "User")

    // Read lines from input file and split each line with comma as delimiter
    // Consider second value from the split i.e, the user who is being followed as key 
    // and generate the output as (user, 1)
    // reduceByKey function performs the required aggregate operation
//    val counts = textFile.map { line =>
//      val pairs = line.split(",")
//      (pairs(1), 1)
//    }.reduceByKey(_ + _)
//
//    println("\n\n-----Number of Partitions-----")
//    println(counts.getNumPartitions)
//
//    println("\n-----Storage Level-----")
//    println(counts.getStorageLevel)
//
//    println("\n-----Debug String-----")
//    println(counts.toDebugString)
//
//    counts.saveAsTextFile(args(1))
  }
}