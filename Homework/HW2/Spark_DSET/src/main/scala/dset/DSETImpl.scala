package dset

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset

object DSETImpl {

  import org.apache.spark.sql.SparkSession

  val session: SparkSession = SparkSession.builder().appName("DSETApp").config("spark.master", "local").getOrCreate()
  import session.implicits._

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\ndset.DSETImpl <input dir> <output dir>")
      System.exit(1)
    }

    // Read from csv to dataset
    val ds = session.read.option("inferSchema", "true").csv(args(0))
    // Group by users being followed and count the number of records in each group
    val output = ds.groupBy("_c1").count()
    
    logger.info(output.explain(true))
    output.coalesce(1).write.format("csv").save(args(1))
  }
}