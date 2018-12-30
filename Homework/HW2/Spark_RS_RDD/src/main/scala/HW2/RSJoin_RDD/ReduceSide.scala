package HW2.RSJoin_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object ReduceSide {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    val maxValue = 40000
    if (args.length != 2) {
      logger.error("Usage:\nrdd.ReduceSide <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Follower")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. 
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }

    val textFile = sc.textFile(args(0))
    
    // Read lines from input file and split each line with comma as delimiter 
    // Check for Max_value and emit user and follower 
    val edges_1 = textFile.map{line => val pair = line.split(",")
                          if((pair(1).toInt)< maxValue && (pair(0).toInt < maxValue))
                          (pair(1), pair(0)) else ("","")}
    
    // Read lines from input file and split each line with comma as delimiter 
    // Check for Max_value and emit follower and user for self join    
    val edges_2 = textFile.map{line => val pair = line.split(",")
                          if((pair(1).toInt)< maxValue && (pair(0).toInt < maxValue))
                          (pair(0), pair(1)) else ("","")}
    
    // Join edges_1 and edges_2 to find Path2 
    val path2RDD = edges_1.join(edges_2).map{case(middle, (from, to)) => if(from != "" && from != to)(from,to) else ("", "")}
    
    // Triangle
    var TriangleRDD = path2RDD.join(edges_1).map{case(middle, (from, to)) => if(from != "" && from == to)(from,to) else ("", "")}
    TriangleRDD = TriangleRDD.filter{case(from, to) => if(from!=""){true}else{false}}
    
    val count = TriangleRDD.count()/3

    sc.parallelize(Seq(count)).saveAsTextFile(args(1))

  }
}