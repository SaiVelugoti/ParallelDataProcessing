package pr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object PageRankRDD {

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

    val kValue = args(0).toInt
    val kSquare = kValue * kValue
    val initialPR = 1.toDouble / kSquare
    
    
    var Temp2: org.apache.spark.rdd.RDD[(String, Double)] = sc.emptyRDD
    var delta = 0.toDouble
    var danglingMassAdj = 0.toDouble
    var prSum = 0.toDouble
    var iter = 1
    var pageRankPerIteration = sc.parallelize(Seq((0, 1.0)))
    
    // Create linear Graph and initial pageRanks with 2 partitions
    var graphRDD: org.apache.spark.rdd.RDD[(String, String)] = sc.emptyRDD
    var pageRankRDD = sc.parallelize(Seq((0.toString(), 0.toDouble)))
    for (k <- 1 to kSquare) {
      pageRankRDD = pageRankRDD.union(sc.parallelize(Seq((k.toString(), initialPR)))).repartition(2)
      if (k.%(kValue).==(0))
        graphRDD = graphRDD.union(sc.parallelize(Seq((k.toString(), 0.toString())))).repartition(2)
      else
        graphRDD = graphRDD.union(sc.parallelize(Seq((k.toString(), (k + 1).toString())))).repartition(2)
    }

    // Update pagerank for 10 iterations by evenly distributing the dangling mass 
    for (iter <- 1 to 10) {
      Temp2 = graphRDD.join(pageRankRDD).map { r => r._2 }.reduceByKey(_ + _)
      delta = Temp2.lookup("0")(0)
      danglingMassAdj = delta / kSquare
      pageRankRDD = pageRankRDD.leftOuterJoin(Temp2)
        .map {
          case (v, (oldpr, Some(newpr))) =>
            if (v.equals(0.toString())) (v, 0.toDouble)
            else (v, newpr + danglingMassAdj)
          case (v, (oldpr, None)) =>
            if (v.equals(0.toString())) (v, 0.toDouble)
            else (v, danglingMassAdj)
        }
      prSum = pageRankRDD.values.sum()
      pageRankPerIteration = pageRankPerIteration.union(sc.parallelize(Seq((iter, prSum)))).repartition(2)
    }

    pageRankPerIteration.saveAsTextFile("IterationPageRank");
    pageRankRDD.saveAsTextFile("finalPageRanks")
  }
}