package pr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ when, sum }

object PageRankDS {

  import org.apache.spark.sql.SparkSession

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Page Rank DS").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path("IterationPageRank"), true)
      hdfs.delete(new org.apache.hadoop.fs.Path("finalPageRanks"), true)
    } catch { case _: Throwable => {} }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    if (args.length != 2) {
      logger.error("Usage:\ndset.PageRankDS <k-value> <output dir>")
      System.exit(1)
    }
    val kValue = Integer.parseInt(args(0))
    val kSquare = kValue * kValue
    val initialPR = 1.toDouble / kSquare

    var iterationPRSum = Seq((0, 1.0)).toDF("iteration", "pagerank sum")
    var prSum = 0.toDouble

    // Generate linear graph and initial page rank table
    var graphDF = Seq(("1", "2")).toDF("vertex", "destination")
    var pageRankDF = Seq(("0", 0.0)).toDF("vertex", "pagerank")

    for (k <- 1 to kSquare) {
      pageRankDF = pageRankDF.union(Seq((k.toString(), initialPR)).toDF())
    }

    for (k <- 2 to kSquare) {
      if (k.%(kValue).==(0))
        graphDF = graphDF.union(Seq((k.toString(), 0.toString())).toDF()).repartition(2)
      else
        graphDF = graphDF.union(Seq((k.toString(), (k + 1).toString())).toDF()).repartition(2)
    }

    for (iter <- 1 to 10) {
      val newtable = graphDF.join(pageRankDF, "vertex").select("destination", "pagerank")
        .groupBy("destination").sum("pagerank")
      val danglingEdgeMass = newtable.filter($"destination" === 0.toString())
        .select("sum(pagerank)").collectAsList()
      val weigthDistr = danglingEdgeMass.get(0).get(0).toString().toDouble / kSquare

      pageRankDF = pageRankDF.join(newtable, ($"vertex" === $"destination"), joinType = "left")
        .withColumnRenamed("sum(pagerank)", "newpagerank")
        .withColumn("pagerank", when($"newpagerank"isNull, weigthDistr).otherwise(($"newpagerank" + weigthDistr)))
        .withColumn("pagerank", when($"vertex" === 0.toString(), 0.0).otherwise($"pagerank"))
        .select("vertex", "pagerank");

      prSum = pageRankDF.agg(sum("pagerank")).collectAsList().get(0).get(0).toString().toDouble;
      iterationPRSum = iterationPRSum.union(Seq((iter, prSum)).toDF());
    }
    iterationPRSum.rdd.saveAsTextFile("IterationPageRank")
    pageRankDF.rdd.saveAsTextFile("finalPageRanks")
  }
}