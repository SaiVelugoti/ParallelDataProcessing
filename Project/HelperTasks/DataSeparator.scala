package HW3.Scala

import org.apache.log4j.LogManager
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Bucketizer, Normalizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object fraudDataSeparation {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    val conf = new SparkConf().setAppName("fraud").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("fraud")
      .getOrCreate()

    // read csv with header true
    var input = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load(args(0)).toDF()
    input.persist()
    import spark.implicits._

    val splits = input.randomSplit(Array(0.8, 0.2), seed = 20L)

    val train = splits(0).toDF().rdd
    val test = splits(1).toDF().rdd
    val trainRDD = train.map(e=>s"${e.mkString(",")}")
    trainRDD.saveAsTextFile(args(1))
    val testRDD = test.map(e=>s"${e.mkString(",")}")
    testRDD.saveAsTextFile(args(2))

  }
}