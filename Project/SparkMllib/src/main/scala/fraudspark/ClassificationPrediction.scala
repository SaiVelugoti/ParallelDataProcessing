package fraud

import org.apache.log4j.LogManager
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Bucketizer, Normalizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql._

import scala.util.Try


object ClassificationPrediction {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    val conf = new SparkConf().setAppName("fraud")// .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("fraud")
      .getOrCreate()

    import spark.implicits._

    // read input file and convert all values to double and create transaction objects
    var inputDF = parseNumber(sc.textFile(args(0)))
                              .map(inputline => Transaction(inputline(0),
                                                            inputline(1),
                                                            inputline(2),
                                                            inputline(3),
                                                            inputline(4),
                                                            inputline(5),
                                                            inputline(6),
                                                            inputline(7),
                                                            inputline(8),
                                                            inputline(9),
                                                            inputline(10))).toDF().cache()

    /* Forming the features vector by selecting the feature columns. We have considered
     * feature columns as all the columns other than the isFraud column */
                                                           
    val featureColumns = Array("step","ttype","amt","nameOrig",
                                "oldBalOrg","newBalOrg","nameDest",
                                "oldBalDest","newBalDest","isFlagged")

    val fea_classifier = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    
    /* Forms a features data frame with a new column features which is an array of all the values of 
     * the featureColumns selected above*/
    
    val featuresDF = fea_classifier.transform(inputDF)

    // Defining the isFraud column as the label by using a StringIndexer    
    val label_index = new StringIndexer().setInputCol("isFraud").setOutputCol("label")
    
    // Forming data frame with a new column label added to the featuresDF dataframe
    val labelDF = label_index.fit(featuresDF).transform(featuresDF)

    /* Splitting the data into train and test data such that fraud transaction records and 
     * non fraud transaction records are present in both test and train data */
    
    val inputFraud = labelDF.filter(x => x.getDouble(9) == 1.0).randomSplit(Array(0.7, 0.3), seed = 20L)
    val inputNotFraud = labelDF.filter(x => x.getDouble(9) == 0.0).randomSplit(Array(0.7, 0.3), seed = 20L)
    
    var train = inputFraud(0).union(inputNotFraud(0)).toDF()
    var test = inputFraud(1).union(inputNotFraud(1)).toDF()
    
    // Creating the Random forest classifier, by setting the parameters for training
    val fraudClassifier = new RandomForestClassifier()
                              .setImpurity("gini")
                              .setMaxDepth(10)
                              .setNumTrees(10)
                              .setFeatureSubsetStrategy("auto")
                              .setSeed(3000)

    // Using the classifier to train the model
    val trainedModel = fraudClassifier.fit(train)
    
    // Printing the random forest trees
    trainedModel.toDebugString

    // Using the trained model on test data to get predictions
    val fraudPrediction = trainedModel.transform(test)

    // Evaluating the accuracy of the predictions using the binary classification evaluator
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val accuracy = evaluator.evaluate(fraudPrediction)
    
    logger.info("Random Forest accuracy : "+accuracy)

    
     
    /* Boosted Strategy 
     * Converting the train and test into an RDD of LabeledPoints
     */
    val trainBoosted = train.rdd.map(trans =>
      new LabeledPoint(trans.getAs("label"),
        org.apache.spark.mllib.linalg.Vectors.fromML(trans.getAs("features"))))
    val testBoosted = test.rdd.map(trans =>
      new LabeledPoint(trans.getAs("label"),
        org.apache.spark.mllib.linalg.Vectors.fromML(trans.getAs("features"))))
    
    // Initializing the strategy and setting parameters  
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    
    boostingStrategy.numIterations = 10
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 10
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    
    // Training the model using the train data and the strategy
    val boostedModel = GradientBoostedTrees.train(trainBoosted, boostingStrategy)
    
    // Using the trained model on test data to get predictions
    val testErrBoosted = testBoosted.map{ point =>
      val prediction = boostedModel.predict(point.features)
      (point.label, prediction)
    }
    
    /* Evaluating the accuracy of the predictions by comparing the predicted label with 
    	 the actual label of the test record */
    val testErr = testErrBoosted.filter(r => r._1 == r._2).count.toDouble / testBoosted.count()
    logger.info("Boosted Strategy accuracy : "+testErr)
    
    
    /* Decision Tree 
     * Defining the train classifier and setting parameters  */    
    val numClasses = 2 
    val categoricalFeatures = Map[Int,Int]()
    val impurity = "entropy"
    val maxDepth = 10
    val maxBins = 30
    
    // Training the model using the train data and the parameters
    val model = DecisionTree.trainClassifier(trainBoosted, numClasses, categoricalFeatures, impurity, maxDepth, maxBins)
    
    // Using the trained model on test data to get predictions
    var labelprediction = testBoosted.map{ testRecord => val prediction = model.predict(testRecord.features)
                                                                   (testRecord.label, prediction)}
    
    /* Evaluating the accuracy of the predictions by comparing the predicted label with 
    	 the actual label of the test record */
    val accuracyDecision = labelprediction.filter(row => row._1 == row._2).count().toDouble / test.count()
    logger.info("Decision Tree Accuracy  : "+accuracyDecision);
    logger.info("Decision Tree Model Trained: "+model.toDebugString);
    
    // Saving the accuracy determined from all the three models to an RDD to save it to an output file
    val finalAccuracyRDD = sc.parallelize(List(Row("Random Forest accuracy",accuracy),
                                               Row("Boosted Strategy accuracy",testErr),
                                               Row("Decision Tree Accuracy",accuracyDecision)))
                                               

                                               
    finalAccuracyRDD.saveAsTextFile(args(1))
    
  }

  /* Method to convert the RDD of Strings to RDD of Double
   * The fields nameOrig and nameDest have one character ('C' or 'M') 
   * at the start of the value. This character is dropped to convert 
   * the field to double.
   * The field type which contains one of 'PAYMENT','TRANSFER','DEBIT'
   * ,'CASH_IN','CASH_OUT'. Inorder to convert the data to double we have
   * assigned data with the following values
   * PAYMENT - 4.0, TRANSFER - 5.0, DEBIT - 3.0, CASH_IN - 1.0 and CASH_OUT - 2.0  */
  
  def parseNumber(inputRDD : RDD[String]): RDD[Array[Double]] = {

    val parsedArray = inputRDD.map(_.split(","))
                              .map{case Array(step,ptype,amt,nameOrig,oldBalOrg,newBalOrg,nameDest,oldBalDest,newBalDest,isFraud,isFlagged)
                                                    => var newPtype = 0.0
                                                       var newNameOrig = nameOrig.drop(1)
                                                       var newNameDest = nameDest.drop(1)
                                                       if(ptype.equals("PAYMENT")){ newPtype = 4.0}
                                                       else if(ptype.equals("TRANSFER")){newPtype = 5.0}
                                                       else if(ptype.equals("DEBIT")){newPtype = 3.0}
                                                       else if(ptype.equals("CASH_IN")){newPtype = 1.0}
                                                       else if(ptype.equals("CASH_OUT")){newPtype = 2.0}
                                                       Array(step.toDouble,
                                                             newPtype,
                                                             amt.toDouble,
                                                             newNameOrig.toDouble,
                                                             oldBalOrg.toDouble,
                                                             newBalOrg.toDouble,
                                                             newNameDest.toDouble,
                                                             oldBalDest.toDouble,
                                                             newBalDest.toDouble,
                                                             isFraud.toDouble,
                                                             isFlagged.toDouble)}

  return parsedArray;
}

  // Defining transaction schema for each record 
  case class Transaction(
    step: Double,
    ttype: Double,
    amt: Double,
    nameOrig: Double,
    oldBalOrg: Double,
    newBalOrg: Double,
    nameDest: Double,
    oldBalDest: Double,
    newBalDest: Double,
    isFraud: Double,
    isFlagged: Double)

}