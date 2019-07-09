package mimir.ml.spark

import mimir.algebra._
import mimir.util._
import mimir.Database

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, BooleanType, IntegerType, StringType, StructField, StructType}
import java.io.StringReader
import scala.collection.mutable.ArrayBuffer;
//import org.apache.lucene.util.Version
//import org.apache.lucene.analysis.en.EnglishAnalyzer
//import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.io.Source
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.classification.{RandomForestClassifier, NaiveBayes, DecisionTreeClassifier, GBTClassifier, LogisticRegression, OneVsRest, LinearSVC, MultilayerPerceptronClassifier}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.NumericType
import java.sql.Timestamp
import java.sql.Date
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import mimir.exec.spark.RAToSpark
import mimir.provenance.Provenance
import org.apache.spark.sql.types.ShortType
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import mimir.ml.spark.SparkML.SparkModelGeneratorParams


object Classification extends SparkML {
  
  
  def classifydb(model : PipelineModel, query : Operator, db:Database) : DataFrame = {
    applyModelDB(model, query, db)
  }
  
  def classify( model : PipelineModel, cols:Seq[(ID, Type)], testData : List[Seq[PrimitiveValue]], db: Database): DataFrame = {
    applyModel(model, cols, testData, db)
  }
    
  override def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._ 
    if(predictions.columns.contains("probability")){
      val provCol = predictions.schema.fields(predictions.schema.fields.map(_.name).indexOf("label")-1).name
      val rowidsProbabilitiesIdxs = predictions.select(provCol,"probability").rdd.map(r => (r.getString(0), r.getAs[org.apache.spark.ml.linalg.DenseVector](1))).collect().map { item =>
          item._2.toArray.zipWithIndex.sortBy(_._1).reverse.slice(0, maxPredictions).map(probIdx => (item._1, probIdx._1, probIdx._2))}.flatten.toSeq
      model.stages(model.stages.length-1).transform(rowidsProbabilitiesIdxs.toDF(provCol,"probability","prediction")).select(provCol,"probability","predictedLabel").sort($"probability".desc,$"predictedLabel").map { x => (x.getString(0), (x.getString(2),x.getDouble(1))) }.collect()
    }
    else extractPredictionsNoProb(model, predictions, maxPredictions)
  }
  
  override def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    if(predictions.columns.contains("probability")){
      val provCol = predictions.schema.fields(predictions.schema.fields.map(_.name).indexOf("label")-1).name
      val probabilitiesIdxs = predictions.where(predictions(provCol) === rowid).select("probability").rdd.map(r => r.getAs[org.apache.spark.ml.linalg.DenseVector](0)).collect().map { item =>
          item.toArray.zipWithIndex.sortBy(_._1).reverse.slice(0, maxPredictions).map(probIdx =>  probIdx)}.flatten.toSeq
      model.stages(model.stages.length-1).transform(probabilitiesIdxs.toDF("probability","prediction")).select("probability","predictedLabel").sort($"probability".desc,$"predictedLabel").map { x => (x.getString(1), x.getDouble(0)) }.collect()
    }
    else extractPredictionsForRowNoProb(model, predictions, rowid, maxPredictions)
  }
  
  def extractPredictionsNoProb(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    val provCol = predictions.schema.fields(predictions.schema.fields.map(_.name).indexOf("label")-1).name
    predictions.select(provCol,"prediction").rdd.map(r => (r.getString(0), r.getDouble(1))).collect().map{ item =>
        (item._1, (item._2.toString(), 1.0))}.toSeq
  }
  
  def extractPredictionsForRowNoProb(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)] = {
    val sqlContext = getSparkSqlContext()
    //import org.apache.spark.sql.functions.monotonicallyIncreasingId 
    import sqlContext.implicits._  
    val provCol = predictions.schema.fields(predictions.schema.fields.map(_.name).indexOf("label")-1).name
    predictions.where(predictions(provCol) === rowid).select("prediction").rdd.map(r => r.getDouble(1)).collect().map { item =>
        (item.toString(), 1.0)}.toSeq    
  }
  
  private def extractFeatures(training:DataFrame, params:SparkModelGeneratorParams):(Array[String], Seq[PipelineStage]) = {
    val cols = training.schema.fields
    //training.show()
    val indexer = new StringIndexer().setInputCol(params.predictionCol.id).setOutputCol("label").setHandleInvalid(params.handleInvalid)
    val labels = indexer.fit(training).labels
    val (tokenizers, hashingTFs) = cols.flatMap(col => {
      col.dataType match {
        case StringType => {
          val tokenizer = new RegexTokenizer().setInputCol(col.name).setOutputCol(s"${col.name}_words")
          val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(s"${col.name}_features").setNumFeatures(20)
          Some((tokenizer, hashingTF))
        }
        case _ => None
      }
    }).unzip
    val assmblerCols = cols.flatMap(col => {
      col.dataType match {
        case StringType => Some(s"${col.name}_features")
        case IntegerType | LongType | DoubleType => Some(col.name)
        case _ => None
      }
    })
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("rawFeatures")
    val normlzr = new Normalizer().setInputCol("rawFeatures").setOutputCol("normFeatures").setP(1.0)
    val scaler = new StandardScaler().setInputCol("normFeatures").setOutputCol("features").setWithStd(true).setWithMean(false)
    (labels,(indexer :: tokenizers ++: hashingTFs ++: (assembler :: normlzr :: scaler :: Nil)))
  }
  
  def NaiveBayesMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    import org.apache.spark.sql.functions.abs
    val trainingp = trainingData.transform(fillNullValues)
    val training = trainingp.schema.fields.filter(col => Seq(IntegerType, LongType, DoubleType).contains(col.dataType)).foldLeft(trainingp)((init, cur) => init.withColumn(cur.name,abs(init(cur.name))) )
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new NaiveBayes().setLabelCol("label").setFeaturesCol("features")//.setModelType("multinomial")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: (classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
   
  def RandomForestMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: (classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def DecisionTreeMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def GradientBoostedTreeBinaryclassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val featureIndexer = new VectorIndexer().setInputCol("assembledFeatures").setOutputCol("features").setMaxCategories(20)
    val classifier = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( featureIndexer :: classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def LogisticRegressionMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true).setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def OneVsRestMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true).setLabelCol("label").setFeaturesCol("features")
    val ovr = new OneVsRest().setClassifier(classifier)
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( ovr :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def LinearSupportVectorMachineBinaryclassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    val classifier = new LinearSVC().setMaxIter(10).setRegParam(0.1).setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def MultilayerPerceptronMulticlassModel(trainingData:DataFrame):SparkML.SparkModelGenerator = params => {
    val training = trainingData.transform(fillNullValues)
    val (labels, featurePipelineStages) = extractFeatures(training,params)
    import org.apache.spark.sql.functions.countDistinct
    import org.apache.spark.sql.functions.col
    val classCount = training.select(countDistinct(col(params.predictionCol.id))).head.getLong(0)
    val layers = Array[Int](training.columns.length, 8, 4, classCount.toInt)
    val classifier = new MultilayerPerceptronClassifier()
      .setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100).setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  /*
  def removeStopWords(text : String) : String = {
    var bodytext = text
      for(removeWord <- Array("a", "an", "another", "any", "certain", "each", "every", "her", "his", "its", "its", "my", "no", "our", "some", "that", "the", "their", "this", "and", "but", "or", "yet", "for", "nor", "so", "as", "aboard", "about", "above", "across", "after", "against", "along", "around", "at", "before", "behind", "below", "beneath", "beside", "between", "beyond", "but", "by", "down", "during", "except", "following", "for", "from", "in", "inside", "into", "like", "minus", "minus", "near", "next", "of", "off", "on", "onto", "onto", "opposite", "out", "outside", "over", "past", "plus", "round", "since", "since", "than", "through", "to", "toward", "under", "underneath", "unlike", "until", "up", "upon", "with", "without", "a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its", "itself", "i've", "j", "just", "k", "keep", "keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure" )){
        bodytext = bodytext.replaceAll(" "+ removeWord + " " , " ")
      }
    bodytext = bodytext.replaceAll("\r\n" , " ")
    bodytext = bodytext.replaceAll("\n" , " ")
    bodytext;
  }
  
  def tokenize(content: String): Seq[String] = {
    val tReader = new StringReader(content)
    val analyzer = new EnglishAnalyzer()
    val tStream = analyzer.tokenStream("contents", tReader)
    val term = tStream.addAttribute(classOf[CharTermAttribute])
    tStream.reset()
    val result = ArrayBuffer.empty[String]
    while(tStream.incrementToken()) {
      val termValue = term.toString
      if (!(termValue matches ".*[\\d\\.].*")) {
        result += term.toString
      }
    }
    result
  } */

}