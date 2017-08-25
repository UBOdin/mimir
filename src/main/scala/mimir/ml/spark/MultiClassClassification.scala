package mimir.ml.spark

import mimir.algebra._
import mimir.util._
import mimir.Database

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// $example off$
import java.io.StringReader
import scala.collection.mutable.ArrayBuffer;
//import org.apache.lucene.util.Version
//import org.apache.lucene.analysis.en.EnglishAnalyzer
//import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.io.Source
import org.apache.spark.ml.feature.{HashingTF, Tokenizer} 
//import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler


case class LabeledRecord(id: String, topic: String, text: String)
case class ReverseIndexRecord(prediction: Double)

object MultiClassClassification {
  type ClassifierModel = PipelineModel
  type ClassifierModelGenerator = (Operator,Database,String) => PipelineModel
  sealed trait Category
  case object Scientific extends Category
  case object NonScientific extends Category
  case class LabeledText(id: Double, category: Category, text: String)
  var sc: Option[SparkContext] = None
  var sqlCtx : Option[SQLContext] = None
  
  def getSparkSession() : SparkContext = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("MultiClassClassification")
      sc match {
        case None => {
          sc = Some(new SparkContext(conf))
          sc.get
        }
        case Some(session) => session
      }
  }
  
  def getSparkSqlContext() : SQLContext = {
    sqlCtx match {
      case None => {
        sqlCtx = Some(new SQLContext(getSparkSession()))
        sqlCtx.get
      }
      case Some(ctx) => ctx
    }
  }

  def classify( model : PipelineModel, colNames:Seq[String], testData : List[Seq[PrimitiveValue]]): DataFrame = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._ 
    val predictions = colNames.length match {
          case 1  =>  model.transform(testData.map(tuple => mimir.util.ListUtils.listToTuple2 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 2  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple3 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 3  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple4 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 4  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple5 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 5  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple6 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 6  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple7 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 7  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple8 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 8  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple9 (tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 9  =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple10(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 10 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple11(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 11 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple12(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 12 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple13(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 13 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple14(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 14 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple15(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 15 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple16(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 16 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple17(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 17 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple18(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 18 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple19(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 19 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple20(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 20 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple21(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case 21 =>  model.transform(testData.map( tuple =>mimir.util.ListUtils.listToTuple22(tuple.map(value => value.asString).toList)).toDF(("rowid" :: colNames.toList) : _*))
          case x  =>  throw new Exception("You have to many columns for training a model")       
     }
     predictions
   }
  
  def prepareData(query : Operator, db:Database) : DataFrame = {
     db.query(query, mimir.exec.mode.BestGuess)(results => {
       val sqlContext = getSparkSqlContext()
      import sqlContext.implicits._
      query.columnNames.length match {
          case 1  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple2 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 2  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple3 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 3  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple4 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 4  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple5 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 5  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple6 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 6  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple7 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 7  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple8 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 8  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple9 (row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 9  =>  results.toList.map(row => mimir.util.ListUtils.listToTuple10(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 10 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple11(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 11 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple12(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 12 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple13(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 13 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple14(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 14 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple15(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 15 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple16(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 16 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple17(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 17 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple18(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 18 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple19(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 19 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple20(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 20 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple21(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case 21 =>  results.toList.map(row => mimir.util.ListUtils.listToTuple22(row.provenance.asString :: row.tuple.map(value => value.asString).toList)).toDF(("rowid" :: query.columnNames.toList) : _*)
          case x  =>  throw new Exception("You have to many columns for training a model")
        }
     }) 
  }
  
  def removeStopWords(text : String) : String = {
    var bodytext = text
      for(removeWord <- Array("a", "an", "another", "any", "certain", "each", "every", "her", "his", "its", "its", "my", "no", "our", "some", "that", "the", "their", "this", "and", "but", "or", "yet", "for", "nor", "so", "as", "aboard", "about", "above", "across", "after", "against", "along", "around", "at", "before", "behind", "below", "beneath", "beside", "between", "beyond", "but", "by", "down", "during", "except", "following", "for", "from", "in", "inside", "into", "like", "minus", "minus", "near", "next", "of", "off", "on", "onto", "onto", "opposite", "out", "outside", "over", "past", "plus", "round", "since", "since", "than", "through", "to", "toward", "under", "underneath", "unlike", "until", "up", "upon", "with", "without", "a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its", "itself", "i've", "j", "just", "k", "keep", "keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure" )){
        bodytext = bodytext.replaceAll(" "+ removeWord + " " , " ")
      }
    bodytext = bodytext.replaceAll("\r\n" , " ")
    bodytext = bodytext.replaceAll("\n" , " ")
    bodytext;
  }
  
   def NaiveBayesMulticlassModel(query:Operator, db:Database, predictionCol:String) : PipelineModel = {
      val training = prepareData(query, db)//.withColumn("label", toLabel($"topic".like("sci%"))).cache
      //training.show()
      val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
      val indexerModel = indexer.fit(training);  
      val (tokenizers, hashingTFs) = query.columnNames.map(col => {
        val tokenizer = new RegexTokenizer().setInputCol(col).setOutputCol(s"${col}_words")
        val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(s"${col}_features").setNumFeatures(20)
        (tokenizer, hashingTF)
      }).unzip
      val assembler = new VectorAssembler().setInputCols(query.columnNames.map(col => s"${col}_features").toArray).setOutputCol("features")
      val classifier = new NaiveBayes().setLabelCol("label").setFeaturesCol("features")//.setModelType("multinomial")
      val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(indexerModel.labels)
      val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: classifier :: labelConverter :: Nil)
      val pipeline = new Pipeline().setStages(stages.toArray)
      pipeline.fit(training)
  }
   
   def RandomForestMLMulticlassModel( query:Operator, db:Database, predictionCol:String): PipelineModel = {
      val training = prepareData(query, db)//.withColumn("label", toLabel($"topic".like("sci%"))).cache
      //training.show()
      val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
      val indexerModel = indexer.fit(training);  
      val (tokenizers, hashingTFs) = query.columnNames.map(col => {
        val tokenizer = new RegexTokenizer().setInputCol(col).setOutputCol(s"${col}_words")
        val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(s"${col}_features").setNumFeatures(20)
        (tokenizer, hashingTF)
      }).unzip
      val assembler = new VectorAssembler().setInputCols(query.columnNames.map(col => s"${col}_features").toArray).setOutputCol("features")
      val classifier = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
      val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(indexerModel.labels)
      val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: classifier :: labelConverter :: Nil)
      val pipeline = new Pipeline().setStages(stages.toArray)
      pipeline.fit(training)
   }
  
  /*def tokenize(content: String): Seq[String] = {
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