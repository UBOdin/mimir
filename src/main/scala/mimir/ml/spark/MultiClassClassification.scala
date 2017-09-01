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
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.classification.{RandomForestClassifier, NaiveBayes, DecisionTreeClassifier, GBTClassifier}
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
import org.apache.spark.sql.types.AnyDataType


object MultiClassClassification {
  type ClassifierModel = PipelineModel
  type ClassifierModelGenerator = (Operator,Database,String) => PipelineModel
  sealed trait Category
  var sc: Option[SparkContext] = None
  var sqlCtx : Option[SQLContext] = None
  
  def getSparkSession() : SparkContext = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("MultiClassClassification")
      sc match {
        case None => {
          val sparkCtx = new SparkContext(conf)
          sc = Some(sparkCtx)
          sparkCtx
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
  
  def classifydb(model : PipelineModel, query : Operator, db:Database, valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType) : DataFrame = {
    val data = db.query(query, mimir.exec.mode.BestGuess)(results => {
      results.toList.map(row => row.provenance +: row.tuple)
    })
    classify(model, ("rowid", TString()) +:db.bestGuessSchema(query), data, valuePreparer, sparkTyper)
  }
  
  def classify( model : PipelineModel, cols:Seq[(String, Type)], testData : List[Seq[PrimitiveValue]], valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType): DataFrame = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._
    model.transform(sqlContext.createDataFrame(
      getSparkSession().parallelize(testData.map( row => {
        Row(row.zip(cols).map(value => valuePreparer(value._1, value._2._2)):_*)
      })), StructType(cols.toList.map(col => StructField(col._1, sparkTyper(col._2), true)))))
  } 
  
  def prepareData(query : Operator, db:Database, valuePreparer: ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType) : DataFrame = {
    val schema = db.bestGuessSchema(query).toList
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._
    sqlContext.createDataFrame(
      getSparkSession().parallelize(db.query(query, mimir.exec.mode.BestGuess)(results => {
        results.toList.map(row => Row((valuePreparer(row.provenance, TString() ) +: row.tuple.zip(schema).map(value => valuePreparer(value._1, value._2._2))):_*))
      })), StructType(StructField("rowid", StringType, false) :: schema.map(col => StructField(col._1, sparkTyper(col._2), true))))
  }
  
  type ValuePreparer = (PrimitiveValue, Type) => Any
  
  private def prepareValue(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => t match {
        case TInt() => 0L
        case TFloat() => 0.0
        case TDate() => ""
        case TString() => ""
        case TBool() => false
        case TRowId() => ""
        case TType() => ""
        case TAny() => ""
        case TTimestamp() => ""
        case TInterval() => ""
        case TUser(name) => prepareValue(value, mimir.algebra.TypeRegistry.registeredTypes(name)._2)
        case x => ""
      }
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case x =>  x.asString 
    }
  }
  
  private def getSparkType(t:Type) : DataType = {
    t match {
      case TInt() => LongType
      case TFloat() => DoubleType
      case TDate() => StringType
      case TString() => StringType
      case TBool() => BooleanType
      case TRowId() => StringType
      case TType() => StringType
      case TAny() => StringType
      case TTimestamp() => StringType
      case TInterval() => StringType
      case TUser(name) => getSparkType(mimir.algebra.TypeRegistry.registeredTypes(name)._2)
      case _ => StringType
    }
  }
  
  def prepareValueStr(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => ""
      case x => x.asString
    }
  }
  
  def getSparkTypeStr(t:Type) = {
    t match {
      case _ => StringType
    }
  }
  
  def extractPredictions(model : PipelineModel, predictions:DataFrame, maxPredictions:Int = 5) : Seq[(String, (String, Double))] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    val (rowidsProbabilities, idxs) = predictions.select("rowid","probability").rdd.map(r => (r.getString(0), r.getAs[org.apache.spark.ml.linalg.DenseVector](1))).collect().map { item =>
        item._2.toArray.zipWithIndex.sortBy(_._1).reverse.slice(0, maxPredictions).map(probIdx => ((item._1, probIdx._1), probIdx._2))}.flatten.toSeq.unzip
    model.stages(model.stages.length-1).transform(idxs.toDF("prediction")).select("predictedLabel").rdd.zip(getSparkSession().parallelize(rowidsProbabilities)).map { x => (x._2._1, (x._1.getString(0), x._2._2)) }.collect()       
  }
  
  def extractPredictionsForRow(model : PipelineModel, predictions:DataFrame, rowid:String, maxPredictions:Int = 5) : Seq[(String, Double)] = {
    val sqlContext = getSparkSqlContext()
    import sqlContext.implicits._  
    val (probabilities, idxs) = predictions.where($"rowid" === rowid).select("probability").rdd.map(r => r.getAs[org.apache.spark.ml.linalg.DenseVector](0)).collect().map { item =>
        item.toArray.zipWithIndex.sortBy(_._1).reverse.slice(0, maxPredictions).map(probIdx =>  probIdx)}.flatten.toSeq.unzip
    model.stages(model.stages.length-1).transform(idxs.toDF("prediction")).select("predictedLabel").rdd.zip(getSparkSession().parallelize(probabilities)).map { x => (x._1.getString(0), x._2) }.collect()  
  }
  
  def NaiveBayesMulticlassModel(valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType)(query:Operator, db:Database, predictionCol:String) : PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
    val cols = training.schema.fields.tail
    //training.show()
    val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
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
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val classifier = new NaiveBayes().setLabelCol("label").setFeaturesCol("features")//.setModelType("multinomial")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(labels)
    val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
   
  def RandomForestMulticlassModel(valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
    //training.show()
    val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
    val indexerModel = indexer.fit(training);  
    val cols = training.schema.fields.tail
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
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val classifier = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(indexerModel.labels)
    val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def DecisionTreeMulticlassModel(valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
    //training.show()
    val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
    val indexerModel = indexer.fit(training);  
    val cols = training.schema.fields.tail
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
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("features")
    val classifier = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(indexerModel.labels)
    val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: classifier :: labelConverter :: Nil)
    val pipeline = new Pipeline().setStages(stages.toArray)
    pipeline.fit(training)
  }
  
  def GradientBoostedTreeMulticlassModel(valuePreparer:ValuePreparer = prepareValue, sparkTyper:Type => DataType = getSparkType)( query:Operator, db:Database, predictionCol:String): PipelineModel = {
    val training = prepareData(query, db, valuePreparer, sparkTyper).na.drop()//.withColumn("label", toLabel($"topic".like("sci%"))).cache
    //training.show()
    val indexer = new StringIndexer().setInputCol(predictionCol).setOutputCol("label").setHandleInvalid("skip")
    val indexerModel = indexer.fit(training);  
    val cols = training.schema.fields.tail
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
    val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("assembledFeatures")
    val featureIndexer = new VectorIndexer().setInputCol("assembledFeatures").setOutputCol("features").setMaxCategories(20)
    val classifier = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
    val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol("predictedLabel").setLabels(indexerModel.labels)
    val stages = indexer :: tokenizers ++: hashingTFs ++: (assembler :: featureIndexer :: classifier :: labelConverter :: Nil)
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