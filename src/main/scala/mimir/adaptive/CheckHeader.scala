package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import mimir.Database
import mimir.parser._
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.views._
import mimir.statistics.FuncDep
import mimir.provenance.Provenance
import mimir.exec.spark.RAToSpark

object CheckHeader
  extends Multilens
  with LazyLogging
{

  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    val viewName = config.schema
    val modelName = ID("MIMIR_CH_", viewName)
    val dhOp = Limit(0,Some(6),config.query)
    //TODO:  This is a temporary fix for detect headers when there are multiple partitions 
    //  spark is returning the first X rows for limit queries from a nondeterministic partition
    //val td = db.query(dhOp)(_.toList.map(_.tuple)).toSeq
    val td = db.compiler.compileToSparkWithRewrites(config.query)
        .take(6).map(row => row.toSeq.map(spel => 
          RAToSpark.sparkInternalRowValueToMimirPrimitive(spel))
          ).toSeq
    val (headerDetected, initialHeaders) = DetectHeader.detect_header(config.query.columnNames, 
        td)
    db.compiler.compileToSparkWithRewrites(config.query).take(6).map(_.toString())
    val detectmodel = 
      new DetectHeaderModel(
        modelName, 
        config.humanReadableName, 
        headerDetected,
        initialHeaders
      )
    Seq(detectmodel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        (ID("TABLE_NAME"),TString())
      ),
      Seq(
        Seq(
          StringPrimitive("DATA")
        )
      )
    )
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        (ID("TABLE_NAME") -> TString()), 
        (ID("ATTR_TYPE")  -> TType()),
        (ID("IS_KEY")     -> TBool()),
        (ID("COLUMN_ID")  -> TInt())
      ),
      (0 until config.query.columnNames.size).map { col =>
        Seq(
          StringPrimitive("DATA"),
          TypePrimitive(TString()),
          BoolPrimitive(false),
          IntPrimitive(col)
        )
      }
    ).addColumns(
      "ATTR_NAME" -> VGTerm(config.schema.withPrefix("MIMIR_CH_"), 0, Seq(Var(ID("COLUMN_ID"))), Seq())
    ).removeColumns("COLUMN_ID")
  }
  
  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] =
  {
    if(table.equals(ID("DATA"))){
      val model = db.models.get(ID("MIMIR_CH_",config.schema)).asInstanceOf[DetectHeaderModel]

      var oper = config.query

      // If we have a header... 
      if(model.headerDetected) { 
        // Strip off row #1 
        val firstRowID = db.query(oper.limit(1))(res => { 
          val resList = res.toList
          if(resList.isEmpty)
            RowIdPrimitive("0")
          else
            resList.head.provenance
        })
        oper = oper.filter { RowIdVar().neq(firstRowID) } 
        
        // And then rename columns accordingly
        oper = oper.renameByID(
          config.query.columnNames
               .zipWithIndex
               .map { case (col, idx) => 
                  // the model takes the column index and returns a string
                  val guessedColumnName = model.bestGuess(0, Seq(IntPrimitive(idx)), Seq())

                  // map from the existing name to the new one
                  col -> ID(guessedColumnName.asString)
               } :_*
        )
      }

      return Some(oper)
    } else { 
      logger.warn(s"Getting invalid table $table from Detect Headers adaptive schema")
      return None 
    }
  }
}
