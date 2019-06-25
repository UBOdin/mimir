package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
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

object CheckHeader
  extends Multilens
  with LazyLogging
{

  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    val viewName = config.schema
    val modelName = ID("MIMIR_CH_", viewName)
    val detectmodel = 
      new DetectHeaderModel(
        modelName, 
        config.humanReadableName, 
        config.query.columnNames, 
        db.query(Limit(0,Some(6),config.query))(_.toList.map(_.tuple)).toSeq
      )
    detectmodel.detect_header()
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
      Some(
          Project( model.columns.zipWithIndex.map( col => 
            ProjectArg(ID(model.bestGuess(0, Seq(IntPrimitive(col._2)), Seq()).asString),Var(col._1)) )
            , config.query) match {
            case proj if model.headerDetected => proj.filter(Not(Comparison(Cmp.Eq, RowIdVar(), RowIdPrimitive("1"))))
            case proj => proj
          })
    } else { 
      logger.warn(s"Getting invalid table $table from Detect Headers adaptive schema")
      None 
    }
  }
}
