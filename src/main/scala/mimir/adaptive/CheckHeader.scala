package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import mimir.Database
import net.sf.jsqlparser.statement.Statement
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
    val modelName = "MIMIR_CH_" + viewName
    val detectmodel = new DetectHeaderModel(modelName, viewName, config.query)
    detectmodel.reconnectToDatabase(db)
    detectmodel.detect_header()
    Seq(detectmodel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        ("TABLE_NAME",TString())
      ),
      Seq(
        Seq(
          StringPrimitive(config.schema)
        )
      )
    )
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        ("TABLE_NAME" , TString()), 
        ("ATTR_TYPE", TType()),
        ("IS_KEY", TBool()),
        ("COLUMN_ID", TInt())
      ),
      (0 until config.query.columnNames.size).map { col =>
        Seq(
          StringPrimitive(config.schema),
          TypePrimitive(TString()),
          BoolPrimitive(false),
          IntPrimitive(col)
        )
      }
    ).addColumn(
      "ATTR_NAME" -> VGTerm("MIMIR_CH_" + config.schema, 0, Seq(Var("COLUMN_ID")), Seq())
    ).removeColumn("COLUMN_ID")
  }
  
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    val model = db.models.get("MIMIR_CH_" + config.schema).asInstanceOf[DetectHeaderModel]
    Some(
        Project( model.query.columnNames.zipWithIndex.map( col => 
          ProjectArg(model.bestGuess(0, Seq(IntPrimitive(col._2)), Seq()).asString,Var(col._1)) )
          , config.query) match {
          case proj if model.headerDetected => proj.limit(-1, 1)
          case proj => proj
        })
  }
}
