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
    val model = db.models.get("MIMIR_CH_" + config.schema).asInstanceOf[DetectHeaderModel]
    SingletonTable(Seq(("TABLE_NAME",StringPrimitive(model.targetName))))
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val model = db.models.get("MIMIR_CH_" + config.schema).asInstanceOf[DetectHeaderModel]
    model.query.columnNames.zipWithIndex.map(col => 
      SingletonTable(Seq(("TABLE_NAME" , StringPrimitive(model.targetName)), ("ATTR_NAME" , model.bestGuess(col._2, Seq(), Seq())),("ATTR_TYPE", TypePrimitive(TString())),("IS_KEY", BoolPrimitive(false))))
     ) match {
      case Seq() => EmptyTable(Seq(("TABLE_NAME", TString()), ("ATTR_NAME", TString()), ("ATTR_TYPE", TType()), ("IS_KEY", TBool())))
      case Seq(sng@SingletonTable(_)) => sng
      case sngs:Seq[SingletonTable] => {
        val revsngs = sngs.reverse
        revsngs.tail.tail.foldLeft(Union(revsngs.tail.head, revsngs.head))((union, sng) => {
          Union(sng, union)
        })
      }
    }
  }
  
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    val model = db.models.get("MIMIR_CH_" + config.schema).asInstanceOf[DetectHeaderModel]
    Some(
        Project( model.query.columnNames.zipWithIndex.map( col => 
          ProjectArg(model.bestGuess(col._2, Seq(), Seq()).asString,Var(col._1)) )
          , config.query) match {
          case proj if model.headerDetected => proj.limit(1000000000, 1)
          case proj => proj
        })
  }
}
