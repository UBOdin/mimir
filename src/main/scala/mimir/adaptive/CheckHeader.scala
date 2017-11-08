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
    val detectmodel = new DetectHeaderModel(modelName);
    val detectResult = detectmodel.detect_header(db,config.query);

    val cols = detectResult._2.toSeq.sortBy(_._1).unzip._2
    
    val tableCatalog = "MIMIR_DH_TABLE_"+config.schema
    db.backend.update(s"""
      CREATE TABLE $tableCatalog (TABLE_NAME string)""")
    db.backend.fastUpdateBatch(s"""
      INSERT INTO $tableCatalog (TABLE_NAME) VALUES (?)""", Seq(Seq(StringPrimitive(viewName))))

    val attrCatalog = "MIMIR_DH_ATTR_"+config.schema
    db.backend.update(s"""
      CREATE TABLE $attrCatalog (TABLE_NAME string, ATTR_NAME string,ATTR_TYPE string,IS_KEY bool)""")
    db.backend.fastUpdateBatch(s"""
      INSERT INTO $attrCatalog (TABLE_NAME,ATTR_NAME,ATTR_TYPE,IS_KEY) VALUES (?,?,?,?);""",
      cols.map { col_name =>
        Seq(StringPrimitive(viewName), StringPrimitive(col_name), TypePrimitive(Type.fromString("varchar")), BoolPrimitive(false))
    })
    Seq(detectmodel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    db.table("MIMIR_DH_TABLE_"+config.schema).project("TABLE_NAME")
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    db.table("MIMIR_DH_ATTR_"+config.schema)
  }
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    val model = db.models.get("MIMIR_CH_" + config.schema).asInstanceOf[DetectHeaderModel]
    Some(Limit(if(model.headerDetected) 1 else 0,Some(10000000),Project(config.query.columnNames.zip(db.query(db.table("MIMIR_DH_ATTR_"+config.schema).project("ATTR_NAME"))(result => result.toList.map(row => row.tuple(0).asString))).map{
      case (a,b) => ProjectArg(b,Var(a))
    },config.query)))
  }


}
