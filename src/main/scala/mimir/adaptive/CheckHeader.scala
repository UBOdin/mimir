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

object CheckHeader
  extends Multilens
  with LazyLogging
{

  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    val lenses          = new mimir.lenses.LensManager(db)
    val views           = new mimir.views.ViewManager(db)
    val col = config.query.expressions(0);
    val tablename = col.toString.split("_")(0);
    var view_name = "";
    val detectmodel = new DetectHeaderModel(
      config.schema,
      view_name,
      false
    );
    detectmodel.detect_header(db,config.query);

    var viewName= detectmodel.view_name
    var cols : Seq[String] = Nil;
    var projectArgs =config.query.columnNames.map{
     col => ProjectArg(col, Var(col))
     cols :+= col
    }
    var queryAttr:Operator = null
    var len = 0;
    var arrs : Seq[mimir.algebra.PrimitiveValue] = null
    var str=""
    db.query(Limit(0,Some(1),config.query))(_.foreach{result =>
      arrs =  result.tuple
    })
    len = arrs.length
    var repl = new ListBuffer[String]()
    for(i<- 0 until len){
      var res = arrs(i).toString()
      res = res.replaceAll("\\'","");
      var ch =  res.toString()(0)

      if(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'){
        repl += res.toString()
      }
    }
    if(repl.size>0){
      cols = Nil
      var querymapping = config.query.columnNames zip (repl)
      projectArgs = querymapping.map{
        case (a,b) => ProjectArg(b,Var(a))
        cols :+= b
      }
    }
    var tableCatalog = config.schema+"_Table"
    db.backend.update(s"""
      CREATE TABLE $tableCatalog (TABLE_NAME string)""")
    db.backend.fastUpdateBatch(s"""
      INSERT INTO $tableCatalog (TABLE_NAME) VALUES (?);
    """,
     Seq(viewName).map{ str =>
       Seq(StringPrimitive(str))
    })

    var attrCatalog = config.schema+"_Attributes"
    db.backend.update(s"""
      CREATE TABLE $attrCatalog (TABLE_NAME string, ATTR_NAME string,ATTR_TYPE string,IS_KEY bool)""")
    db.backend.fastUpdateBatch(s"""
      INSERT INTO $attrCatalog (TABLE_NAME,ATTR_NAME,ATTR_TYPE,IS_KEY) VALUES (?,?,?,?);""",
      cols.map { col_name =>
        Seq(StringPrimitive(viewName), StringPrimitive(col_name), TypePrimitive(Type.fromString("varchar")), BoolPrimitive(false))
    })
    return Seq(detectmodel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val queryString = "select TABLE_NAME from "+config.schema+"_Table";
    var parser = new MimirJSqlParser(new ByteArrayInputStream(queryString.getBytes));
    val stmt: Statement = parser.Statement();
    val tableQuery = db.sql.convert(stmt.asInstanceOf[net.sf.jsqlparser.statement.select.Select])
    return tableQuery
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val queryString = "select * from "+config.schema+"_Attributes";
    var parser = new MimirJSqlParser(new ByteArrayInputStream(queryString.getBytes));
    val stmt: Statement = parser.Statement();
    val attributeQuery = db.sql.convert(stmt.asInstanceOf[net.sf.jsqlparser.statement.select.Select])
    return attributeQuery
  }
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    var attr : Seq[mimir.algebra.PrimitiveValue]= Nil;
    db.query("select ATTR_NAME from "+config.schema+"_Attributes")(_.foreach{result =>
      attr :+= result.tuple(0)
    })
    var querymapping = config.query.columnNames zip (attr)
    var projectArgs = querymapping.map{
      case (a,b) => ProjectArg(b.toString(),Var(a))
    }
    return Some(Limit(1,Some(1000000000),Project(projectArgs,config.query)))

  }


}
