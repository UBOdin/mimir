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
  var viewName = ""
  var detect:Boolean=false
  var query:Operator = null
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

    viewName= detectmodel.view_name
    detect = detectmodel.detect

    var projectArgs =config.query.columnNames.map( col => ProjectArg(col, Var(col)))
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
      var querymapping = config.query.columnNames zip (repl)
      projectArgs = querymapping.map{
        case (a,b) => ProjectArg(b,Var(a))
      }
    }
    views.create(viewName+"_RAW",Limit(1,Some(1000000000),Project(projectArgs,config.query)));
    val oper = db.table(viewName+"_RAW")
    val l = List(new FloatPrimitive(.5))
    lenses.create("TYPE_INFERENCE", viewName.toUpperCase, oper, l)


    return Seq(detectmodel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val queryString = "select * from "+viewName;
    var parser = new MimirJSqlParser(new ByteArrayInputStream(queryString.getBytes));
    val stmt: Statement = parser.Statement();
    val tableQuery = db.sql.convert(stmt.asInstanceOf[net.sf.jsqlparser.statement.select.Select])
    return tableQuery
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    println("here")
    var projectArgs =config.query.columnNames.map( col => ProjectArg(col, Var(col)))
    var queryAttr:Operator = null
    if(detect){
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
        var querymapping = config.query.columnNames zip (repl)
        projectArgs = querymapping.map{
          case (a,b) => ProjectArg(b,Var(a))
        }
      }
      queryAttr = Limit(1,Some(1000000000),Project(projectArgs,config.query));
    }else{
      queryAttr = Project(projectArgs,config.query);
    }
    query = queryAttr

    return queryAttr
  }
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    return None

  }


}
