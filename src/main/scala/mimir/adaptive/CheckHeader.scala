package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.statistics.FuncDep

object CheckHeader
  extends Multilens
  with LazyLogging
{
  def checkheader(db: Database, config: MultilensConfig): Boolean = {
    val col = config.query.expressions(0);
    val tablename = col.toString.split("_")(0);
    //val detectmodel = new DetectHeaderModel();
    //var detect = detectmodel.detect_header(db,config.query);
    //var detect  = detect_header(db, config.query);
    var len = 0;
    var arrs : Seq[mimir.algebra.PrimitiveValue] = null

    var str=""
    db.query(Limit(0,Some(1),config.query))(_.foreach{result =>
      arrs =  result.tuple
    })
    len = arrs.length

    for(i<- 0 until len){
      val res = arrs(i)
      var ch =  res.toString()(0)
      if(ch.toByte >= '0' && ch.toByte <= '9'){
        str ++= s"""COLUMN_$i AS COL_$i ,""";
      }
      else{
        str ++= s"""COLUMN_$i AS $res ,""";
      }
    }

    var query = ""

    str = str.slice(0,(str.length()-2));
    str = str.replaceAll("\\'","");
    //ProjectArg(str,str.toExpression);
    //Project(str,config.query);
    query = "CREATE VIEW "+ config.schema +" AS SELECT " +str +" from "+ tablename+"_RAW limit 1,1000000000000"
    db.backend.update(query);
    return false;
  }



  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    val col = config.query.expressions(0);
    val tablename = col.toString.split("_")(0);
    var view_name = "";
    val detectmodel = new DetectHeaderModel(
      config.schema,
      view_name
    );
    detectmodel.detect_header(db,config.query);
    //var detect  = detect_header(db, config.query);
    var len = 0;
    var arrs : Seq[mimir.algebra.PrimitiveValue] = null
    var str=""
    db.query(Limit(0,Some(1),config.query))(_.foreach{result =>
      arrs =  result.tuple
    })
    len = arrs.length
    var projectArgs =
      config.query.columnNames.
        map( col => ProjectArg(col, Var(col)))
    println(projectArgs)
    var repl : Seq[String] = null
    for(i<- 0 until len){
      val res = arrs(i)
      var ch =  res.toString()(0)
      if(ch.toByte >= '0' && ch.toByte <= '9'){
        str ++= s"""COLUMN_$i AS COL_$i ,""";
      }
      else{
        repl+:res.toString()

        //projectArgs+=ProjectArg("COLUMN_$i", Var(res.toString()))
        str ++= s"""COLUMN_$i AS $res ,""";
      }
    }
projectArgs = config.query.columnNames.map( col => ProjectArg(col, repl))
    //projectArgs +=1
println(projectArgs)
    var query = ""

    str = str.slice(0,(str.length()-2));
    str = str.replaceAll("\\'","");
    //ProjectArg(str,str.toExpression);
    //Project(str,config.query);
    query = "CREATE VIEW "+ detectmodel.view_name +" AS SELECT " +str +" from "+ tablename+"_RAW limit 1,1000000000000"
    println(query)
    db.backend.update(query);
    var ret : TraversableOnce[Model] = null
    return ret
  }

  final def spanningTreeLens(db: Database, config: MultilensConfig): Operator =
  {
    val model = db.models.get(s"MIMIR_DA_CHOSEN_${config.schema}:MIMIR_FD_PARENT")
    RepairKeyLens.assemble(
      db.table(s"MIMIR_DA_FDG_${config.schema}"),
      Seq("MIMIR_FD_CHILD"),
      Seq(("MIMIR_FD_PARENT", model)),
      Some("MIMIR_FD_PATH_LENGTH")
    )
  }

  final def convertNodesToNamesInQuery(
    db: Database,
    config: MultilensConfig,
    nodeCol: String,
    labelCol: String,
    typeCol: Option[String],
    query: Operator
  ): Operator =
  {
    Project(
      query.columnNames.map { col =>
        if(col.equals(nodeCol)){
          ProjectArg(labelCol, Var("ATTR_NAME"))
        } else {
          ProjectArg(col, Var(col))
        }
      } ++ typeCol.map { col =>
        ProjectArg(col, Var("ATTR_TYPE"))
      },
      Select(Comparison(Cmp.Eq, Var(nodeCol), Var("ATTR_NODE")),
        Join(
          db.table(s"MIMIR_DA_SCH_${config.schema}"),
          query
        )
      )
    )
  }


  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val spanningTree = spanningTreeLens(db, config)
    logger.trace(s"Table Catalog Spanning Tree: \n$spanningTree")
    val tableQuery =
      convertNodesToNamesInQuery(db, config, "TABLE_NODE", "TABLE_NAME", None,
        spanningTree
          .map( "TABLE_NODE" -> Var("MIMIR_FD_PARENT") )
          .distinct
      )
    logger.trace(s"Table Catalog Query: \n$tableQuery")
    return tableQuery
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val spanningTree = spanningTreeLens(db, config)
    logger.trace(s"Attr Catalog Spanning Tree: \n$spanningTree")
    val childAttributeQuery =
      convertNodesToNamesInQuery(db, config, "MIMIR_FD_CHILD", "ATTR_NAME", Some("ATTR_TYPE"),
        convertNodesToNamesInQuery(db, config, "MIMIR_FD_PARENT", "TABLE_NAME", None,
          spanningTree
        )
      ).addColumn("IS_KEY" -> BoolPrimitive(false))

    val parentAttributeQuery =
      Project(Seq(
          ProjectArg("TABLE_NAME", Var("TABLE_NAME")),
          ProjectArg("ATTR_NAME", Var("TABLE_NAME")),
          ProjectArg("ATTR_TYPE", Var("ATTR_TYPE")),
          ProjectArg("IS_KEY", BoolPrimitive(true))
        ),
        convertNodesToNamesInQuery(db, config, "TABLE_NODE", "TABLE_NAME", Some("ATTR_TYPE"),
          // SQLite does something stupid with FIRST that prevents it from figuring out that
          // -1 is an integer.  Add 1 to force it to realize that it's dealing with a number
          spanningTree
            .map( "TABLE_NODE" -> Var("MIMIR_FD_PARENT") )
            .distinct
            .filter( Comparison(Cmp.Gt, Arithmetic(Arith.Add, Var("TABLE_NODE"), IntPrimitive(1)), IntPrimitive(0)) )
        )
      )
    val jointQuery =
      Union(childAttributeQuery, parentAttributeQuery)
    logger.trace(s"Attr Catalog Query: \n$jointQuery")
    return jointQuery
  }
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    db.query(
      Project(
        Seq(ProjectArg("ATTR_NAME", Var("ATTR_NAME"))),
        Select(
          Comparison(Cmp.Eq, Var("TABLE_NAME"), StringPrimitive(table)),
          attrCatalogFor(db, config)
        )
      )
    ) { results =>
      val attrs:Seq[String] = results.map { row => row(0).asString }.toSeq

      if(attrs.isEmpty){ return None; }

      var baseQuery =
        Project(
          attrs.map( attr => ProjectArg(attr, Var(attr)) ),
          config.query
        )

      if(table == "ROOT"){
        Some(baseQuery)
      } else {
        val repairModels = attrs.
          filter { !_.equals(table) }.
          map { attr =>
            (
              attr,
              db.models.get(s"MIMIR_DA_CHOSEN_${config.schema}:MIMIR_NORM:$table:$attr")
            )
          }
        Some(
          RepairKeyLens.assemble(
            baseQuery,
            Seq(table),
            repairModels,
            None
          )
        )
      }
    }
  }
}
@SerialVersionUID(1001L)
class DetectHeaderModel(
  name: String,
  var view_name: String
)
{
  def detect_header(db: Database, query: Operator): Unit = {
    var detect = false;
    var header : Seq[mimir.algebra.PrimitiveValue] = null;
    var arrs : Seq[Seq[mimir.algebra.PrimitiveValue]] = Seq.empty[Seq[mimir.algebra.PrimitiveValue]];
    db.query(Limit(0,Some(1),query))(_.foreach{result =>
      header =  result.tuple
    })
    db.query(Limit(1,Some(5),query))(_.foreach{result =>
      arrs:+= result.tuple
    })
    val sample  = arrs.iterator
    val columnLength = header.size;
    var columnType =  scala.collection.mutable.Map[Int, String]()
    for(i <- 0 until columnLength ){
      columnType+= (i -> null)
    }

    var flag = 0;
    while(sample.hasNext){
      flag = 1;
      val row = sample.next

      for (col <- columnType.keySet){
        if(row(col) != NullPrimitive()){
          var i  = Cast.apply(TFloat(),row(col))
          if(i==NullPrimitive()){
            i = Cast.apply(TDate(),row(col))
          }
          if(i != NullPrimitive()){
            if(columnType(col) != "true"){
              if(columnType(col) == null){
                columnType(col) = "true"
              }
              else{
                columnType -= col
              }
            }
          }
          else{
              columnType(col)  = (row(col).toString().length()-2).toString();
          }
        }
      }
    }
    if (flag == 0){
      detect =  false;
    }
    var hasHeader = 0
    for (c<-columnType.keySet){
      if(columnType(c)!="true"){
        if (header(c).toString.length()-2 == columnType(c).toInt) {
            hasHeader=hasHeader-1;
        } else {
            hasHeader=hasHeader+1;
        }
      }else {
          var i  = Cast.apply(TFloat(),header(c))
          if(i==NullPrimitive()){
            i = Cast.apply(TDate(),header(c))
          }
          if( i == NullPrimitive()){
            hasHeader = hasHeader+1
          }else{
            hasHeader=hasHeader - 1;
            detect =  false;
          }
        }
      }
    detect = hasHeader > 0
    if(detect ==true){
      view_name = name+"_HEADER"
    }else{
      view_name = name+"_HEADER_CORRECTION"
    }
  }
}
