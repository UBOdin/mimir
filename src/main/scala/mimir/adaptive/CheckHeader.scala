package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import mimir.Database
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
    val views  = new mimir.views.ViewManager(db);
    val lenses = new mimir.lenses.LensManager(db);
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

    views.create(detectmodel.view_name+"_RAW", Limit(1,Some(1000000000),Project(projectArgs,config.query)));

    val oper = db.table(detectmodel.view_name+"_RAW")
    val l = List(new FloatPrimitive(.5))
    lenses.create("TYPE_INFERENCE", detectmodel.view_name.toUpperCase, oper, l)

    var ret : TraversableOnce[Model] = null
    return Seq(detectmodel)
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
