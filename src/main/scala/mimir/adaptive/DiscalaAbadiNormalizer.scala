package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.statistics.FuncDep

object DiscalaAbadiNormalizer
  extends Multilens
  with LazyLogging
{
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating DiscalaAbadiNormalizer: $config")

    logger.debug("Creating FD Graph")
      val fd = new FuncDep()
      fd.buildEntities(db, config.query, config.schema)

    logger.debug("Dumping Base Schema")
      val schTable = s"MIMIR_DA_SCH_${config.schema}"
      val fullSchema = Seq((("ROOT", TRowId()), -1)) ++ fd.sch.zipWithIndex

      val htSch = 
        HardTable(
            Seq(("ATTR_NAME", TString()), ("ATTR_NODE", TInt()), ("ATTR_TYPE", TString())),
            fullSchema.map { case ((attr, t), idx) => 
            Seq(StringPrimitive(attr), IntPrimitive(idx), TypePrimitive(t))
          }
        )
      db.views.create(schTable, htSch)
      
      logger.debug("Dumping FD Graph")
      val fdTable = s"MIMIR_DA_FDG_${config.schema}"
      val htFd = 
        HardTable(
            Seq(("MIMIR_FD_PARENT", TInt()), ("MIMIR_FD_CHILD", TInt()), ("MIMIR_FD_PATH_LENGTH", TInt())),
            // Add the basic edges
            (fd.fdGraph.getEdges.asScala.map { case (edgeParent, edgeChild) =>
              Seq(
                IntPrimitive(edgeParent), 
                IntPrimitive(edgeChild),
                if(fd.parentTable.getOrElse(edgeParent, Set[Int]()) contains edgeChild){ IntPrimitive(2) } 
                  else { IntPrimitive(1) }
              )
            }).toSeq ++
            // And add in each blacklisted node as an edge off of the root
            (( 
              (0 until fd.sch.size).toSet[Int] -- fd.nodeTable.asScala.map(_.toInt).toSet
            ).map { col => 
              Seq(
                IntPrimitive(-1),
                IntPrimitive(col),
                IntPrimitive(2)
              )
            } )
        )
    db.views.create(fdTable, htFd)

    val groupingModel = 
      new DAFDRepairModel(
        s"MIMIR_DA_CHOSEN_${config.schema}:MIMIR_FD_PARENT",
        config.schema,
        db.metadataTable(fdTable),
        Seq(("MIMIR_FD_CHILD", TInt())),
        "MIMIR_FD_PARENT",
        TInt(),
        Some("MIMIR_FD_PATH_LENGTH"),
        fullSchema.map { x => (x._2.toLong -> x._1._1) }.toMap
      )
    groupingModel.reconnectToDatabase(db)
    val schemaLookup =
      fullSchema.map( x => (x._2 -> x._1) ).toMap

    // for every possible parent/child relationship except for ROOT
    val parentKeyRepairs = 
      fd.fdGraph.getEdges.asScala.
        filter( _._1 != -1 ).
        map { case (edgeParent, edgeChild) =>
          val (parent, parentType) = schemaLookup(edgeParent)
          val (child, childType) = schemaLookup(edgeChild)
          val model = 
            new RepairKeyModel(
              s"MIMIR_DA_CHOSEN_${config.schema}:MIMIR_NORM:$parent:$child", 
              s"$child in $parent",
              config.query,
              Seq((parent, parentType)),
              child,
              childType,
              None
            )
          groupingModel.reconnectToDatabase(db)
          logger.trace(s"INSTALLING: $parent -> $child: ${model.name}")
          model 
        }
    return Seq(groupingModel)++parentKeyRepairs
  }

  /**
   * Use repair-key to construct a spanning tree out of the Functional Dependency Graph
   *
   * Returns a query that constructs the spanning tree table.
   */
  final def spanningTreeLens(db: Database, config: MultilensConfig): Operator =
  {
    val model = db.models.get(s"MIMIR_DA_CHOSEN_${config.schema}:MIMIR_FD_PARENT")
    RepairKeyLens.assemble(
      db.metadataTable(s"MIMIR_DA_FDG_${config.schema}"),
      Seq("MIMIR_FD_CHILD"), 
      Seq(("MIMIR_FD_PARENT", model)),
      Some("MIMIR_FD_PATH_LENGTH")
    )
  }

  /**
   * Assuming that 'query.labelCol' is the name of a column with a spanning tree node ID, 
   * return a version of query with 'labelCol' replaced by the name of the attribute rather 
   * than its ID.  Optionally add the type of the attribute in 'typeCol'.
   */
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
          db.metadataTable(s"MIMIR_DA_SCH_${config.schema}"),
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

    // We want to figure out which nodes belong to which table.
    // 
    // All non-leaf nodes in the spanning tree define a table consisting of 
    //   - The non-leaf node itself
    //   - All of the non-leaf node's children

    // First compute all of teh non-leaf node's children.
    // We start with the spanning tree, which is given in terms of Parent/Child NodeIDs
    //  - All of the children define the attribute name
    //  - All of the parents define the table that the attribute belongs to
    val childAttributeQuery =
      convertNodesToNamesInQuery(db, config, "MIMIR_FD_CHILD", "ATTR_NAME", Some("ATTR_TYPE"),
        convertNodesToNamesInQuery(db, config, "MIMIR_FD_PARENT", "TABLE_NAME", None,
          spanningTree
        )
      ).addColumn("IS_KEY" -> BoolPrimitive(false))

    // The parent node is also part of the table, so figure out which are the non-leaf nodes
    // and make them part of the table they define.
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
    db.queryMetadata(
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
class DAFDRepairModel(
  name: String, 
  context: String, 
  source: Operator, 
  keys: Seq[(String, Type)], 
  target: String,
  targetType: Type,
  scoreCol: Option[String],
  attrLookup: Map[Long,String]
) extends RepairKeyModel(name, context, source, keys, target, targetType, scoreCol)
{
  override def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    getFeedback(idx, args) match {
      case None => {
        val possibilities = getDomain(idx, args, hints).sortBy(-_._2).map { _._1.asLong }
        val best = possibilities.head
        s"${attrLookup(args(0).asLong)} could be organized under any of ${possibilities.map { x =>  attrLookup(x)+" ("+x+")" }.mkString(", ")}; I chose ${attrLookup(best) }"
      }
      case Some(choice) => 
        s"You told me to organize ${attrLookup(args(0).asLong)} under ${attrLookup(choice.asLong)}"
    }
  }
}