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
      val schTable = config.schema.withPrefix("MIMIR_DA_SCH_")
      val fullSchema:Seq[((ID, Type), Int)] = Seq(
        ((ID("ROOT"), TRowId()), -1)) ++ fd.sch.zipWithIndex

      val htSch = 
        HardTable(
            Seq(
              ID("ATTR_NAME") -> TString(),
              ID("ATTR_NODE") -> TInt(), 
              ID("ATTR_TYPE") -> TString()
            ),
            fullSchema.map { case ((attr, t), idx) => 
            Seq(StringPrimitive(attr.id), IntPrimitive(idx), TypePrimitive(t))
          }
        )
      db.views.create(schTable, htSch)
      
      logger.debug("Dumping FD Graph")
      val fdTable = config.schema.withPrefix("MIMIR_DA_FDG_")
      val htFd = 
        HardTable(
            Seq(
              ID("MIMIR_FD_PARENT") -> TInt(), 
              ID("MIMIR_FD_CHILD") -> TInt(),
              ID("MIMIR_FD_PATH_LENGTH") -> TInt()
            ),
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
        ID("MIMIR_DA_CHOSEN_", config.schema, ":MIMIR_FD_PARENT"),
        config.schema.id,
        db.metadataTable(fdTable),
        Seq(
          ID("MIMIR_FD_CHILD") -> TInt()
        ),
        ID("MIMIR_FD_PARENT"),
        TInt(),
        Some(ID("MIMIR_FD_PATH_LENGTH")),
        fullSchema.map { x => (x._2.toLong -> x._1._1) }.toMap
      )
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
              ID("MIMIR_DA_CHOSEN_", config.schema, ":MIMIR_NORM:$parent:$child"), 
              s"$child in $parent",
              config.query,
              Seq((parent, parentType)),
              child,
              childType,
              None
            )
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
    val model = db.models.get(ID("MIMIR_DA_CHOSEN_", config.schema, ":MIMIR_FD_PARENT"))
    RepairKeyLens.assemble(
      db.metadataTable(ID("MIMIR_DA_FDG_", config.schema)),
      Seq(ID("MIMIR_FD_CHILD")), 
      Seq(
        ID("MIMIR_FD_PARENT") -> model
      ),
      Some(ID("MIMIR_FD_PATH_LENGTH"))
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
    nodeCol: ID, 
    labelCol: ID, 
    typeCol: Option[ID],
    query: Operator
  ): Operator = 
  {

    val keepColumns = 
      query.columnNames
        .filter { !_.equals(nodeCol) }
        .map { col => col -> Var(col) } ++
        Seq( labelCol -> Var(ID("ATTR_NAME")) ) ++
        typeCol.map { col => col -> Var(ID("ATTR_TYPE")) }

    val ret = 
      query.join(db.metadataTable(ID("MIMIR_DA_SCH_", config.schema)))
           .filter( Var(nodeCol).eq(Var(ID("ATTR_NODE"))) )
           .mapByID(keepColumns:_*)
    //sanity check: 
    try {
      db.typechecker.schemaOf(ret)
    } catch {
      case e:MissingVariable => throw new Exception(e.getMessage + "\n" + ret)
    }
    return ret
  }


  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val spanningTree = spanningTreeLens(db, config)
    logger.trace(s"Table Catalog Spanning Tree: \n$spanningTree")
    val tableQuery = 
      convertNodesToNamesInQuery(db, config, ID("TABLE_NODE"), ID("TABLE_NAME"), None,
        spanningTree
          .map( "TABLE_NODE" -> Var(ID("MIMIR_FD_PARENT")) )
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

    // First compute all of the non-leaf node's children.
    // We start with the spanning tree, which is given in terms of Parent/Child NodeIDs
    //  - All of the children define the attribute name
    //  - All of the parents define the table that the attribute belongs to
    val childAttributeQuery =
      convertNodesToNamesInQuery(db, config, ID("MIMIR_FD_CHILD"), ID("ATTR_NAME"), Some(ID("ATTR_TYPE")),
        convertNodesToNamesInQuery(db, config, ID("MIMIR_FD_PARENT"), ID("TABLE_NAME"), None,
          spanningTree
        )
      ).addColumns("IS_KEY" -> BoolPrimitive(false))

    // The parent node is also part of the table, so figure out which are the non-leaf nodes
    // and make them part of the table they define.
    val parentAttributeQuery =
        convertNodesToNamesInQuery(db, config, ID("TABLE_NODE"), ID("TABLE_NAME"), Some(ID("ATTR_TYPE")),
          // SQLite does something stupid with FIRST that prevents it from figuring out that 
          // -1 is an integer.  Add 1 to force it to realize that it's dealing with a number
          spanningTree
            .map( "TABLE_NODE" -> Var(ID("MIMIR_FD_PARENT")) )
            .distinct
            .filter( Comparison(Cmp.Gt, Arithmetic(Arith.Add, Var(ID("TABLE_NODE")), IntPrimitive(1)), IntPrimitive(0)) )
        )
        .map(
          "TABLE_NAME" -> Var(ID("TABLE_NAME")), 
          "ATTR_NAME" -> Var(ID("TABLE_NAME")), 
          "ATTR_TYPE" -> Var(ID("ATTR_TYPE")),
          "IS_KEY" -> BoolPrimitive(false)
        )
      

    val jointQuery =
      Union(childAttributeQuery, parentAttributeQuery)
    logger.trace(s"Attr Catalog Query: \n$jointQuery")
    return jointQuery
  }
  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] = 
  {
    db.queryMetadata(
      attrCatalogFor(db, config)
        .filter(Var(ID("TABLE_NAME")).eq(table))
        .project("ATTR_NAME")
    ) { results => 
      val attrs:Seq[ID] = results.map { row => ID(row(0).asString) }.toSeq 

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
              db.models.get(ID("MIMIR_DA_CHOSEN_",config.schema,s":MIMIR_NORM:$table:$attr"))
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
  name: ID, 
  context: String, 
  source: Operator, 
  keys: Seq[(ID, Type)], 
  target: ID,
  targetType: Type,
  scoreCol: Option[ID],
  attrLookup: Map[Long,ID]
) extends RepairKeyModel(name, context, source, keys, target, targetType, scoreCol)
{
  override def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    getFeedback(idx, args) match {
      case None => {
        val possibilities = getDomain(idx, args, hints).sortBy(-_._2).map { _._1.asLong }
        val best = possibilities.head
        s"${attrLookup(args(0).asLong)} could be organized under any of ${possibilities.map { x =>  attrLookup(x).id+" ("+x+")" }.mkString(", ")}; I chose ${attrLookup(best) }"
      }
      case Some(choice) => 
        s"You told me to organize ${attrLookup(args(0).asLong)} under ${attrLookup(choice.asLong)}"
    }
  }
}