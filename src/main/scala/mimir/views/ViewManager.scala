package mimir.views

import java.sql.SQLException
import sparsity.Name

import mimir._
import mimir.algebra._
import mimir.provenance._
import mimir.ctables._
import mimir.exec._
import mimir.exec.mode._
import mimir.serialization._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.metadata._
import mimir.data.SchemaProvider


class ViewManager(db:Database) 
  extends SchemaProvider
  with LazyLogging
{
  
  var viewTable: MetadataMap = null

  /**
   * Initialize the view manager: 
   *   - Create a system catalog table to store information about views
   */
  def init(): Unit = 
  {
    viewTable = db.metadata.registerMap(ID("MIMIR_VIEWS"), Seq(InitMap(Seq(
        ID("QUERY") -> TString(),
        ID("METADATA") -> TInt()
      )
    )))
  }

  /**
   * Instantiate a new view
   * @param  name           The name of the view to create
   * @param  query          The query to back the view with
   * @throws SQLException   If a view or table with the same name already exists
   */
  def create(name: ID, query: Operator): Unit =
  {
    logger.debug(s"CREATE VIEW $name AS $query")
    if(db.catalog.tableExists(name)){
      throw new SQLException(s"View '$name' already exists")
    }
    viewTable.put(name, Seq(
      StringPrimitive(Json.ofOperator(query).toString),
      IntPrimitive(0)
    ))
    // updateMaterialization(name)
  }

  /**
   * Alter an existing view to use a new query
   * @param  name           The name of the view to alter
   * @param  query          The new query to back the view with
   * @throws SQLException   If a view or table with the same name already exists
   */
  def alter(name: ID, query: Operator): Unit =
  {
    val properties = apply(name)
    viewTable.update(name, Map(
      ID("QUERY") -> StringPrimitive(Json.ofOperator(query).toString)
    )) 
    if(properties.isMaterialized){
      materialize(name)
    }
  }

  /**
   * Drop an existing view
   * @param  name           The name of the view to alter
   * @throws SQLException   If a view or table with the same name already exists
   */
  def drop(name: ID, ifExists: Boolean = false): Unit =
  {
    val properties = 
      get(name) match {
        case Some(properties) => properties
        case None => if(ifExists){ return }
                     else { throw new SQLException(s"Unknown View '$name'") }
      }
    viewTable.rm(name)
    if(properties.isMaterialized){ 
      db.catalog.bulkStorageProvider().dropStoredTable(properties.materializedName)
    }
  }

  /**
   * Obtain the properties of the specified view
   * @param name    The name of the view to look up
   * @return        Properties for the specified query or None if the view doesn't exist
   */
  def get(name: Name): Option[ViewMetadata] = get(ID.upper(name))
  def get(name: ID): Option[ViewMetadata] = 
  {
    val results = 
    logger.trace(s"Getting View $name")
    viewTable.get(name).map(_._2.toSeq).map( 
      { 
        case Seq(StringPrimitive(s), IntPrimitive(meta)) => {
          val query = Json.toOperator(Json.parse(s))
          val isMaterialized = 
            meta != 0
          
          new ViewMetadata(name, query, isMaterialized, db)
        }
      }
    )
  }

  /**
   * Obtain properties for the specified view
   * @param name            The name of the view to look up
   * @return                Properties for the specified query
   * @throws SQLException   If the specified view does not exist
   */
  def apply(name: Name): ViewMetadata = apply(ID.upper(name))
  def apply(name: ID): ViewMetadata = 
  {
    get(name) match {
      case None => 
        throw new SQLException(s"Unknown View '$name'")
      case Some(properties) =>
        properties
    }
  }

  /**
   * Materialize the specified view
   * @param  name        The name of the view to materialize
   */
  def materialize(name: ID): Unit =
  {
    val properties = apply(name)
    val bulkStorage = db.catalog.bulkStorageProvider()
    if(bulkStorage.tableExists(properties.materializedName)){
      throw new SQLException(s"View '$name' is already materialized")
    }
    val (
      query, 
      baseSchema,
      columnTaint,
      rowTaint,
      provenance
    ) = BestGuess.rewriteRaw(db, properties.query)

    val columns:Seq[ID] = baseSchema.map(_._1)
    logger.debug(s"SCHEMA: $columns; $rowTaint; $columnTaint; $provenance")

    val completeQuery = 
      Project(
        columns.map { col => ProjectArg(col, Var(col)) } ++
        Seq(
          ProjectArg(
            ViewAnnotation.taintBitVectorColumn,
            ExpressionUtils.boolsToBitVector(
              Seq(rowTaint)++columns.map { col => columnTaint(col) }
            )
          )
        )++
        (provenance.toSet -- columns.toSet).map { col => ProjectArg(col, Var(col)) },
        query
      )

    logger.debug(s"RAW: $completeQuery")
    logger.debug(s"MATERIALIZE: $name(${completeQuery.columnNames.mkString(",")})")

    bulkStorage.createStoredTableAs(completeQuery, properties.materializedName, db)

    viewTable.update(name, Map(ID("METADATA") -> IntPrimitive(1)))
  }

  /**
   * Remove the materialization for the specified view
   * @param  name        The name of the view to dematerialize
   */
  def dematerialize(name: ID): Unit = { 
    val metadata = apply(name)
    if(metadata.isMaterialized){
      db.catalog.bulkStorageProvider().dropStoredTable(metadata.materializedName)
      viewTable.update(name, Map(ID("METADATA") -> IntPrimitive(0)))
    } else { 
      throw new SQLException(s"$name is already materialized")
    }
  }

  /**
   * List all views known to the view manager
   * @return     A list of all view names
   */
  def list(): Seq[ID] =
    viewTable.keys

  /**
   * Return a query that can be used to list all views known to Mimir.
   * Used mainly by Mimir's system catalog
   * @return    A query that returns a list of all known views when executed
   */
  def listViewsQuery: Operator = 
  {
    HardTable(
      Seq( ID("TABLE_NAME") -> TString() ),
      list.map { col => StringPrimitive(col.id) }.map { Seq(_) }
    )
  }

  /**
   * Return a query that can be used to list the attributes of all views known to Mimir.
   * Used mainly by Mimir's system catalog
   * (presently unimplemented)
   */
  def listAttrsQuery: Operator = 
  {
    logger.warn("Constructing lens attribute list not implemented yet")
    HardTable(Seq(
        ID("TABLE_NAME") -> TString(), 
        ID("ATTR_NAME")  -> TString(),
        ID("ATTR_TYPE")  -> TString(),
        ID("IS_KEY")     -> TBool()
      ),
      list().flatMap { view => 
        db.typechecker.schemaOf(get(view).get.query).map { case (col, t) =>
          Seq(
            StringPrimitive(view.id),
            StringPrimitive(col.id),
            TypePrimitive(t),
            BoolPrimitive(false)
          )
        }

      }
    )
  }

  /**
   * Rebuild Adaptive Views: The first step in query rewriting.
   *
   * For each adaptive view in the provided query, rerun viewFor so we get 
   *  an updated view in case of hard-coded model use in the view creation
   *  and subsequent feedback.  
   * @param op    The operator to rebuild adaptive views in
   * @return      A version of the tree for `op` with refreshed View nodes.
   */
   def rebuildAdaptiveViews(op:Operator): Operator = op match {
     case AdaptiveView(schema, name, oper, anno) => db.adaptiveSchemas.viewFor(schema, name).get
     case _ => op.recur(rebuildAdaptiveViews(_))
   }
  
  /**
   * Resolve views: The final step in query rewriting.
   *
   * For each view in the provided query, decide whether the view can be resolved to
   * a materialized view table, or whether it needs to be executed directly.  
   * @param op    The operator to resolve views in
   * @return      A version of the tree for `op` with no View nodes.
   */
  def resolve(op: Operator): Operator =
  {
    op match {
      case View(name, query, wantAnnotations) => {
        val metadata = apply(name)
        if(!metadata.isMaterialized){
          logger.debug(s"Not using materialized view: '$name' is not materialized")
          return resolve(query)
        }
        val haveAnnotations = Set(
          ViewAnnotation.BEST_GUESS,
          ViewAnnotation.TAINT,
          ViewAnnotation.PROVENANCE
        )
        val missingAnnotations = wantAnnotations -- haveAnnotations

        if(!missingAnnotations.isEmpty) {
          logger.debug(s"Not using materialized view: Missing { ${missingAnnotations.mkString(", ")} } from '$name'")
          return resolve(query)
        }

        logger.debug(s"Using materialized view: Materialized '$name' with { ${wantAnnotations.mkString(", ")} } <- ${metadata.materializedName}")

        Project(
          metadata.schemaWith(wantAnnotations).map { col => 
            ProjectArg(col._1, Var(col._1))
          },
          materializedTableOperator(metadata)
        )
      }
      case AdaptiveView(schema, name, query, wantAnnotations) => {
        return resolve(query)
      }

      case _ =>
        op.recur(resolve(_))
    }
  }

  def listTables() = list()
  def tableSchema(table: ID) = 
    get(table).map { view => db.typechecker.schemaOf(view.query) }

  def logicalplan(table: ID) = None
  def view(table: ID) = Some(apply(table).operator)

  def materializedTableOperator(table: ID): Operator =
    materializedTableOperator(apply(table:ID))
  def materializedTableOperator(metadata: ViewMetadata): Operator =
  {
    // retain the base columns
    var projectedColumns = 
      metadata.schema.map { case (col, _) => ProjectArg(col, Var(col)) } 

    // if we want the taint columns, decode them
    if(metadata.annotations(ViewAnnotation.TAINT)){
      projectedColumns ++= 
        metadata.schemaWith(Set(ViewAnnotation.PROVENANCE)).zipWithIndex.map { case ((col, _), idx) => 
          ProjectArg(
            OperatorDeterminism.mimirColDeterministicColumn(col),
            Comparison(Cmp.Eq,
              Arithmetic(Arith.BitAnd,
                Var(ViewAnnotation.taintBitVectorColumn),
                IntPrimitive(1l << (idx+1))
              ),
              IntPrimitive(1l << (idx+1))
            )
          )
        } :+ 
          ProjectArg(
            OperatorDeterminism.mimirRowDeterministicColumnName,
            Comparison(Cmp.Eq,
              Arithmetic(Arith.BitAnd,
                Var(ViewAnnotation.taintBitVectorColumn),
                IntPrimitive(1l)
              ),
              IntPrimitive(1l)
            )
          )
    }

    if(metadata.annotations(ViewAnnotation.PROVENANCE)){
      projectedColumns ++= 
        metadata.provenanceCols.map { col => ProjectArg(col, Var(col)) }
    }

    return Project(projectedColumns, 
      Table(
        metadata.materializedName, 
        db.catalog.bulkStorageProviderID,
        metadata.materializedSchema,
        Seq()
      )
    )
  }

}


object ViewManager 
{
  val SCHEMA = ID("VIEW")
  val MATERIALIZED_VIEW_SCHEMA = ID("MATERIALIZED_VIEW")
}