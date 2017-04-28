package mimir.views;

import java.sql.SQLException;
import mimir._;
import mimir.algebra._;
import mimir.provenance._;
import mimir.ctables._;
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.sql.JDBCBackend


class ViewManager(db:Database) extends LazyLogging {
  
  val viewTable = "MIMIR_VIEWS"

  /**
   * Initialize the view manager: 
   *   - Create a system catalog table to store information about views
   */
  def init(): Unit = 
  {
    db.requireTable(viewTable, Seq(
        ("NAME", TString()),
        ("QUERY", TString()),
        ("METADATA", TInt())
      ), 
      primaryKey = Some("NAME")
    )
  }

  /**
   * Instantiate a new view
   * @param  name           The name of the view to create
   * @param  query          The query to back the view with
   * @throws SQLException   If a view or table with the same name already exists
   */
  def create(name: String, query: Operator): Unit =
  {
    logger.debug(s"CREATE VIEW $name AS $query")
    if(db.tableExists(name)){
      throw new SQLException(s"View '$name' already exists")
    }
    db.backend.update(s"INSERT INTO $viewTable(NAME, QUERY, METADATA) VALUES (?,?,0)", 
      Seq(
        StringPrimitive(name), 
        StringPrimitive(db.querySerializer.serialize(query))
      ))
    // updateMaterialization(name)
  }

  /**
   * Alter an existing view to use a new query
   * @param  name           The name of the view to alter
   * @param  query          The new query to back the view with
   * @throws SQLException   If a view or table with the same name already exists
   */
  def alter(name: String, query: Operator): Unit =
  {
    val properties = apply(name)
    db.backend.update(s"UPDATE $viewTable SET QUERY=? WHERE NAME=?", 
      Seq(
        StringPrimitive(db.querySerializer.serialize(query)),
        StringPrimitive(name)
      )) 
    if(properties.isMaterialized){
      db.backend.update("DROP TABLE ${name}")
      materialize(name)
    }
  }

  /**
   * Drop an existing view
   * @param  name           The name of the view to alter
   * @throws SQLException   If a view or table with the same name already exists
   */
  def drop(name: String): Unit =
  {
    val properties = apply(name)
    db.backend.update(s"DELETE FROM $viewTable WHERE NAME=?", 
      Seq(StringPrimitive(name))
    )
    if(properties.isMaterialized){
      db.backend.update("DROP TABLE ${name}")
    }
  }

  /**
   * Obtain the properties of the specified view
   * @param name    The name of the view to look up
   * @return        Properties for the specified query or None if the view doesn't exist
   */
  def get(name: String): Option[ViewMetadata] =
  {
    val results = 
      db.backend.resultRows(s"SELECT QUERY, METADATA FROM $viewTable WHERE name = ?", 
        Seq(StringPrimitive(name))
      )
    results.take(1).headOption.map(_.toSeq).map( 
      { 
        case Seq(StringPrimitive(s), IntPrimitive(meta)) => {
          val query =
            db.querySerializer.deserializeQuery(s)
          val isMaterialized = 
            meta != 0
          
          new ViewMetadata(name, query, db.bestGuessSchema(query), isMaterialized)
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
  def apply(name: String): ViewMetadata =
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
  def materialize(name: String): Unit =
  {
    if(db.backend.getTableSchema(name) != None){
      throw new SQLException(s"View '$name' is already materialized")
    }
    val properties = apply(name)
    val (
      query,
      baseSchema,
      columnTaint,
      rowTaint,
      provenance
    ) = db.compiler.compileInline(properties.query)

    val columns = baseSchema.map(_._1)
    val colbg = db.bestGuessSchema(properties.query)
    
    val completeQuery = 
      Project(
        colbg.map { col => ProjectArg(col._1, Function("CAST",Seq(Var(col._1), TypePrimitive(col._2)))) } ++
        columns.map { col => ProjectArg(
          CTPercolator.mimirColDeterministicColumnPrefix + col,
          Conditional(columnTaint(col), IntPrimitive(1), IntPrimitive(0))
        )} ++
        Seq(ProjectArg(CTPercolator.mimirRowDeterministicColumnName, 
          Function("CAST", Seq(Conditional(rowTaint, IntPrimitive(1), IntPrimitive(0)), TypePrimitive(TInt())))
        )) ++
        provenance.map { col => ProjectArg(col, Var(col)) } ++
        provenance.map { col => ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix + col, IntPrimitive(1))},
        query
      )

    val inlinedSQL = db.compiler.sqlForBackend(completeQuery)
    val tableSchema = completeQuery.schema
    val tableDef = tableSchema.map( x => x._1+" "+Type.toString(x._2) ).mkString(",")
    val tableCols = tableSchema.map( _._1 ).mkString(",")
    val colFillIns = tableSchema.map( _ => "?").mkString(",")
    //val insertCmd = s"INSERT INTO $name( $tableCols ) VALUES ($colFillIns);"
    val insertCmd = s"INSERT INTO $name $inlinedSQL;"
    db.backend.update(  s"CREATE TABLE $name ( $tableDef );"  )
    db.backend.update(insertCmd)
    //db.backend.asInstanceOf[JDBCBackend].fastUpdateBatch(insertCmd, db.backend.execute(inlinedSQL))
    //db.backend.selectInto(name, inlinedSQL.toString)

    logger.debug(s"MATERIALIZE: $name(${completeQuery.schema.mkString(",")})")
    logger.debug(s"QUERY: $inlinedSQL")

    db.backend.update(s"""
      UPDATE $viewTable SET METADATA = 1 WHERE NAME = ?
    """, Seq(
      StringPrimitive(name)
    ))
  }

  /**
   * Remove the materialization for the specified view
   * @param  name        The name of the view to dematerialize
   */
  def dematerialize(name: String): Unit = {
    if(db.backend.getTableSchema(name) == None){
      throw new SQLException(s"View '$name' is not materialized")
    }
    db.backend.update(s"""
      DROP TABLE $name
    """)
    db.backend.update(s"""
      UPDATE $viewTable SET METADATA = 0 WHERE NAME = ?
    """, Seq(
      StringPrimitive(name)
    ))
  }

  /**
   * List all views known to the view manager
   * @return     A list of all view names
   */
  def list(): List[String] =
  {
    db.backend.
      resultRows(s"SELECT name FROM $viewTable").
      flatten.
      map( _.asString ).
      toList
  }

  /**
   * Return a query that can be used to list all views known to Mimir.
   * Used mainly by Mimir's system catalog
   * @return    A query that returns a list of all known views when executed
   */
  def listViewsQuery: Operator = 
  {
    Project(
      Seq(
        ProjectArg("TABLE_NAME", Var("NAME"))
      ),
      db.getTableOperator(viewTable)
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
    EmptyTable(Seq(
      ("TABLE_NAME", TString()), 
      ("ATTR_NAME", TString()),
      ("ATTR_TYPE", TString()),
      ("IS_KEY", TBool())
    ))
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
          logger.debug(s"Invalid materialized view: '$name' is not materialized")
          return resolve(query)
        }
        val haveAnnotations = Set(
          ViewAnnotation.BEST_GUESS,
          ViewAnnotation.TAINT,
          ViewAnnotation.PROVENANCE
        )
        val missingAnnotations = wantAnnotations -- haveAnnotations

        if(!missingAnnotations.isEmpty) {
          logger.debug(s"Invalid materialized view: Missing { ${missingAnnotations.mkString(", ")} } from '$name'")
          return resolve(query)
        }

        logger.debug(s"Valid materialized view: Using Materialized '$name' with { ${wantAnnotations.mkString(", ")} } <- ${metadata.table}")

        Project(
          metadata.schemaWith(wantAnnotations).map { col => 
            ProjectArg(col._1, Var(col._1))
          },
          metadata.table
        )
      }

      case _ =>
        op.recur(resolve(_))
    }
  }

}