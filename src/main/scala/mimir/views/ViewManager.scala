package mimir.views;

import java.sql.SQLException;
import mimir._;
import mimir.algebra._;
import mimir.provenance._;
import mimir.ctables._;
import com.typesafe.scalalogging.slf4j.LazyLogging

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
    db.backend.update(s"INSERT INTO $viewTable(NAME, QUERY, MATERIALIZED) VALUES (?,?,0)", 
      List(
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
      List(
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
      List(StringPrimitive(name))
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
        List(StringPrimitive(name))
      )
    results.take(1).headOption.map(_.toSeq).map( 
      { 
        case Seq(StringPrimitive(s), IntPrimitive(meta)) => {
          val query =
            db.querySerializer.deserializeQuery(s)
          val isMaterialized = 
            ViewMetadata.isMaterialized(meta)
          val rowId = 
            if(ViewMetadata.hasRowId(meta)){
              Some(Provenance.compile(query)._2)    
            } else { None}
          val taint =
            if(ViewMetadata.hasTaint(meta)){
              Some( (
                query.schema.map { 
                  col => (col._1 -> Var(CTPercolator.mimirColDeterministicColumnPrefix+col._1))
                }.toMap,
                Var(CTPercolator.mimirRowDeterministicColumnName)
              ) )
            } else { None }
          
          new ViewMetadata(query, isMaterialized, rowId, taint)
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
    val inlinedSQL = db.compiler.sqlForBackend(query)
    db.backend.selectInto(name, inlinedSQL)
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

}