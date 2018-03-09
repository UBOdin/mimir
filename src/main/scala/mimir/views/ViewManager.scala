package mimir.views;

import java.sql.SQLException;

import net.sf.jsqlparser.statement.select.SelectBody

import mimir._
import mimir.algebra._
import mimir.provenance._
import mimir.ctables._
import mimir.exec._
import mimir.exec.mode._
import mimir.serialization._
import com.typesafe.scalalogging.slf4j.LazyLogging


class ViewManager(db:Database) extends LazyLogging {
  
  val viewTable = "MIMIR_VIEWS"

  /**
   * Initialize the view manager: 
   *   - Create a system catalog table to store information about views
   */
  def init(): Unit = 
  {
    db.requireMetadataTable(viewTable, Seq(
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
    db.metadataBackend.update(s"INSERT INTO $viewTable(NAME, QUERY, METADATA) VALUES (?,?,0)", 
      Seq(
        StringPrimitive(name), 
        StringPrimitive(Json.ofOperator(query).toString)
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
    db.metadataBackend.update(s"UPDATE $viewTable SET QUERY=? WHERE NAME=?", 
      Seq(
        StringPrimitive(Json.ofOperator(query).toString),
        StringPrimitive(name)
      )) 
    if(properties.isMaterialized){
      db.metadataBackend.update("DROP TABLE ${name}")
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
    db.metadataBackend.update(s"DELETE FROM $viewTable WHERE NAME=?", 
      Seq(StringPrimitive(name))
    )
    if(properties.isMaterialized){
      db.metadataBackend.update("DROP TABLE ${name}")
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
      db.metadataBackend.resultRows(s"SELECT QUERY, METADATA FROM $viewTable WHERE name = ?", 
        Seq(StringPrimitive(name))
      )
    results.take(1).headOption.map(_.toSeq).map( 
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
    ) = BestGuess.rewriteRaw(db, properties.query)

    val columns:Seq[String] = baseSchema.map(_._1)
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

    //val (inlinedSQL:SelectBody, _) = db.compiler.sqlForBackend(completeQuery)
        
    //logger.debug(s"QUERY: $inlinedSQL")

    //db.metadataBackend.selectInto(name, inlinedSQL.toString)
    db.backend.createTable(name, completeQuery)

    db.metadataBackend.update(s"""
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
    db.metadataBackend.update(s"""
      DROP TABLE $name
    """)
    db.metadataBackend.update(s"""
      UPDATE $viewTable SET METADATA = 0 WHERE NAME = ?
    """, Seq(
      StringPrimitive(name)
    ))
    db.backend.invalidateCache
  }

  /**
   * List all views known to the view manager
   * @return     A list of all view names
   */
  def list(): List[String] =
  {
    db.metadataBackend.
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
      db.metadataTable(viewTable)
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
        ("TABLE_NAME", TString()), 
        ("ATTR_NAME", TString()),
        ("ATTR_TYPE", TString()),
        ("IS_KEY", TBool())
      ),
      list().flatMap { view => 
        db.typechecker.schemaOf(get(view).get.query).map { case (col, t) =>
          Seq(
            StringPrimitive(view),
            StringPrimitive(col),
            TypePrimitive(t),
            BoolPrimitive(false)
          )
        }

      }
    )
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

        logger.debug(s"Using materialized view: Materialized '$name' with { ${wantAnnotations.mkString(", ")} } <- ${metadata.table}")

        Project(
          metadata.schemaWith(wantAnnotations).map { col => 
            ProjectArg(col._1, Var(col._1))
          },
          metadata.table
        )
      }
      case AdaptiveView(schema, name, query, wantAnnotations) => {
        return resolve(query)
      }

      case _ =>
        op.recur(resolve(_))
    }
  }

}
