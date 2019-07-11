package mimir.data

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import java.sql.SQLException
import sparsity.Name

import mimir.algebra._


/**
 * A connector bridging Mimir to a source of tables like an
 * external JDBC database or Hive.  Also used internally
 * by Mimir to represent tables it provides internally.
 */
trait SchemaProvider extends LazyLogging {
  // Implementations must define these...

  /** 
   * @return The identities of all tables provided by this schema provider.
   */
  def listTables: Iterable[ID]

  /**
   * @param table   The identity of a table
   * @return        The schema of the table or None if the table is not provided
   */
  def tableSchema(table: ID): Option[Seq[(ID, Type)]]

  /**
   * @param table   The identity of a table
   * @return        True if the table exists
   */
  def tableExists(table: ID): Boolean = (tableSchema(table) != None)

  // The following three methods are optional, but at least 
  // one must return a non-None value **/

  /**
   * Retrieve a table as a data frame if it is possible to do so natively
   * @param table   The identity of a table
   * @return        A dataframe instance of this table or None
   * 
   * One of the methods [dataframe] or [view] must be implemented.  In general,
   * it should not be necessary to implement more than one of these.  In other
   * words, the implementation of this method should not materialize the table
   * as a data frame if it is not natively representable as such.
   *
   * NOTE: 
   *   In addition to the explicitly defined attributes (ie., as returned
   *   by [tableSchema]), the following attributes MUST be defined in the 
   *   schema of the logical plan: 
   *    - ROWID (Integer): A unique, stable identifier for each row
   */
  def logicalplan(table: ID): Option[LogicalPlan]

  /**
   * Retrieve a table as a view if it is defined as such
   * @param table   The identity of a table
   * @return        The view defining this table or None
   * 
   * One of the methods [dataframe] or [view] must be implemented.  In general,
   * it should not be necessary to implement more than one of these.  In other
   * words, the implementation of this method should not materialize the table
   * as a data frame if it is not natively representable as such.
   */
  def view(table: ID): Option[Operator]

  //
  // These come for free but may be overridden
  //

  def isVisible: Boolean = true

  def resolveTableCaseInsensitive(table: String): Option[ID] = 
    listTables.find { _.id.equalsIgnoreCase(table) } 
  def resolveTableByName(table: Name): Option[ID] = 
  {
    if(table.quoted){ 
      val id = ID(table.name)
      if(tableExists(id)) { return Some(id) } else { return None }
    } else { 
      resolveTableCaseInsensitive(table.name)
    }
  }

  /**
   * A query to retrieve the tables defined by this provider
   * @return        A query to retrieve the tables defined by this provider
   *
   * This method is used to show schemas to the user (e.g., as part of sys.tables).
   * A default implementation of this method is provided in terms of [listTables].
   * It may be overridden by specific implementations of SchemaProvider, for  
   * example to capture schema-level uncertainty.
   */
  def listTablesQuery: Operator =
    HardTable(
      Seq( ID("TABLE_NAME") -> TString() ), 
      listTables.map { _.id }
                .map { StringPrimitive(_) }
                .map { Seq(_) }
                .toSeq
    )

  /**
   * A query to retrieve the schema of all tables defined by this provider
   * @return        A query to retrieve the schema of all
   *
   * This method is used to show schemas to the user (e.g., as part of sys.attrs).
   * A default implementation of this method is provided in terms of [tableSchema].
   * It may be overridden by specific implementations of SchemaProvider, for  
   * example to capture schema-level uncertainty.
   */
  def listAttributesQuery: Operator = 
    HardTable(
      Seq(
        ID("TABLE_NAME")  -> TString(), 
        ID("ATTR_NAME")   -> TString(),
        ID("ATTR_TYPE")   -> TString(),
        ID("IS_KEY")      -> TBool()
      ),
      listTables.flatMap { table => 
        tableSchema(table)
          .toSeq.flatten
          .map { case (attr, t) =>
            Seq(
              StringPrimitive(table.id), 
              StringPrimitive(attr.id),
              TypePrimitive(t),
              BoolPrimitive(false)
            )
          }
      }.toSeq
    )

  /**
   * A representation of one of this provider's tables as an Operator
   * @param providerName   The name of this provider
   * @param tableName      The name of the table to produce an operator for
   * @return               An operator that represents the requested table
   * 
   * This function generalizes the [view] method to return a [Table] if 
   * the requested table can not be represented as a view.
   */ 
  def tableOperator(providerName: ID, tableName: ID, tableAlias: ID): Operator =
    view(tableName)
      .getOrElse { 
        Table(
          tableName, 
          tableAlias, 
          providerName, 
          tableSchema(tableName)
            .getOrElse { 
              throw new SQLException(s"No such table or view '$tableName'")
            },
          Nil
        )
      }

}
