package mimir.sql;

import java.sql._

import mimir.algebra.Type

import scala.collection.mutable.ListBuffer
;

object JDBCUtils {
  def convertSqlType(t: Int): Type.T = { 
    t match {
      case (java.sql.Types.FLOAT | 
            java.sql.Types.DOUBLE)   => Type.TFloat
      case (java.sql.Types.INTEGER)  => Type.TInt
      case (java.sql.Types.DATE)     => Type.TDate
      case (java.sql.Types.VARCHAR |
            java.sql.Types.NULL)     => Type.TString
      case (java.sql.Types.ROWID)    => Type.TRowId
    }
  }
}

class JDBCBackend(conn: Connection) extends Backend
{
  def execute(sel: String): ResultSet = 
  {
    //println(sel)
    val stmt = conn.createStatement();
    val ret = stmt.executeQuery(sel)
    stmt.closeOnCompletion();
    return ret;
  }
  def execute(sel: String, args: List[String]): ResultSet = 
  {
    //println(""+sel+" <- "+args)
    val stmt = conn.prepareStatement(sel);
    var i: Int = 0;
    args.map( (a) => {
      i += 1;
      stmt.setString(i, a);
    })
    stmt.executeQuery()
  }
  
  def update(upd: String): Unit =
  {
    val stmt = conn.createStatement()
    stmt.executeUpdate(upd)
    stmt.close()
  }

  def update(upd: List[String]): Unit =
  {
    val stmt = conn.createStatement()
    upd.indices.foreach(i => stmt.addBatch(upd(i)))
    stmt.executeBatch()
    stmt.close()
  }

  def update(upd: String, args: List[String]): Unit =
  {
    val stmt = conn.prepareStatement(upd);
    var i: Int = 0;
    args.map( (a) => {
      i += 1;
      stmt.setString(i, a);
    })
    stmt.execute()
    stmt.close();
  }
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]] =
  {
    val tables = this.getAllTables().map{(x) => x.toUpperCase}
    if(!tables.contains(table.toUpperCase)) return None

    val cols = conn.getMetaData()
      .getColumns(null, null, table, "%")

    var ret = List[(String, Type.T)]()

    while(cols.isBeforeFirst()){ cols.next(); }
    while(!cols.isAfterLast()){
      ret = ret ++ List((
          cols.getString("COLUMN_NAME").toUpperCase, 
          JDBCUtils.convertSqlType(cols.getInt("DATA_TYPE"))
        ))
      cols.next();
    }
    return Some(ret);
  }

  def getAllTables(): List[String] = {
    val metadata = conn.getMetaData()
    val tables = metadata.getTables(null, null, "", null)

    val tableNames = new ListBuffer[String]()

    while(tables.next()) {
      tableNames.append(tables.getString("TABLE_NAME"))
    }

    tables.close()
    tableNames.toList
  }

  def close(): Unit = {
    conn.close()
  }
}