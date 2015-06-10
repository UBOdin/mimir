package mimir.sql;

import java.sql._;
import net.sf.jsqlparser.statement.select.Select;
import mimir.algebra.Type;

object JDBCUtils {
  def convertSqlType(t: Int): Type.T = { 
    t match {
      case (java.sql.Types.FLOAT | 
            java.sql.Types.DOUBLE)   => Type.TFloat
      case (java.sql.Types.INTEGER |
            java.sql.Types.ROWID)    => Type.TInt
      case (java.sql.Types.DATE)     => Type.TDate
      case (java.sql.Types.VARCHAR)  => Type.TString
    }
  }
}

class JDBCBackend(conn: Connection) extends Backend
{
  def execute(sel: String): ResultSet = 
  {
    // println(sel)
    val stmt = conn.createStatement();
    val ret = stmt.executeQuery(sel)
    stmt.closeOnCompletion();
    return ret;
  }
  def execute(sel: String, args: List[String]): ResultSet = 
  {
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
    val stmt = conn.createStatement();
    stmt.executeUpdate(upd)
    stmt.close();
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
  
  def getTableSchema(table: String): List[(String, Type.T)] = 
  {
    val cols = 
      conn.getMetaData().getColumns(null, null, table, "%");
    var ret = List[(String, Type.T)]();
    while(cols.isBeforeFirst()){ cols.next(); }
    while(!cols.isAfterLast()){
      ret = ret ++ List((
          cols.getString("COLUMN_NAME").toUpperCase, 
          JDBCUtils.convertSqlType(cols.getInt("DATA_TYPE"))
        ))
      cols.next();
    }
    return ret;
  }
}