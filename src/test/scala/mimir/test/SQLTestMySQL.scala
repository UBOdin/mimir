package mimir.test

import mimir.Database
import mimir.sql.JDBCBackend
import org.specs2.mutable.Specification

object DBTestMySQLInstance {

  private var db: Database = null;

  def get(DBName: String): Database = {
    db = new Database(new JDBCBackend("mysql", DBName));
    db.backend.open();
    db
  }
}

abstract class SQLTestSpecsMySQL(val backend:String, dbName: String)
  extends Specification
{
    def db = DBTestMySQLInstance.get(dbName);
}
