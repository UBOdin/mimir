package mimir.oracle

import java.io.{File, FileReader}

import mimir.Database
import mimir.algebra.{Var, ProjectArg, Project}
import mimir.parser.MimirJSqlParser
import mimir.sql.JDBCBackend
import org.specs2.mutable.Specification
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select

object OracleTpchQueries extends Specification {

  val queryFolder = new File("../test/tpch_queries/oracle/small/noagg/")
  val q1 = "1.sql"
  val q3 = "3.sql"
  val q5 = "5.sql"
  val q9 = "9.sql"

  val dbName = "osmall.db"
  val backend = "oracle"
  val db = new Database(new JDBCBackend(backend, dbName))

  "Mimir" should  {
    "Run tpch query 5 on Oracle" >> {
      if(new File("config/jdbc.property").exists()){

        db.backend.open()
        val parser = new MimirJSqlParser(new FileReader(new File(queryFolder, q5)))
        val sel = parser.Statement().asInstanceOf[Select]
        val raw = db.sql.convert(sel)
        db.check(raw)
        val rawPlusRowID = Project(
          List(ProjectArg("MIMIR_PROVENANCE", Var("ROWID_MIMIR"))) ++
            raw.schema.map( (x) => ProjectArg(x._1, Var(x._1))),
          raw)
        val firstRow = db.query(rawPlusRowID){ _.next }

        db.backend.close()
        firstRow.tuple.size must beGreaterThan(0)
      } else {
        skipped("Skipping Oracle Tests (add a config/jdbc.property to test)")
      }
      ok
    }
  }
}
