package mimir.exec;

import java.io.{StringReader,BufferedReader,FileReader,File}
import scala.collection.JavaConversions._

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers

import net.sf.jsqlparser.statement.select.{PlainSelect}

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object SqlParserSpec extends Specification with FileMatchers {

	def stmt(s: String) = {
		new MimirJSqlParser(new StringReader(s)).Statement()
	}
	def query(s: String) = 
		db.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def parser = new ExpressionParser(db.lenses.modelForLens)
	def expr = parser.expr _

	val tempDB:String = "tempDB"
	val testData = List[ (String, File, List[String]) ](
			(	"R", new File("../test/r_test/r.csv"), 
				List("A int", "B int", "C int")
			)
		)

	val db:Database = {
		if(tempDB != null){
			val dbFile = new File(new File("databases"), tempDB)
			if(dbFile.exists()){ dbFile.delete(); }
			dbFile.deleteOnExit();
		}
		val d = new Database("testdb", new JDBCBackend("sqlite",
			if(tempDB == null){ "testdb" } else { tempDB.toString }
		))
	    try {
		    d.backend.open();
			d.initializeDBForMimir();
		} catch {
			case e:Exception => e.printStackTrace()

		}
		testData.foreach ( _ match { case ( tableName, tableData, tableCols ) => 
			d.update("CREATE TABLE "+tableName+"("+tableCols.mkString(", ")+");")
			val lines = new BufferedReader(new FileReader(tableData))
			var line: String = lines.readLine()
			while(line != null){
				d.update("INSERT INTO "+tableName+" VALUES (" + line + ");")
				line = lines.readLine()
			}
		})
		d
	}

	"The Sql Parser" should {
		"Handle trivial queries" in {
			db.query("SELECT * FROM R;").allRows must be equalTo List( 
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),NullPrimitive(),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)

			db.query("SELECT A FROM R;").allRows must be equalTo List(
				List(IntPrimitive(1)),
				List(IntPrimitive(1)),
				List(IntPrimitive(2)),
				List(IntPrimitive(1)),
				List(IntPrimitive(1)),
				List(IntPrimitive(2)),
				List(IntPrimitive(4))
			)
		}

		"Create and query lenses" in {
		 	db.createLens(stmt(
		 		"CREATE LENS SaneR AS SELECT * FROM R WITH MISSING_VALUE('B')"
		 	).asInstanceOf[CreateLens]);
		 	db.optimize(
		 		query("SELECT * FROM SaneR")
		 	) must be equalTo 
		 		Project(List(ProjectArg("A", Var("R_A")), 
		 					 ProjectArg("B", expr(
		 					 	"CASE WHEN R_B IS NULL THEN {{ SANER_1[ROWID_MIMIR] }} ELSE R_B END")),
		 					 ProjectArg("C", Var("R_C"))
		 				), Table("R", Map(("R_A", Type.TInt), 
		 								  ("R_B", Type.TInt), 
		 								  ("R_C", Type.TInt)).toList,
		 							  Map(("ROWID_MIMIR", Type.TRowId)).toList
				))

		 	;
			db.query(query("SELECT * FROM SaneR")).allRows must be equalTo List(
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)
		}

		"Create Lenses with no or multiple arguments" in {
			stmt("CREATE LENS test1 AS SELECT * FROM R WITH MISSING_VALUE('A','B')") must be equalTo new CreateLens("test1", 
					stmt("SELECT * FROM R").asInstanceOf[net.sf.jsqlparser.statement.select.Select].getSelectBody(),
					"MISSING_VALUE",
					List[net.sf.jsqlparser.expression.Expression](
						new net.sf.jsqlparser.expression.StringValue("A"), 
						new net.sf.jsqlparser.expression.StringValue("B")
					)
				)
			stmt("CREATE LENS test2 AS SELECT * FROM R WITH TYPE_INFERENCE()") must be equalTo new CreateLens("test2", 
					stmt("SELECT * FROM R").asInstanceOf[net.sf.jsqlparser.statement.select.Select].getSelectBody(),
					"TYPE_INFERENCE",
					List[net.sf.jsqlparser.expression.Expression]()
				)

		}

	}
}