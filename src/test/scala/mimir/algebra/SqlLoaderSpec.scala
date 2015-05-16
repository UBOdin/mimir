package mimir.ctables;

import java.io.{StringReader,BufferedReader,FileReader,File}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object SqlLoaderSpec extends Specification with FileMatchers {

	def stmt(s: String) = {
		new MimirJSqlParser(new StringReader(s)).Statement()
	}
	def query(s: String) = 
		db.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def parser = new ExpressionParser(db.lenses.modelForLens)
	def expr = parser.expr _

	val tempDB:File = null
	val testData = List[ (String, File, List[String]) ](
			(	"R", new File("test/r_test/r.csv"), 
				List("A int", "B int", "C int")
			)
		)

	val db = {
		if(tempDB != null){
			if(tempDB.exists()){ tempDB.delete(); }
		}
		// tempDB.deleteOnExit();
		var d = new Database(new JDBCBackend(Mimir.connectSqlite(
			if(tempDB == null){ "" } else { tempDB.toString() }
		)));
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
		 					 	"CASE WHEN R_B IS NULL THEN {{ SANER_0[ROWID] }} ELSE R_B END")),
		 					 ProjectArg("C", Var("R_C"))
		 				), Table("R", Map(("R_A", Type.TInt), 
		 								  ("R_B", Type.TInt), 
		 								  ("R_C", Type.TInt)), 
		 							  Map( ("ROWID", Type.TInt) 
		 				)))

		 	;
			db.query(query("SELECT * FROM SaneR")).allRows must be equalTo List(
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)
		}

	}
}