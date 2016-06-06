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
	/* method name changed from query to convert 6/3/16 */
	def convert(s: String) =
		db.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def parser = new ExpressionParser(db.lenses.modelForLens)
	def expr = parser.expr _

	val tempDB:String = "tempDB"
	val testData = List[ (String, File, List[String]) ](
			(	"R", new File("../test/r_test/r.csv"), 
				List("A int", "B int", "C int")
			),
			("S", new File("../test/r_test/s.csv"),
				List("B int", "D int")
			),
			("T", new File("../test/r_test/t.csv"),
				List("D int", "E int")
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

		"Parse trivial aggregate queries" in {
			db.optimize(convert("SELECT SUM(A) FROM R"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1")),
					List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList,
						List()
					))

			db.optimize(convert("SELECT AVG(A) FROM R"))must be equalTo
				Aggregate(List(AggregateArg("AVG", List(Var("R_A")), "EXPR_1")),
					List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()))

			db.optimize(convert("SELECT MAX(A) FROM R"))must be equalTo
				Aggregate(List(AggregateArg("MAX", List(Var("R_A")), "EXPR_1")),
					List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()))

			db.optimize(convert("SELECT MIN(A) FROM R"))must be equalTo
				Aggregate(List(AggregateArg("MIN", List(Var("R_A")), "EXPR_1")),
					List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()))

			db.optimize(convert("SELECT COUNT(*) FROM R"))must be equalTo
				Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")),
					List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()))

			db.optimize(convert("SELECT COUNT(*) FROM R, S"))must be equalTo
				Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")),
					List(), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))

			db.optimize(convert("SELECT COUNT(*) FROM R, S WHERE R.B = S.B"))must be equalTo
				Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")),
					List(), Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT SUM(A) FROM R, S WHERE R.B = S.B"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1")),
					List(), Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT SUM(A), AVG(D) FROM R, S WHERE R.B = S.B"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1"),
					AggregateArg("AVG", List(Var("S_D")), "EXPR_2")),
					List(), Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT SUM(A + B), AVG(D + B) FROM R, S WHERE R.B = S.B"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Add, Var("R_A"), Var("S_B"))), "EXPR_1"),
					AggregateArg("AVG", List(Arithmetic(Arith.Add, Var("S_D"), Var("S_B"))), "EXPR_2")),
					List(), Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT SUM(A * D) FROM R, S WHERE R.B = S.B"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("S_D"))), "EXPR_1")),
					List(), Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")), Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
						Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT SUM(A * E) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D)"))must be equalTo
				Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("T_E"))), "EXPR_1")),
					List(), Select(Comparison(Cmp.Eq, Var("S_D"), Var("T_D")),
										Join(Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
											Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
											Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))),
											Table("T", Map(("T_D", Type.TInt), ("T_E", Type.TInt)).toList, List()))))
		}

		"Create and query lenses" in {
		 	db.createLens(stmt(
		 		"CREATE LENS SaneR AS SELECT * FROM R WITH MISSING_VALUE('B')"
		 	).asInstanceOf[CreateLens]);
		 	db.optimize(
		 		convert("SELECT * FROM SaneR")
		 	) must be equalTo 
		 		Project(List(ProjectArg("A", Var("R_A")), 
		 					 ProjectArg("B", expr(
		 					 	"IF R_B IS NULL THEN {{ SANER_1[ROWID_MIMIR] }} ELSE R_B END")),
		 					 ProjectArg("C", Var("R_C"))
		 				), Table("R", Map(("R_A", Type.TInt), 
		 								  ("R_B", Type.TInt), 
		 								  ("R_C", Type.TInt)).toList,
		 							  Map(("ROWID_MIMIR", Type.TRowId)).toList
				))

		 	;
			db.query(convert("SELECT * FROM SaneR")).allRows must be equalTo List(
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