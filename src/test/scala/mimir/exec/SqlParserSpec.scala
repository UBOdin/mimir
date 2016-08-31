package mimir.exec;

import java.io.{StringReader,BufferedReader,FileReader,File}
import java.sql.SQLException
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
	def convert(s: String) =
		db.sql.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def parser = new ExpressionParser(db.lenses.modelForLens)
	def expr = parser.expr _

	val tempDB:String = "tempDB"
	val testData = List[ (String, File, List[String]) ](
			(	"R", new File("test/r_test/r.csv"), 
				List("A int", "B int", "C int")
			),
			("S", new File("test/r_test/s.csv"),
				List("B int", "D int")
			),
			("T", new File("test/r_test/t.csv"),
				List("D int", "E int")
			)
		)

	val db:Database = {
		try {
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
				d.backend.update("CREATE TABLE "+tableName+"("+tableCols.mkString(", ")+");")
				val lines = new BufferedReader(new FileReader(tableData))
				var line: String = lines.readLine()
				while(line != null){
					d.backend.update("INSERT INTO "+tableName+" VALUES (" + line + ");")
					line = lines.readLine()
				}
			})
			d
		} catch {
			case e : Throwable => System.err.println(e.getMessage()); throw e;
		}
	}

	sequential

	"The Sql Parser" should {
		"Handle trivial queries" in {
			db.backend.resultRows("SELECT * FROM R;") must be equalTo List( 
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),NullPrimitive(),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)

			db.backend.resultRows("SELECT A FROM R;") must be equalTo List(
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
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1")), List(),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT SUM(A) AS TIM FROM R"))must be equalTo
				Project(List(ProjectArg("TIM", Var("TIM"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "TIM")), List(),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))
			/* Couldn't find away to avoid the \s*.*\s* which was being added to the front and back of the test case exception string */
			db.optimize(convert("SELECT SUM(*) AS TIM FROM R"))must throwA[SQLException]
				//throwA[SQLException]("Syntax error in query expression: SUM(*)")
				//throwA("Syntax error in query expression: SUM(*)")
				//throwA[SQLException]("Syntax error in query expression: SUM(*)")


			db.optimize(convert("SELECT AVG(A) FROM R"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("AVG", List(Var("R_A")), "EXPR_1")),List(),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List())))

			db.optimize(convert("SELECT MAX(A) FROM R"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("MAX", List(Var("R_A")), "EXPR_1")),	List(), Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List())))

			db.optimize(convert("SELECT MIN(A) FROM R"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("MIN", List(Var("R_A")), "EXPR_1")),	List(),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List())))

			db.optimize(convert("SELECT COUNT(*) FROM R"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")),	List(),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List())))

			db.optimize(convert("SELECT COUNT(*) FROM R, S"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(),
						Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
							Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT COUNT(*) FROM R, S WHERE R.B = S.B"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT SUM(A) FROM R, S WHERE R.B = S.B"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1")), List(),
				 		Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
				 			Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT SUM(A), AVG(D) FROM R, S WHERE R.B = S.B"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1")), ProjectArg("EXPR_2", Var("EXPR_2"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1"),
					AggregateArg("AVG", List(Var("S_D")), "EXPR_2")),	List(),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT SUM(A + B), AVG(D + B) FROM R, S WHERE R.B = S.B"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1")), ProjectArg("EXPR_2", Var("EXPR_2"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Add, Var("R_A"), Var("S_B"))), "EXPR_1"),
						AggregateArg("AVG", List(Arithmetic(Arith.Add, Var("S_D"), Var("S_B"))), "EXPR_2")),	List(),
							Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
								Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
									Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT SUM(A * D) FROM R, S WHERE R.B = S.B"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("S_D"))), "EXPR_1")),	List(),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT SUM(A * E) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D)"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("T_E"))), "EXPR_1")), List(),
						Select(Comparison(Cmp.Eq, Var("S_D"), Var("T_D")),
							Join(Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
								Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
									Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))),
										Table("T", Map(("T_D", Type.TInt), ("T_E", Type.TInt)).toList, List())))))

		}


		"Parse simple aggregate-group by queries" in {
			db.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

/* Illegal Group By Queries */
			db.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY C"))must
				throwA[SQLException]("Illegal Group By Query: 'A' is not a Group By argument.")

			db.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY A, C"))must
				throwA[SQLException]("Illegal Group By Query: 'B' is not a Group By argument.")

			db.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY C"))must
				throwA[SQLException]("Illegal Group By Query: 'A' is not a Group By argument.")

			db.optimize(convert("SELECT A, SUM(B), * FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal Group By Query: 'A' is not a Group By argument.")

/* Illegal All Columns/All Table Columns queries */
			db.optimize(convert("SELECT SUM(B), * FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Columns' [*] in Aggregate Query.")

			db.optimize(convert("SELECT *, SUM(B) FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Columns' [*] in Aggregate Query.")

			db.optimize(convert("SELECT *, SUM(B) AS GEORGIE FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Columns' [*] in Aggregate Query.")

			db.optimize(convert("SELECT R.*, SUM(B) FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Table Columns' [R.*] in Aggregate Query.")

			db.optimize(convert("SELECT R.*, SUM(B) AS CHRISTIAN FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Table Columns' [R.*] in Aggregate Query.")

			db.optimize(convert("SELECT SUM(B), R.* FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Table Columns' [R.*] in Aggregate Query.")

			db.optimize(convert("SELECT SUM(B) AS FRAN, R.* FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal use of 'All Table Columns' [R.*] in Aggregate Query.")

			db.optimize(convert("SELECT 1 + SUM(B) AS FRAN, R.* FROM R GROUP BY C"))must throwA[SQLException]
				throwA("Illegal Aggregate query in the from of SELECT INT + AGG_FUNCTION.")

/* Variant Test Cases */
			db.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("BOB", Var("R_A")), ProjectArg("ALICE", Var("ALICE"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "ALICE")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))

			db.optimize(convert("SELECT A, SUM(B) AS ALICE FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("ALICE", Var("ALICE"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "ALICE")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))

			db.optimize(convert("SELECT SUM(B) AS ALICE FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("ALICE", Var("ALICE"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "ALICE")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))

			db.optimize(convert("SELECT SUM(B), A AS ALICE FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("EXPR_1", Var("EXPR_1")), ProjectArg("ALICE", Var("R_A"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))

			db.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A, C"))must be equalTo
				Project(List(ProjectArg("BOB", Var("R_A")), ProjectArg("ALICE", Var("ALICE"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "ALICE")), List(Var("R_A"), Var("R_C")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
						)))

			(convert("SELECT * FROM (SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A)subq WHERE ALICE > 5")) must be equalTo
					Project(List(ProjectArg("BOB", Var("SUBQ_BOB")), ProjectArg("ALICE", Var("SUBQ_ALICE"))),
						Select(Comparison(Cmp.Gt, Var("SUBQ_ALICE"), IntPrimitive(5)),
							Project(List(ProjectArg("SUBQ_BOB", Var("R_A")), ProjectArg("SUBQ_ALICE", Var("SUBQ_ALICE"))),
								Aggregate(List(AggregateArg("SUM", List(Var("R_B")), "SUBQ_ALICE")), List(Var("R_A")),
									Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List())
									))))


			/* END: Variant Test Cases */
			db.optimize(convert("SELECT A, AVG(B) FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("AVG", List(Var("R_B")), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT A, MIN(B) FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("MIN", List(Var("R_B")), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT A, MAX(B) FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("MAX", List(Var("R_B")), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT A, COUNT(*) FROM R GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT A, B, COUNT(*) FROM R GROUP BY A,B"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A"), Var("R_B")),
						Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()
					)))

			db.optimize(convert("SELECT A, COUNT(*) FROM R, S GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A")),
						Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
							Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S GROUP BY A, R.B"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A"), Var("R_B")),
						Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
							Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S GROUP BY A, R.B, C"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("C", Var("R_C")),
				ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A"), Var("R_B"), Var("R_C")),
						Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
							Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))))

			db.optimize(convert("SELECT A, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A"), Var("R_B")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("C", Var("R_C")),
				ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("COUNT", List(), "EXPR_1")), List(Var("R_A"), Var("R_B"), Var("R_C")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT A, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_C")), "EXPR_1")), List(Var("R_A")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))


			db.optimize(convert("SELECT A, R.B, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_C")), "EXPR_1")), List(Var("R_A"), Var("R_B")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C"))must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("R_C"))), "EXPR_1")),
					List(Var("R_A"), Var("R_B"), Var("R_C")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A), AVG(C) FROM R, S WHERE R.B = S.B GROUP BY R.B"))must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1")), ProjectArg("EXPR_2", Var("EXPR_2"))),
					Aggregate(List(AggregateArg("SUM", List(Var("R_A")), "EXPR_1"), AggregateArg("AVG", List(Var("R_C")), "EXPR_2")),
					List(Var("R_B")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT A, SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1")), ProjectArg("EXPR_2", Var("EXPR_2"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Add, Var("R_A"), Var("S_B"))), "EXPR_1"),
					AggregateArg("AVG", List(Arithmetic(Arith.Add, Var("R_C"), Var("S_B"))), "EXPR_2")), List(Var("R_A")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A*C) FROM R, S WHERE R.B = S.B GROUP BY R.B"))must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Arithmetic(Arith.Mult, Var("R_A"), Var("R_C"))), "EXPR_1")),
						List(Var("R_B")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
								Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List())))))

			db.optimize(convert("SELECT A, SUM(D) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D) GROUP BY A"))must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("EXPR_1", Var("EXPR_1"))),
					Aggregate(List(AggregateArg("SUM", List(Var("T_D")), "EXPR_1")),
					List(Var("R_A")),
						Select(Comparison(Cmp.Eq, Var("S_D"), Var("T_D")),
							Join(Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
								Join(Table("R", Map(("R_A", Type.TInt), ("R_B", Type.TInt), ("R_C", Type.TInt)).toList, List()),
									Table("S", Map(("S_B", Type.TInt), ("S_D", Type.TInt)).toList, List()))),
									Table("T", Map(("T_D", Type.TInt), ("T_E", Type.TInt)).toList, List())))))


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
		 					 	"IF R_B IS NULL THEN {{ SANER_1[ROWID] }} ELSE R_B END")),
		 					 ProjectArg("C", Var("R_C"))
		 				), Table("R", Map(("R_A", Type.TInt), 
		 								  ("R_B", Type.TInt), 
		 								  ("R_C", Type.TInt)).toList,
		 							  List()
				))
			val guessCacheData = 
			 	db.backend.resultRows("SELECT "+
			 		db.bestGuessCache.keyColumn(0)+","+
			 		db.bestGuessCache.dataColumn+" FROM "+
			 		db.bestGuessCache.cacheTableForLens("SANER", 1)
			 	)
			guessCacheData must contain( ===(List[PrimitiveValue](IntPrimitive(3), IntPrimitive(3))) )
		 	
			db.query(convert("SELECT * FROM SaneR")).allRows must be equalTo List(
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),IntPrimitive(3),IntPrimitive(1)),
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