package mimir.sql;

import java.io.{StringReader,BufferedReader,FileReader,File}
import java.sql.SQLException
import scala.collection.JavaConversions._

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import com.typesafe.scalalogging.slf4j.Logger

import net.sf.jsqlparser.statement.select.{PlainSelect}

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.util._
import mimir.test._

object SqlParserSpec 
	extends Specification 
	with FileMatchers 
	with SQLParsers
{

	def convert(s: String) =
		db.sql.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def parser = new ExpressionParser(db.models.getModel)
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
				val dbFile = new File(tempDB)
				if(dbFile.exists()){ dbFile.delete(); }
				dbFile.deleteOnExit();
			}
			val d = new Database(new JDBCBackend("sqlite",
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
				LoadCSV.handleLoadTable(d, tableName, tableData, false)
			})
			d
		} catch {
			case e : Throwable => System.err.println(e.getMessage()); throw e;
		}
	}

	sequential

	"The Sql Parser" should {
		"Handle trivial queries" in {
			db.backend.resultRows("SELECT * FROM R;").toList must be equalTo List( 
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),NullPrimitive(),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)

			db.backend.resultRows("SELECT A FROM R;").toList must be equalTo List(
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
			db.optimize(convert("SELECT SUM(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("R_A")), "SUM_1")),
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
				))

			db.optimize(convert("SELECT SUM(A) AS TIM FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("R_A")), "TIM")), 
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					))

			db.optimize(convert("SELECT SUM(*) AS TIM FROM R")) must throwA[RAException]

			db.optimize(convert("SELECT AVG(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("AVG", false, List(Var("R_A")), "AVG_1")),
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()))

			db.optimize(convert("SELECT MAX(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("MAX", false, List(Var("R_A")), "MAX_1")),	
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()))

			db.optimize(convert("SELECT MIN(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("MIN", false, List(Var("R_A")), "MIN_1")),
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()))

			db.optimize(convert("SELECT COUNT(*) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT_1")),	
					Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()))

			db.optimize(convert("SELECT COUNT(*) FROM R, S")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT_1")), 
					Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
						Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))

			db.optimize(convert("SELECT COUNT(*) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT_1")), 
					Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
						Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT SUM(A) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("R_A")), "SUM_1")), 
			 		Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
			 			Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT SUM(A), AVG(D) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(
					AggFunction("SUM", false, List(Var("R_A")), "SUM_1"),
					AggFunction("AVG", false, List(Var("S_D")), "AVG_2")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT SUM(A + B), AVG(D + B) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(
					AggFunction("SUM", false, List(Arithmetic(Arith.Add, Var("R_A"), Var("S_B"))), "SUM_1"),
					AggFunction("AVG", false, List(Arithmetic(Arith.Add, Var("S_D"), Var("S_B"))), "AVG_2")),	
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT SUM(A * D) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("R_A"), Var("S_D"))), "SUM_1")),	
					Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
						Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT SUM(A * E) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D)")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("R_A"), Var("T_E"))), "SUM_1")), 
					Select(Comparison(Cmp.Eq, Var("S_D"), Var("T_D")),
						Join(Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))),
									Table("T", Map(("T_D", TInt()), ("T_E", TInt())).toList, List()))))

		}


		"Parse simple aggregate-group by queries" in {
			db.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_SUM_2")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

/* Illegal Group By Queries */
			db.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY A, C")) must throwA[SQLException]

			db.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT A, SUM(B), * FROM R GROUP BY C")) must throwA[SQLException]

/* Illegal All Columns/All Table Columns queries */
			db.optimize(convert("SELECT SUM(B), * FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT *, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT *, SUM(B) AS GEORGIE FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT R.*, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT R.*, SUM(B) AS CHRISTIAN FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT SUM(B), R.* FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT SUM(B) AS FRAN, R.* FROM R GROUP BY C")) must throwA[SQLException]

			db.optimize(convert("SELECT 1 + SUM(B) AS FRAN, R.* FROM R GROUP BY C")) must throwA[SQLException]

/* Variant Test Cases */
			db.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("BOB", Var("R_A")), ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_ALICE")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
						)))

			db.optimize(convert("SELECT A, SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_ALICE")),
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
						)))

			db.optimize(convert("SELECT SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_ALICE")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
						)))

			db.optimize(convert("SELECT SUM(B), A AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("SUM_1", Var("MIMIR_AGG_SUM_1")), ProjectArg("ALICE", Var("R_A"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_SUM_1")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
						)))

			db.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A, C")) must be equalTo
				Project(List(ProjectArg("BOB", Var("R_A")), ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("R_A"), Var("R_C")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_ALICE")),
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
						)))

			(convert("SELECT * FROM (SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A)subq WHERE ALICE > 5")) must be equalTo
					Project(List(ProjectArg("BOB", Var("SUBQ_BOB")), ProjectArg("ALICE", Var("SUBQ_ALICE"))),
						Select(Comparison(Cmp.Gt, Var("SUBQ_ALICE"), IntPrimitive(5)),
							Project(List(ProjectArg("SUBQ_BOB", Var("R_A")), ProjectArg("SUBQ_ALICE", Var("MIMIR_AGG_SUBQ_ALICE"))),
								Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_SUBQ_ALICE")), 
									Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List())
									))))


			/* END: Variant Test Cases */
			db.optimize(convert("SELECT A, AVG(B) FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("AVG_2", Var("MIMIR_AGG_AVG_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("AVG", false, List(Var("R_B")), "MIMIR_AGG_AVG_2")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

			db.optimize(convert("SELECT A, MIN(B) FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("MIN_2", Var("MIMIR_AGG_MIN_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("MIN", false, List(Var("R_B")), "MIMIR_AGG_MIN_2")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

			db.optimize(convert("SELECT A, MAX(B) FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("MAX_2", Var("MIMIR_AGG_MAX_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("MAX", false, List(Var("R_B")), "MIMIR_AGG_MAX_2")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

			db.optimize(convert("SELECT A, COUNT(*) FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("COUNT_2", Var("MIMIR_AGG_COUNT_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_2")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

			db.optimize(convert("SELECT A, B, COUNT(*) FROM R GROUP BY A,B")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("COUNT_3", Var("MIMIR_AGG_COUNT_3"))),
					Aggregate(List(Var("R_A"), Var("R_B")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_3")), 
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()
					)))

			db.optimize(convert("SELECT A, COUNT(*) FROM R, S GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("COUNT_2", Var("MIMIR_AGG_COUNT_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_2")), 
						Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S GROUP BY A, R.B")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("COUNT_3", Var("MIMIR_AGG_COUNT_3"))),
					Aggregate(List(Var("R_A"), Var("R_B")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_3")), 
						Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S GROUP BY A, R.B, C")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("C", Var("R_C")),
				ProjectArg("COUNT_4", Var("MIMIR_AGG_COUNT_4"))),
					Aggregate(List(Var("R_A"), Var("R_B"), Var("R_C")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_4")), 
						Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
							Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))))

			db.optimize(convert("SELECT A, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("COUNT_2", Var("MIMIR_AGG_COUNT_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_2")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("COUNT_3", Var("MIMIR_AGG_COUNT_3"))),
					Aggregate(List(Var("R_A"), Var("R_B")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_3")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("C", Var("R_C")),
				ProjectArg("COUNT_4", Var("MIMIR_AGG_COUNT_4"))),
					Aggregate(List(Var("R_A"), Var("R_B"), Var("R_C")), List(AggFunction("COUNT", false, List(), "MIMIR_AGG_COUNT_4")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT A, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))),
					Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_C")), "MIMIR_AGG_SUM_2")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))


			db.optimize(convert("SELECT A, R.B, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("B", Var("R_B")), ProjectArg("SUM_3", Var("MIMIR_AGG_SUM_3"))),
					Aggregate(List(Var("R_A"), Var("R_B")), List(AggFunction("SUM", false, List(Var("R_C")), "MIMIR_AGG_SUM_3")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C")) must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))),
					Aggregate(
						List(Var("R_A"), Var("R_B"), Var("R_C")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("R_A"), Var("R_C"))), "MIMIR_AGG_SUM_2")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A), AVG(C) FROM R, S WHERE R.B = S.B GROUP BY R.B")) must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2")), ProjectArg("AVG_3", Var("MIMIR_AGG_AVG_3"))),
					Aggregate(
						List(Var("R_B")),
						List(	AggFunction("SUM", false, List(Var("R_A")), "MIMIR_AGG_SUM_2"), 
									AggFunction("AVG", false, List(Var("R_C")), "MIMIR_AGG_AVG_3")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT A, SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2")), ProjectArg("AVG_3", Var("MIMIR_AGG_AVG_3"))),
					Aggregate(List(Var("R_A")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Add, Var("R_A"), Var("S_B"))), "MIMIR_AGG_SUM_2"),
								AggFunction("AVG", false, List(Arithmetic(Arith.Add, Var("R_C"), Var("S_B"))), "MIMIR_AGG_AVG_3")), 
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT R.B, SUM(A*C) FROM R, S WHERE R.B = S.B GROUP BY R.B")) must be equalTo
				Project(List(ProjectArg("B", Var("R_B")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))),
					Aggregate(
						List(Var("R_B")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("R_A"), Var("R_C"))), "MIMIR_AGG_SUM_2")),
						Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
							Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
								Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List())))))

			db.optimize(convert("SELECT A, SUM(D) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D) GROUP BY A")) must be equalTo
				Project(List(ProjectArg("A", Var("R_A")), ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))),
					Aggregate(
						List(Var("R_A")),
						List(AggFunction("SUM", false, List(Var("T_D")), "MIMIR_AGG_SUM_2")),
						Select(Comparison(Cmp.Eq, Var("S_D"), Var("T_D")),
							Join(Select(Comparison(Cmp.Eq, Var("R_B"), Var("S_B")),
								Join(Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List()),
									Table("S", Map(("S_B", TInt()), ("S_D", TInt())).toList, List()))),
									Table("T", Map(("T_D", TInt()), ("T_E", TInt())).toList, List())))))

		}

		"Get the types right in aggregates" >> {
			db.backend.update(stmts("test/data/Product_Inventory.sql").map(_.toString))
			
			val q = db.optimize(db.sql.convert(selectStmt("""
				SELECT COMPANY, SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
			""")))
			q must be equalTo(
				Project(
					List(
						ProjectArg("COMPANY", Var("PRODUCT_INVENTORY_COMPANY")),
						ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))
					),
					Aggregate(
						List(Var("PRODUCT_INVENTORY_COMPANY")), 
						List(AggFunction("SUM", false, List(Var("PRODUCT_INVENTORY_QUANTITY")), "MIMIR_AGG_SUM_2")),
						Table("PRODUCT_INVENTORY", List( 
								("PRODUCT_INVENTORY_ID", TString()), 
								("PRODUCT_INVENTORY_COMPANY", TString()), 
								("PRODUCT_INVENTORY_QUANTITY", TInt()), 
								("PRODUCT_INVENTORY_PRICE", TFloat()) 
							), List())
				))
			)
			q.schema must contain(eachOf[(String,Type)]( ("COMPANY",TString()), ("SUM_2", TInt()) ))

			LoggerUtils.debug(List(
				// "mimir.sql.RAToSql",
				// "mimir.exec.Compiler"
			), () => {
				db.query(q).allRows
			}) must not beEmpty
		}

		"Support DISTINCT Aggregates" >> {
			db.optimize(convert("SELECT COUNT(DISTINCT A) AS SHAZBOT FROM R")) must be equalTo
					Aggregate(
						List(),
						List(AggFunction("COUNT", true, List(Var("R_A")), "SHAZBOT")),
						Table("R", Map(("R_A", TInt()), ("R_B", TInt()), ("R_C", TInt())).toList, List())
					)
		}

		"Support Aggregates with Selections" >> {
			val q = db.optimize(db.sql.convert(selectStmt("""
				SELECT COUNT(DISTINCT COMPANY) AS SHAZBOT
				FROM PRODUCT_INVENTORY
				WHERE COMPANY = 'Apple';
			""")))
			q must be equalTo(
				Aggregate(
					List(),
					List(AggFunction("COUNT", true, List(Var("PRODUCT_INVENTORY_COMPANY")), "SHAZBOT")),
					Select(
						Comparison(Cmp.Eq, Var("PRODUCT_INVENTORY_COMPANY"), StringPrimitive("Apple")),
						Table("PRODUCT_INVENTORY", List( 
								("PRODUCT_INVENTORY_ID", TString()), 
								("PRODUCT_INVENTORY_COMPANY", TString()), 
								("PRODUCT_INVENTORY_QUANTITY", TInt()), 
								("PRODUCT_INVENTORY_PRICE", TFloat()) 
							), List()
					))
				))
		}

		"Create and query lenses" >> {
		 	db.update(stmt(
		 		"CREATE LENS SaneR AS SELECT * FROM R WITH MISSING_VALUE('B')"
		 	).asInstanceOf[CreateLens]);
		 	db.getAllTables() must contain("SANER")
		 	db.optimize(
		 		convert("SELECT * FROM SaneR")
		 	) must be equalTo 
		 		Project(List(ProjectArg("A", Var("SANER_A")), 
		 					 ProjectArg("B", Var("SANER_B")),
		 					 ProjectArg("C", Var("SANER_C"))
		 				), Table("SANER", Map(("SANER_A", TInt()), 
		 								  ("SANER_B", TInt()), 
		 								  ("SANER_C", TInt())).toList,
		 							  List()
				))
			val guessCacheData = 
			 	db.backend.resultRows("SELECT "+
			 		db.bestGuessCache.keyColumn(0)+","+
			 		db.bestGuessCache.dataColumn+" FROM "+
			 		db.bestGuessCache.cacheTableForModel(
			 			db.models.getModel("SANER:WEKA:B"), 0)
			 	)
			guessCacheData must contain( ===(Seq[PrimitiveValue](IntPrimitive(3), IntPrimitive(3))) )
		 	
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