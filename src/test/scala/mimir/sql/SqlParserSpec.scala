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
import mimir.load._
import mimir.util._
import mimir.test._
import mimir.ctables._

object SqlParserSpec 
	extends Specification 
	with FileMatchers 
	with SQLParsers
{

	def convert(s: String) =
		db.sql.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select])
	def expr = ExpressionParser.expr _

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
			),
			(	"R_REVERSED", new File("test/r_test/r.csv"), 
				List("C int", "B int", "A int")
			)
		)

	val db:Database = {
		try {
			if(tempDB != null){
				val dbFile = new File(tempDB)
				if(dbFile.exists()){ dbFile.delete(); }
				dbFile.deleteOnExit();
			}
			val j = new JDBCBackend("sqlite",
									if(tempDB == null){ "testdb" } else { tempDB.toString }
							)
			val d = new Database(j)
	    try {
		    d.backend.open();
				j.enableInlining(d)
				d.initializeDBForMimir();
			} catch {
				case e:Exception => e.printStackTrace()

			}
			testData.foreach ( _ match { case ( tableName, tableData, tableCols ) => 
				d.backend.update("CREATE TABLE "+tableName+"("+tableCols.mkString(", ")+");")
				LoadCSV(d, tableName, tableData, Map("HEADER" -> "NO"))
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

		"Handle IN queries" in {
			db.backend.resultRows("SELECT B FROM R WHERE A IN (2,3,4)").toList must not contain(Seq(IntPrimitive(3)))
		}

		"Handle CAST operations" in {
			val cast1:(String=>Type) = (tstring: String) =>
				db.typechecker.schemaOf(convert(s"SELECT CAST('FOO' AS $tstring) FROM R"))(0)._2
			val cast2:(String=>Type) = (tstring: String) =>
				db.typechecker.schemaOf(convert(s"SELECT CAST('FOO', '$tstring') FROM R"))(0)._2

			cast1("int") must be equalTo TInt()
			cast1("double") must be equalTo TFloat()
			cast1("string") must be equalTo TString()
			cast1("date") must be equalTo TDate()
			cast1("timestamp") must be equalTo TTimestamp()
			cast1("flibble") must throwA[RAException]

			cast2("int") must be equalTo TInt()
			cast2("double") must be equalTo TFloat()
			cast2("string") must be equalTo TString()
			cast2("date") must be equalTo TDate()
			cast2("timestamp") must be equalTo TTimestamp()
			cast2("flibble") must throwA[RAException]
		}

		"Parse trivial aggregate queries" in {
			db.compiler.optimize(convert("SELECT SUM(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("A")), "SUM")),
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
				))

			db.compiler.optimize(convert("SELECT SUM(A) AS TIM FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("A")), "TIM")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					))

			db.typechecker.schemaOf(convert("SELECT SUM(*) AS TIM FROM R")) must throwA[RAException]

			db.compiler.optimize(convert("SELECT AVG(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("AVG", false, List(Var("A")), "AVG")),
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()))

			db.compiler.optimize(convert("SELECT MAX(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("MAX", false, List(Var("A")), "MAX")),	
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()))

			db.compiler.optimize(convert("SELECT MIN(A) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("MIN", false, List(Var("A")), "MIN")),
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()))

			db.compiler.optimize(convert("SELECT COUNT(*) FROM R")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT")),	
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()))

			db.compiler.optimize(convert("SELECT SUM(A), SUM(B) FROM R")) must be equalTo
				Aggregate(List(), List(
						AggFunction("SUM", false, List(Var("A")), "SUM_1"),
						AggFunction("SUM", false, List(Var("B")), "SUM_2")
					),	
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()))


			db.compiler.optimize(convert("SELECT COUNT(*) FROM R, S")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Join(Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
						Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())))

			db.compiler.optimize(convert("SELECT COUNT(*) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT SUM(A) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Var("A")), "SUM")), 
			 		Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
			 			Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT SUM(A), AVG(D) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(
					AggFunction("SUM", false, List(Var("A")), "SUM"),
					AggFunction("AVG", false, List(Var("D")), "AVG")),
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT SUM(A + B), AVG(D + B) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(
					AggFunction("SUM", false, List(Arithmetic(Arith.Add, Var("A"), Var("B"))), "SUM"),
					AggFunction("AVG", false, List(Arithmetic(Arith.Add, Var("D"), Var("B"))), "AVG")),	
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT SUM(A * D) FROM R, S WHERE R.B = S.B")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("A"), Var("D"))), "SUM")),	
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT SUM(A * E) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D)")) must be equalTo
				Aggregate(List(), List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("A"), Var("E"))), "SUM")), 
					Select(Comparison(Cmp.Eq, Var("D"), Var("D_0")),
						Join(
							Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
								Join(
									Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
									Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())
								)
							),
							Table("T","T", Map(("D_0", TInt()), ("E", TInt())).toList, List()))))

		}


		"Parse simple aggregate-group by queries" in {
			db.compiler.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("B")), "SUM")),
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
				))

/* Illegal Group By Queries */
			db.compiler.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY A, C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT A, B, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT A, SUM(B), * FROM R GROUP BY C")) must throwA[SQLException]

/* Illegal All Columns/All Table Columns queries */
			db.compiler.optimize(convert("SELECT SUM(B), * FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT *, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT *, SUM(B) AS GEORGIE FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT R.*, SUM(B) FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT R.*, SUM(B) AS CHRISTIAN FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT SUM(B), R.* FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT SUM(B) AS FRAN, R.* FROM R GROUP BY C")) must throwA[SQLException]

			db.compiler.optimize(convert("SELECT 1 + SUM(B) AS FRAN, R.* FROM R GROUP BY C")) must throwA[SQLException]

/* Variant Test Cases */
			db.compiler.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("BOB", Var("A")), ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("B")), "MIMIR_AGG_ALICE")), 
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					)))

			db.compiler.optimize(convert("SELECT A, SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
					Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("B")), "ALICE")),
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					))

			db.compiler.optimize(convert("SELECT SUM(B) AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("B")), "MIMIR_AGG_ALICE")), 
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					)))

			db.compiler.optimize(convert("SELECT SUM(B), A AS ALICE FROM R GROUP BY A")) must be equalTo
				Project(List(ProjectArg("SUM", Var("MIMIR_AGG_SUM")), ProjectArg("ALICE", Var("A"))),
					Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("B")), "MIMIR_AGG_SUM")), 
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					)))

			db.compiler.optimize(convert("SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A, C")) must be equalTo
				Project(List(ProjectArg("BOB", Var("A")), ProjectArg("ALICE", Var("MIMIR_AGG_ALICE"))),
					Aggregate(List(Var("A"), Var("C")), List(AggFunction("SUM", false, List(Var("B")), "MIMIR_AGG_ALICE")),
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()
					)))

			(convert("SELECT * FROM (SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A)subq WHERE ALICE > 5")) must be equalTo
					Project(List(ProjectArg("BOB", Var("SUBQ_BOB")), ProjectArg("ALICE", Var("SUBQ_ALICE"))),
						Select(Comparison(Cmp.Gt, Var("SUBQ_ALICE"), IntPrimitive(5)),
							Project(List(ProjectArg("SUBQ_BOB", Var("R_A")), ProjectArg("SUBQ_ALICE", Var("MIMIR_AGG_SUBQ_ALICE"))),
								Aggregate(List(Var("R_A")), List(AggFunction("SUM", false, List(Var("R_B")), "MIMIR_AGG_SUBQ_ALICE")), 
									Project(Seq(ProjectArg("R_A", Var("A")), ProjectArg("R_B", Var("B")), ProjectArg("R_C", Var("C"))), 
										Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
										)))))


			/* END: Variant Test Cases */
			db.compiler.optimize(convert("SELECT A, AVG(B) FROM R GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("AVG", false, List(Var("B")), "AVG")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)

			db.compiler.optimize(convert("SELECT A, MIN(B) FROM R GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("MIN", false, List(Var("B")), "MIN")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)

			db.compiler.optimize(convert("SELECT A, MAX(B) FROM R GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("MAX", false, List(Var("B")), "MAX")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)

			db.compiler.optimize(convert("SELECT A, COUNT(*) FROM R GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)

			db.compiler.optimize(convert("SELECT A, B, COUNT(*) FROM R GROUP BY A,B")) must be equalTo
				Aggregate(List(Var("A"), Var("B")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)

			db.compiler.optimize(convert("SELECT A, COUNT(*) FROM R, S GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Join(
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
						Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())))

			db.compiler.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S GROUP BY A, R.B")) must be equalTo
				Aggregate(List(Var("A"), Var("B")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Join(
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
						Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())))

			db.compiler.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S GROUP BY A, R.B, C")) must be equalTo
				Aggregate(List(Var("A"), Var("B"), Var("C")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Join(
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
						Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())))

			db.compiler.optimize(convert("SELECT A, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
					Join(
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
						Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT A, R.B, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B")) must be equalTo
				Aggregate(List(Var("A"), Var("B")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT A, R.B, C, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C")) must be equalTo
				Aggregate(List(Var("A"), Var("B"), Var("C")), List(AggFunction("COUNT", false, List(), "COUNT")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT A, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
				Aggregate(List(Var("A")), List(AggFunction("SUM", false, List(Var("C")), "SUM")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))


			db.compiler.optimize(convert("SELECT A, R.B, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B")) must be equalTo
				Aggregate(List(Var("A"), Var("B")), List(AggFunction("SUM", false, List(Var("C")), "SUM")), 
					Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
						Join(
							Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
							Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C")) must be equalTo
				Project(List(ProjectArg("B", Var("B")), ProjectArg("SUM", Var("MIMIR_AGG_SUM"))),
					Aggregate(
						List(Var("A"), Var("B"), Var("C")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("A"), Var("C"))), "MIMIR_AGG_SUM")),
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List())))))

			db.compiler.optimize(convert("SELECT R.B, SUM(A), AVG(C) FROM R, S WHERE R.B = S.B GROUP BY R.B")) must be equalTo
					Aggregate(
						List(Var("B")),
						List(	AggFunction("SUM", false, List(Var("A")), "SUM"), 
									AggFunction("AVG", false, List(Var("C")), "AVG")),
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT A, SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B GROUP BY A")) must be equalTo
					Aggregate(List(Var("A")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Add, Var("A"), Var("B"))), "SUM"),
								 AggFunction("AVG", false, List(Arithmetic(Arith.Add, Var("C"), Var("B"))), "AVG")), 
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT R.B, SUM(A*C) FROM R, S WHERE R.B = S.B GROUP BY R.B")) must be equalTo
					Aggregate(
						List(Var("B")),
						List(AggFunction("SUM", false, List(Arithmetic(Arith.Mult, Var("A"), Var("C"))), "SUM")),
						Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))))

			db.compiler.optimize(convert("SELECT A, SUM(D) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D) GROUP BY A")) must be equalTo
				Aggregate(
					List(Var("A")),
					List(AggFunction("SUM", false, List(Var("D")), "SUM")),
					Select(Comparison(Cmp.Eq, Var("D"), Var("D_0")),
						Join(Select(Comparison(Cmp.Eq, Var("B"), Var("B_0")),
							Join(
								Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List()),
								Table("S","S", Map(("B_0", TInt()), ("D", TInt())).toList, List()))),
							Table("T","T", Map(("D_0", TInt()), ("E", TInt())).toList, List()))))

		}

		"Get the types right in aggregates" >> {
			db.backend.update(stmts("test/data/Product_Inventory.sql").map(_.toString))
			
			val q = db.compiler.optimize(db.sql.convert(selectStmt("""
				SELECT COMPANY, SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
			""")))
			q must be equalTo(
				Aggregate(
					List(Var("COMPANY")), 
					List(AggFunction("SUM", false, List(Var("QUANTITY")), "SUM")),
					Table("PRODUCT_INVENTORY","PRODUCT_INVENTORY", List( 
						("ID", TString()), 
						("COMPANY", TString()), 
						("QUANTITY", TInt()), 
						("PRICE", TFloat()) 
					), List())
				)
			)
			db.typechecker.schemaOf(q) must contain(eachOf[(String,Type)]( 
				("COMPANY",TString()), 
				("SUM", TInt()) 
			))

			LoggerUtils.debug(
				// "mimir.sql.RAToSql",
				// "mimir.exec.Compiler"
			){
				db.query(q){ _.toSeq must not beEmpty }
			} 
		}

		"Support DISTINCT Aggregates" >> {
			db.compiler.optimize(convert("SELECT COUNT(DISTINCT A) AS SHAZBOT FROM R")) must be equalTo
					Aggregate(
						List(),
						List(AggFunction("COUNT", true, List(Var("A")), "SHAZBOT")),
						Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
					)
		}

		"Support Aggregates with Selections" >> {
			val q = db.compiler.optimize(db.sql.convert(selectStmt("""
				SELECT COUNT(DISTINCT COMPANY) AS SHAZBOT
				FROM PRODUCT_INVENTORY
				WHERE COMPANY = 'Apple';
			""")))
			q must be equalTo(
				Aggregate(
					List(),
					// This one is a bit odd... technically the rewrite is correct!
					// I'm not 100% sure that this is valid SQL though.
					List(AggFunction("COUNT", true, List(StringPrimitive("Apple")), "SHAZBOT")),
					// List(AggFunction("COUNT", true, List(Var("COMPANY")), "SHAZBOT")),
					Select(
						Comparison(Cmp.Eq, Var("COMPANY"), StringPrimitive("Apple")),
						Table("PRODUCT_INVENTORY","PRODUCT_INVENTORY", List( 
								("ID", TString()), 
								("COMPANY", TString()), 
								("QUANTITY", TInt()), 
								("PRICE", TFloat()) 
							), List()
					))
				))
		}

		"Respect column ordering of base relations" >> {
			convert("SELECT * FROM R").columnNames must be equalTo(
				Seq("A", "B", "C")
			)
			convert("SELECT * FROM R_REVERSED").columnNames must be equalTo(
				Seq("C", "B", "A")
			)
		}

		"Create and query lenses" >> {
		 	db.update(stmt(
		 		"CREATE LENS SaneR AS SELECT * FROM R WITH MISSING_VALUE('B')"
		 	).asInstanceOf[CreateLens]);
		 	db.getAllTables() must contain("SANER")
		 	db.compiler.optimize(
		 		convert("SELECT * FROM SaneR")
		 	) must be equalTo 
		 		View("SANER", 
			 		Project(List(ProjectArg("A", Var("A")), 
			 					 ProjectArg("B", 
			 					 	 Conditional(IsNullExpression(Var("B")),
			 					 	 	 Conditional(
			 					 	 	 	Comparison(Cmp.Eq,
				 					 	 	 	VGTerm("SANER:META:B", 0, Seq(), Seq()),
			 					 	 	 		StringPrimitive("SPARKML")
			 					 	 	 	),
			 					 	 	 	VGTerm("SANER:SPARKML:B", 0, Seq(RowIdVar()), Seq(Var("A"), Var("B"), Var("C"))),
			 					 	 	  NullPrimitive()
			 					 	 	 ),
			 					 	 	 Var("B")
		 					 	 	 )),
			 					 ProjectArg("C", Var("C"))
			 				), Table("R","R", Map(("A", TInt()), 
			 								  ("B", TInt()), 
			 								  ("C", TInt())).toList,
			 							  List()
					)),
					Set[mimir.views.ViewAnnotation.T]()
				)

			// val guessCacheData = 
			//  	db.backend.resultRows("SELECT "+
			//  		db.bestGuessCache.keyColumn(0)+","+
			//  		db.bestGuessCache.dataColumn+" FROM "+
			//  		db.bestGuessCache.cacheTableForModel(
			//  			db.models.get("SANER:WEKA:B"), 0)
			//  	)
			// guessCacheData must contain( ===(Seq[PrimitiveValue](IntPrimitive(3), IntPrimitive(2))) )
		 	
			db.query(convert("SELECT * FROM SaneR")){ _.map { row =>
				(row("A").asInt, row("B").asInt, row("C"))
			}.toSeq must contain(
				(1, 2, IntPrimitive(3)),
				(1, 3, IntPrimitive(1)),
				(2, 2, IntPrimitive(1)),
				(1, 2, NullPrimitive()),
				(1, 4, IntPrimitive(2)),
				(2, 2, IntPrimitive(1)),
				(4, 2, IntPrimitive(4))
			) }
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

		"Support multi-clause CASE statements" in {
			db.compiler.optimize(convert("""
				SELECT CASE WHEN R.A = 1 THEN 'A' WHEN R.A = 2 THEN 'B' ELSE 'C' END AS Q FROM R
			""")) must be equalTo
				Project(List(ProjectArg("Q", 
						Conditional(expr("A = 1"), StringPrimitive("A"),
							Conditional(expr("A = 2"), StringPrimitive("B"), StringPrimitive("C")
						)))),
					Table("R","R", Map(("A", TInt()), ("B", TInt()), ("C", TInt())).toList, List())
				)
			
		}

	}
}
