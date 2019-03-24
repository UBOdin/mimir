package mimir.sql;

import java.io.{StringReader,BufferedReader,FileReader,File}
import java.sql.SQLException
import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import org.specs2.specification.core.{Fragment,Fragments}
import com.typesafe.scalalogging.slf4j.Logger

import sparsity.Name
import sparsity.parser.SQL

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.backend._
import mimir.util._
import mimir.test._
import mimir.ctables._
import mimir.ml.spark.SparkML
import mimir.algebra.spark.OperatorTranslation

object SqlParserSpec 
	extends Specification 
	with FileMatchers 
	with SQLParsers
{

	def convert(s: String) =
		db.sqlToRA(MimirSQL.Select(s))
	def expr = ExpressionParser.expr _

	val tempDB:String = "tempDB"
	val testData = Seq[ (String, String, Seq[(String, String)]) ](
			(	"R", "test/r_test/r.csv", 
				Seq(("A","int"), ("B", "int"), ("C", "int"))
			),
			("S", "test/r_test/s.csv",
				Seq(("B","int"), ("D","int"))
			),
			("T", "test/r_test/t.csv",
				Seq(("D","int"), ("E","int"))
			),
			(	"R_REVERSED", "test/r_test/r.csv",
				Seq(("C","int"), ("B","int"), ("A","int"))
			)
		)

	val db:Database = {
		try {
			if(tempDB != null){
				val dbFile = new File(tempDB)
				if(dbFile.exists()){ dbFile.delete(); }
				dbFile.deleteOnExit();
			}
			val j = new JDBCMetadataBackend("sqlite",
									if(tempDB == null){ "testdb" } else { tempDB.toString }
							)
			val sback = new SparkBackend(if(tempDB == null){ "testdb" } else { tempDB.toString.split("[\\\\/]").last.replaceAll("\\..*", "") })
			val d = new Database(sback, j)
	    try {
	      d.metadataBackend.open()
		    d.backend.open();
	      SparkML(sback.sparkSql)
        OperatorTranslation.db = d
				j.enableInlining(d)
				d.initializeDBForMimir();
			} catch {
				case e:Exception => e.printStackTrace()

			}
			testData.foreach ( _ match { case ( tableName, tableData, tableCols ) => 
				d.backend.dropTable(ID(tableName))
			  LoadCSV.handleLoadTableRaw(d, 
			  	targetTable = ID(tableName), 
			  	sourceFile = tableData,
			  	targetSchema = Some(tableCols.map(el => (ID(el._1), Type.fromString(el._2)))),
			  	options = Map()
			  )
			})
			d
		} catch {
			case e : Throwable => System.err.println(e.getMessage()); throw e;
		}
	}

	sequential

	"The Sql Parser" should {
		"Handle trivial queries" in {
			db.query(convert("SELECT * FROM R;"))(_.toList.map(_.tuple)) must be equalTo List( 
				List(IntPrimitive(1),IntPrimitive(2),IntPrimitive(3)),
				List(IntPrimitive(1),IntPrimitive(3),IntPrimitive(1)),
				List(IntPrimitive(2),NullPrimitive(),IntPrimitive(1)),
				List(IntPrimitive(1),IntPrimitive(2),NullPrimitive()),
				List(IntPrimitive(1),IntPrimitive(4),IntPrimitive(2)),
				List(IntPrimitive(2),IntPrimitive(2),IntPrimitive(1)),
				List(IntPrimitive(4),IntPrimitive(2),IntPrimitive(4))
			)

			db.query(convert("SELECT A FROM R;"))(_.toList.map(_.tuple)) must be equalTo List(
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
			db.query(convert("SELECT B FROM R WHERE R.A IN (2,3,4)"))(_.toList.map(_.tuple)) must not contain(Seq(IntPrimitive(3)))
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

		val tableR = Table(ID("R"),ID("R"), Seq(
				ID("A") -> TInt(), 
				ID("B") -> TInt(), 
				ID("C") -> TInt()
			), Seq()
		)
		val tableS = Table(ID("S"),ID("S"), Seq(
				ID("B_0") -> TInt(), 
				ID("D") -> TInt()
			), Seq())
		val tableT = Table(ID("T"),ID("T"), Seq(
				ID("D_0") -> TInt(), 
				ID("E") -> TInt()
			), Seq())
		val testJoin = 
			tableR.join(tableS)
						.filter{ ID("B").eq( Var(ID("B_0")) ) }

		def aggFn(agg: String, alias: String, target: String*) =
			AggFunction(ID.lower(agg), false, target.map { ID(_) }.map { Var(_) }, ID.upper(alias))
		def aggFnExpr(agg: String, alias: String, target: String*) =
			AggFunction(ID.lower(agg), false, target.map { expr(_) }, ID.upper(alias))

		"Parse trivial aggregate queries" in {

			Fragments.foreach(
				// single aggregates
				Seq(
					 "sum"  -> Seq(Var(ID("A"))), 
					 "avg"  -> Seq(Var(ID("A"))), 
					 "min"  -> Seq(Var(ID("A"))), 
					 "max"  -> Seq(Var(ID("A"))), 
					 "count" -> Seq() 
				)
			) { case (agg, args) =>
				db.compiler.optimize(
					convert(s"SELECT $agg(${args.headOption.getOrElse("*")}) FROM R")
				) must be equalTo 
					Aggregate(Seq(), 
						Seq( AggFunction(ID.lower(agg), false, args, ID.upper(agg)) ),
						tableR
					)
			}

			Fragments.foreach(Seq(
				("SELECT SUM(A), SUM(B) FROM R", 
					Seq(aggFn("sum", "sum_1", "A"),
							aggFn("sum", "sum_2", "B")),
					tableR
				), 
				("SELECT COUNT(*) FROM R, S",
					Seq(aggFn("count", "count")), 
					Join(tableR, tableS)
				),
				("SELECT COUNT(*) FROM R, S WHERE R.B = S.B",
					Seq(aggFn("count", "count")), 
					testJoin
				),
				("SELECT SUM(A) FROM R, S WHERE R.B = S.B",
					Seq(aggFn("sum", "sum", "A")),
					testJoin
				),
				("SELECT SUM(A), AVG(D) FROM R, S WHERE R.B = S.B",
					Seq(aggFn("sum","SUM", "A"),
							aggFn("avg","AVG", "D")),
					testJoin
				),
				("SELECT SUM(A + B), AVG(D + B) FROM R, S WHERE R.B = S.B",
					Seq(aggFnExpr("sum", "SUM", "A + B"),
							aggFnExpr("avg", "AVG", "D + B")),
					testJoin	
				),
				("SELECT SUM(A * D) FROM R, S WHERE R.B = S.B",
					Seq(aggFnExpr("sum", "SUM", "A * D")),
					testJoin	
				),
				("SELECT SUM(A * E) FROM R, S, T WHERE (R.B = S.B) AND (S.D = T.D)",
					Seq(aggFnExpr("sum", "SUM", "A * E")),
					testJoin.join(tableT)
									.filter { ID("D").eq { Var(ID("D_0")) } }
				)
			)) { case (query, aggFns, source) =>
				db.compiler.optimize(convert(query)) must be equalTo
					Aggregate(Seq(), aggFns, source)
			}

		}

		"Parse Mixed-Case Aggregate Queries" in {
			db.compiler.optimize(convert("SELECT Sum(A) FROM R")) must be equalTo
				db.table(ID("R")).aggregateParsed( "SUM" -> "SUM(A)" )

			db.compiler.optimize(convert("SELECT sum(A) FROM R")) must be equalTo
				db.table(ID("R")).aggregateParsed( "SUM" -> "SUM(A)" )

			db.compiler.optimize(convert("SELECT first(A) FROM R")) must be equalTo
				db.table(ID("R")).aggregateParsed( "FIRST" -> "FIRST(A)" )

			db.compiler.optimize(convert("SELECT first(A) FROM R GROUP BY B")) must be equalTo
				db.table(ID("R")).groupByParsed("B")( "MIMIR_AGG_FIRST" -> "FIRST(A)" )
					.rename( "MIMIR_AGG_FIRST" -> "FIRST" )
					.project("FIRST")
		}

		"Parse simple aggregate-group by queries" in {
			db.compiler.optimize(convert("SELECT A, SUM(B) FROM R GROUP BY A")) must be equalTo
				tableR.groupByParsed("A")( "SUM" -> "SUM(B)" )

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
			val v = (x:String) => Var(ID(x))
			Fragments.foreach(Seq(
				"SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A" ->
					tableR.groupByParsed( "A" )( "MIMIR_AGG_ALICE" -> "SUM(B)" )
								.map( "BOB" -> v("A"), 
										  "ALICE" -> v("MIMIR_AGG_ALICE")
												),

				"SELECT A, SUM(B) AS ALICE FROM R GROUP BY A" -> 
					tableR.groupByParsed( "A" )( "ALICE" -> "SUM(B)" ),

				"SELECT SUM(B) AS ALICE FROM R GROUP BY A" ->
					tableR.groupByParsed( "A" )( "ALICE" -> "SUM(B)" )
								.removeColumns("A"),

				"SELECT SUM(B), A AS ALICE FROM R GROUP BY A" ->
					tableR.groupByParsed( "A" )( "MIMIR_AGG_SUM" -> "SUM(B)" )
								.map( "SUM" -> v("SUM"), "ALICE" -> v("A") ),

				"SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A, C" ->
					tableR.groupByParsed( "A", "C" )( "MIMIR_AGG_ALICE" -> "SUM(B)" )
					      .map( "BOB" -> v("A"), 
					            "ALICE" -> v("MIMIR_AGG_ALICE") ),

				"SELECT * FROM (SELECT A AS BOB, SUM(B) AS ALICE FROM R GROUP BY A)subq WHERE ALICE > 5" ->
					tableR.groupByParsed( "A" )( "MIMIR_AGG_SUBQ_ALICE" -> "SUM(B)" )
								.map( "BOB" -> v("A"),
										  "ALICE" -> v("MIMIR_AGG_SUBQ_ALICE"))
								.filter { v("ALICE").gt(5) },

				"SELECT A, AVG(B) FROM R GROUP BY A" -> 
					tableR.groupByParsed("A")("AVG" -> "AVG(B)"),

				"SELECT A, COUNT(*) FROM R GROUP BY A" ->
					tableR.groupByParsed("A")("COUNT" -> "COUNT()"),

				"SELECT A, COUNT(*) FROM R, S GROUP BY A" ->
					tableR.join(tableS)
								.groupByParsed("A")("COUNT" -> "COUNT()"),

				"SELECT A, B, COUNT(*) FROM R GROUP BY A,B" ->
					tableR.join(tableS)
								.groupByParsed("A", "B")("COUNT" -> "COUNT()"),

				"SELECT A, R.B, COUNT(*) FROM R, S GROUP BY A, R.B" ->
					tableR.join(tableS)
								.groupByParsed("A", "B")("COUNT" -> "COUNT()"),

				"SELECT A, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A" ->
					testJoin.groupByParsed("A")("COUNT" -> "COUNT()"),

				"SELECT A, R.B, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B" ->
					testJoin.groupByParsed("A", "B")("COUNT" -> "COUNT()")
			)) { case (query, expected) => 
				db.compiler.optimize(convert(query)) must be equalTo expected
			}

		}

		"Parse queries with HAVING clauses" >> {
			db.compiler.optimize(convert("""
				SELECT AVG(A) AS A FROM R GROUP BY C HAVING AVG(B)>70000; 
			""")) must be equalTo
				Project(Seq(ProjectArg(ID("A"), Var(ID("MIMIR_AGG_A")))), 
					db.table(ID("R")).groupByParsed("C")( 
						"MIMIR_AGG_A" -> "AVG(A)",
						"MIMIR_HAVING_0" -> "AVG(B)"
					).filterParsed("MIMIR_HAVING_0 > 70000"))

		}

		"Get the types right in aggregates" >> {
			db.backend.dropTable(ID("PRODUCT_INVENTORY"))
			LoadCSV.handleLoadTableRaw(db, 
				ID("PRODUCT_INVENTORY"),
				"test/data/Product_Inventory.csv",
				Some(Seq(
					ID("ID") -> TString(),
					ID("COMPANY") -> TString(),
					ID("QUANTITY") -> TInt(),
					ID("PRICE") -> TFloat()
				)), 
				Map()
			)
			
			val q = db.compiler.optimize(db.sqlToRA(selectStmt("""
				SELECT COMPANY, SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
			""")))
			q must be equalTo(
				Aggregate(
					List(Var(ID("COMPANY"))), 
					List(AggFunction(ID("sum"), false, List(Var(ID("QUANTITY"))), ID("SUM"))),
					Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), List( 
						ID("ID") -> TString(), 
						ID("COMPANY") -> TString(), 
						ID("QUANTITY") -> TInt(), 
						ID("PRICE") -> TFloat() 
					), List())
				)
			)
			db.typechecker.schemaOf(q) must contain(eachOf[(ID,Type)]( 
				ID("COMPANY") -> TString(), 
				ID("SUM") ->  TInt() 
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
						List(AggFunction(ID("count"), true, List(Var(ID("A"))), ID("SHAZBOT"))),
						Table(ID("R"),ID("R"), Seq(
							ID("A") -> TInt(),
							ID("B") -> TInt(), 
							ID("C") -> TInt()
						), 
						List())
					)
		}

		"Support Aggregates with Selections" >> {
			val q = db.compiler.optimize(db.sqlToRA(selectStmt("""
				SELECT COUNT(DISTINCT COMPANY) AS SHAZBOT
				FROM PRODUCT_INVENTORY
				WHERE COMPANY = 'Apple';
			""")))
			q must be equalTo(
				Aggregate(
					List(),
					// This one is a bit odd... technically the rewrite is correct!
					// I'm not 100% sure that this is valid SQL though.
					List(AggFunction(ID("count"), true, List(StringPrimitive("Apple")), ID("SHAZBOT"))),
					// List(AggFunction(ID("count"), true, List(Var(ID("COMPANY"))), "SHAZBOT")),
					Select(
						Comparison(Cmp.Eq, Var(ID("COMPANY")), StringPrimitive("Apple")),
						Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), Seq( 
								ID("ID") -> TString(), 
								ID("COMPANY") -> TString(), 
								ID("QUANTITY") -> TInt(), 
								ID("PRICE") -> TFloat() 
							), Seq()
					))
				))
		}

		"Respect column ordering of base relations" >> {
			convert("SELECT * FROM R").columnNames must be equalTo(
				Seq(ID("A"), ID("B"), ID("C"))
			)
			convert("SELECT * FROM R_REVERSED").columnNames must be equalTo(
				Seq(ID("C"), ID("B"), ID("A"))
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
		 		View(ID("SANER"), 
			 		Project(List(ProjectArg(ID("A"), Var(ID("A"))), 
			 					 ProjectArg(ID("B"), 
			 					 	 Conditional(IsNullExpression(Var(ID("B"))),
			 					 	 	 Conditional(
			 					 	 	 	Comparison(Cmp.Eq,
				 					 	 	 	VGTerm(ID("SANER:META:B"), 0, Seq(), Seq()),
			 					 	 	 		StringPrimitive("SPARKML")
			 					 	 	 	),
			 					 	 	 	VGTerm(ID("SANER:SPARKML:B"), 0, Seq(RowIdVar()), Seq(Var(ID("A")), Var(ID("B")), Var(ID("C")))),
			 					 	 	  NullPrimitive()
			 					 	 	 ),
			 					 	 	 Var(ID("B"))
		 					 	 	 )),
			 					 ProjectArg(ID("C"), Var(ID("C")))
			 				), Table(ID("R"),ID("R"), Seq(
			 									ID("A") -> TInt(), 
			 								  ID("B") -> TInt(), 
			 								  ID("C") -> TInt()),
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
				(row(ID("A")).asInt, row(ID("B")).asInt, row(ID("C")))
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
			stmt("CREATE LENS test1 AS SELECT * FROM R WITH MISSING_VALUE('A','B')") must be equalTo 
				CreateLens(Name("test1"), 
					MimirSQL.Select("SELECT * FROM R").body,
					Name("MISSING_VALUE"),
					Seq[sparsity.expression.Expression](
						sparsity.expression.StringPrimitive("A"), 
						sparsity.expression.StringPrimitive("B")
					)
				)
			stmt("CREATE LENS test2 AS SELECT * FROM R WITH TYPE_INFERENCE()") must be equalTo 
				CreateLens(Name("test2"), 
					MimirSQL.Select("SELECT * FROM R").body,
					Name("TYPE_INFERENCE"),
					Seq()
				)

		}

		"Support multi-clause CASE statements" in {
			db.compiler.optimize(convert("""
				SELECT CASE WHEN R.A = 1 THEN 'A' WHEN R.A = 2 THEN 'B' ELSE 'C' END AS Q FROM R
			""")) must be equalTo
				Project(List(ProjectArg(ID("Q"), 
						Conditional(expr("A = 1"), StringPrimitive("A"),
							Conditional(expr("A = 2"), StringPrimitive("B"), StringPrimitive("C")
						)))),
					Table(ID("R"),ID("R"), Seq(
						ID("A") -> TInt(), 
						ID("B") -> TInt(), 
						ID("C") -> TInt()
					), Seq())
				)
			
		}

	}
}
