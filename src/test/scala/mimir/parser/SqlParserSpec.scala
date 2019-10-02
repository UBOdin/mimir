package mimir.parser;

import java.io.{StringReader,BufferedReader,FileReader,File}
import java.sql.SQLException
import scala.collection.JavaConversions._

import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import org.specs2.specification._
import org.specs2.specification.core.{Fragment,Fragments}
import com.typesafe.scalalogging.Logger

import sparsity.Name
import sparsity.parser.SQL

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.backend._
import mimir.metadata._
import mimir.util._
import mimir.test._
import mimir.ctables._
import mimir.ml.spark.SparkML
import mimir.data.{ FileFormat, LoadedTables }

class SqlParserSpec 
	extends SQLTestSpecification("SQLParserSpec")
	with FileMatchers
	with BeforeAll
{

	val testData = Seq[ (String, String, Seq[String]) ](
			(	"R", "test/r_test/r.csv", 
				Seq("A", "B", "C")
			),
			("S", "test/r_test/s.csv",
				Seq("B", "D")
			),
			("T", "test/r_test/t.csv",
				Seq("D", "E")
			)
		)

	def beforeAll = 
	{
		for( ( tableName, tableData, tableCols ) <- testData){
			// println(s"Loading $tableName")
			db.loader.linkTable(
				sourceFile = tableData,
				targetTable = ID(tableName),
				format = FileFormat.CSV
			)
		}
	}

	def convert(stmt: String) = db.sqlToRA(selectStmt(stmt))


	val tableR = Table(ID("R"), LoadedTables.SCHEMA, Seq(
			ID("_c0") -> TString(), 
			ID("_c1") -> TString(), 
			ID("_c2") -> TString()
		), Seq()
	)
	val tableS = Table(ID("S"), LoadedTables.SCHEMA, Seq(
			ID("_c0_0") -> TString(), 
			ID("_c1_0") -> TString()
		), Seq())
	val tableT = Table(ID("T"), LoadedTables.SCHEMA, Seq(
			ID("_c0_1") -> TString(), 
			ID("_c1_1") -> TString()
		), Seq())
	val testJoin = 
		tableR.join(tableS)
					.filter{ ID("_c1").eq( Var(ID("_c0_0")) ) }

	def aggFn(agg: String, alias: String, target: String*) =
		AggFunction(ID.lower(agg), false, target.map { ID(_) }.map { Var(_) }, ID.upper(alias))
	def aggFnExpr(agg: String, alias: String, target: String*) =
		AggFunction(ID.lower(agg), false, target.map { expr(_) }, ID.upper(alias))


	val v = (x:String) => Var(ID(x))

	sequential

	"The Sql Parser" should {
		"Handle trivial queries" >> {
			db.query(convert("SELECT * FROM R;"))(_.toList.map(_.tuple)) must be equalTo List( 
				List(StringPrimitive("1"),StringPrimitive("2"),StringPrimitive("3")),
				List(StringPrimitive("1"),StringPrimitive("3"),StringPrimitive("1")),
				List(StringPrimitive("2"),NullPrimitive(),StringPrimitive("1")),
				List(StringPrimitive("1"),StringPrimitive("2"),NullPrimitive()),
				List(StringPrimitive("1"),StringPrimitive("4"),StringPrimitive("2")),
				List(StringPrimitive("2"),StringPrimitive("2"),StringPrimitive("1")),
				List(StringPrimitive("4"),StringPrimitive("2"),StringPrimitive("4"))
			)

			db.query(convert("SELECT `_c0` FROM R;"))(_.toList.map(_.tuple)) must be equalTo List(
				List(StringPrimitive("1")),
				List(StringPrimitive("1")),
				List(StringPrimitive("2")),
				List(StringPrimitive("1")),
				List(StringPrimitive("1")),
				List(StringPrimitive("2")),
				List(StringPrimitive("4"))
			)
		}

		"Handle IN queries" >> {
			db.query(
				convert("SELECT `_c1` FROM R WHERE R.`_c0` IN ('2','3','4');")
			) { _.toList.map(_.tuple) } must not contain(Seq(StringPrimitive("3"))) 
		}

		"Handle CAST operations" >> {
			val cast1:(String=>Type) = (tstring: String) =>
				db.typechecker.schemaOf(convert(s"SELECT CAST('FOO' AS $tstring) FROM R;"))(0)._2

			cast1("int") must be equalTo TInt()
			cast1("double") must be equalTo TFloat()
			cast1("string") must be equalTo TString()
			cast1("date") must be equalTo TDate()
			cast1("timestamp") must be equalTo TTimestamp()
			cast1("flibble") must throwA[RAException]
		}

		"Parse trivial aggregate queries" >> {

			Fragments.foreach(
				// single aggregates
				Seq(
					 "sum"  -> Seq(Var(ID("_c0"))), 
					 "avg"  -> Seq(Var(ID("_c0"))), 
					 "min"  -> Seq(Var(ID("_c0"))), 
					 "max"  -> Seq(Var(ID("_c0"))), 
					 "count" -> Seq() 
				).map { case (agg, args) => (agg, args, s"SELECT $agg(${args.headOption.getOrElse("*")}) FROM R;") }
			) { case (agg, args, query) =>
				query in {
					db.compiler.optimize(convert(query)) must be equalTo 
						Aggregate(Seq(), 
							Seq( AggFunction(ID.lower(agg), false, args, ID.upper(agg)) ),
							tableR
						)
				}
			}

			Fragments.foreach(Seq(
				("SELECT SUM(`_c0`), SUM(`_c1`) FROM R;", 
					Seq(aggFn("sum", "sum_1", "_c0"),
							aggFn("sum", "sum_2", "_c1")),
					tableR
				), 
				("SELECT COUNT(*) FROM R, S;",
					Seq(aggFn("count", "count")), 
					Join(tableR, tableS)
				),
				("SELECT COUNT(*) FROM R, S WHERE R.`_c1` = S.`_c0`;",
					Seq(aggFn("count", "count")), 
					testJoin
				),
				("SELECT SUM(`_c0`) FROM R, S WHERE R.`_c1` = S.`_c0`;",
					Seq(aggFn("sum", "sum", "_c0")),
					testJoin
				)
			)) { case (query, aggFns, source) =>

				query in {
					db.compiler.optimize(convert(query)) must be equalTo
						Aggregate(Seq(), aggFns, source)
				}
			}

		}

		"Parse Mixed-Case Aggregate Queries" >> {
			db.compiler.optimize(convert("SELECT Sum(`_c0`) FROM R;")) must be equalTo
				db.table("R").aggregateParsed( "SUM" -> "SUM(`_c0`)" )

			db.compiler.optimize(convert("SELECT sum(`_c0`) FROM R;")) must be equalTo
				db.table("R").aggregateParsed( "SUM" -> "SUM(`_c0`)" )

			db.compiler.optimize(convert("SELECT first(`_c0`) FROM R;")) must be equalTo
				db.table("R").aggregateParsed( "FIRST" -> "FIRST(`_c0`)" )

			db.compiler.optimize(convert("SELECT first(`_c0`) FROM R GROUP BY `_c1`;")) must be equalTo
				db.table("R").groupByParsed("_c1")( "MIMIR_AGG_FIRST" -> "FIRST(`_c0`)" )
					.rename( "MIMIR_AGG_FIRST" -> "FIRST" )
					.project("FIRST")
		}

		"Parse simple aggregate-group by queries" >> {

			/* Illegal Group By Queries */
			db.compiler.optimize(convert("SELECT `_c0`, SUM(`_c1`) FROM R GROUP BY `_c2`;")) must throwA[SQLException]

			/* Illegal All Columns/All Table Columns queries */
			db.compiler.optimize(convert("SELECT SUM(`_c1`), * FROM R GROUP BY `_c2`;")) must throwA[SQLException]
			db.compiler.optimize(convert("SELECT R.*, SUM(`_c1`) FROM R GROUP BY `_c2`;")) must throwA[SQLException]

			/* Variant Test Cases */

			Fragments.foreach(Seq(
				"SELECT `_c0` AS BOB, SUM(`_c1`) AS ALICE FROM R GROUP BY `_c0`;" ->
					tableR.groupByParsed( "_c0" )( "MIMIR_AGG_ALICE" -> "SUM(`_c1`)" )
								.map( "BOB" -> v("_c0"), 
										  "ALICE" -> v("MIMIR_AGG_ALICE")
												),

				"SELECT * FROM (SELECT `_c0` AS BOB, SUM(`_c1`) AS ALICE FROM R GROUP BY `_c0`) subq WHERE ALICE > 5;" ->
					tableR.groupByParsed( "_c0" )( "MIMIR_AGG_SUBQ_ALICE" -> "SUM(`_c1`)" )
								.filter { v("MIMIR_AGG_SUBQ_ALICE").gt(5) }
								.map( "BOB" -> v("_c0"),
										  "ALICE" -> v("MIMIR_AGG_SUBQ_ALICE")),

				"SELECT `_c2`, R.`_c1`, COUNT(*) FROM R, S WHERE R.`_c1` = S.`_c0` GROUP BY `_c2`, R.`_c1`;" ->
					testJoin.groupByParsed("_c2", "_c1")("COUNT" -> "COUNT()")
			)) { case (query, expected) => 
				query in {
					db.compiler.optimize(convert(query)) should be equalTo expected
				}
			}
		}

		"Parse queries with HAVING clauses" >> {
			LoggerUtils.trace(
				// "mimir.sql.SqlToRA"
			) { 
				db.compiler.optimize(convert("""
					SELECT AVG(`_c0`) AS A FROM R GROUP BY `_c2` HAVING AVG(`_c1`)>70000; 
				""")) must be equalTo
					Project(Seq(ProjectArg(ID("A"), Var(ID("MIMIR_AGG_A")))), 
						db.table("R").groupByParsed("_c2")( 
							"MIMIR_AGG_A" -> "AVG(`_c0`)",
							"MIMIR_HAVING_0" -> "AVG(`_c1`)"
						).filterParsed("MIMIR_HAVING_0 > 70000"))
			}
		}

		"Get the types right in aggregates" >> {
			loadCSV( 
				sourceFile = "test/data/Product_Inventory.csv",
				targetTable = "PRODUCT_INVENTORY",
				targetSchema = Seq("PID", "COMPANY", "QUANTITY", "PRICE")
			)
			
			val q = db.compiler.optimize(db.sqlToRA(selectStmt("""
				SELECT COMPANY, SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
			""")))
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
			db.compiler.optimize(convert("SELECT COUNT(DISTINCT `_c0`) AS SHAZBOT FROM R;")) must be equalTo
					Aggregate(
						List(),
						List(AggFunction(ID("count"), true, List(Var(ID("_c0"))), ID("SHAZBOT"))),
						tableR
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
						db.table("product_inventory")
					)
				)
			)
		}

		"Create and query lenses" >> {
		 	db.update(stmt(
		 		"CREATE LENS SaneR AS SELECT * FROM R WITH MISSING_VALUE('`_C1`');"
		 	).asInstanceOf[CreateLens]);
		 	db.tableExists("SANER") must beTrue
		 	db.compiler.optimize(
		 		convert("SELECT * FROM SaneR")
		 	) must be equalTo db.compiler.optimize(db.table("SANER"))

			// val guessCacheData = 
			//  	db.backend.resultRows("SELECT "+
			//  		db.bestGuessCache.keyColumn(0)+","+
			//  		db.bestGuessCache.dataColumn+" FROM "+
			//  		db.bestGuessCache.cacheTableForModel(
			//  			db.models.get("SANER:WEKA:B"), 0)
			//  	)
			// guessCacheData must contain( ===(Seq[PrimitiveValue](IntPrimitive(3), IntPrimitive(2))) )
		 	
			db.query(convert("SELECT * FROM SaneR;")){ _.map { row =>
				(row(ID("_C0")).asInt, row(ID("_C1")).asInt, row(ID("_C2")))
			}.toSeq must contain(
				(1, 2, StringPrimitive("3")),
				(1, 3, StringPrimitive("1")),
				(2, 2, StringPrimitive("1")),
				(1, 2, NullPrimitive()),
				(1, 4, StringPrimitive("2")),
				(2, 2, StringPrimitive("1")),
				(4, 2, StringPrimitive("4"))
			) }
		}

		"Create Lenses with no or multiple arguments" >> {
			stmt("CREATE LENS test1 AS SELECT * FROM R WITH MISSING_VALUE('A','B');") must be equalTo 
				CreateLens(Name("test1"), 
					MimirSQL.Select("SELECT * FROM R").body,
					Name("MISSING_VALUE"),
					Seq[sparsity.expression.Expression](
						sparsity.expression.StringPrimitive("A"), 
						sparsity.expression.StringPrimitive("B")
					)
				)
			stmt("CREATE LENS test2 AS SELECT * FROM R WITH TYPE_INFERENCE();") must be equalTo 
				CreateLens(Name("test2"), 
					MimirSQL.Select("SELECT * FROM R").body,
					Name("TYPE_INFERENCE"),
					Seq()
				)

		}

		"Support multi-clause CASE statements" >> {
			db.compiler.optimize(convert("""
				SELECT CASE WHEN R.`_c0` = '1' THEN 'A' WHEN R.`_c0` = '2' THEN 'B' ELSE 'C' END AS Q FROM R;
			""")) must be equalTo
				Project(List(ProjectArg(ID("Q"), 
						Conditional(expr("`_c0` = '1'"), StringPrimitive("A"),
							Conditional(expr("`_c0` = '2'"), StringPrimitive("B"), StringPrimitive("C")
						)))),
					tableR
				)
			
		}

	}
}
