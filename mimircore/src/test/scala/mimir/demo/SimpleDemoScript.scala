package mimir.demo;

import java.io.{StringReader,BufferedReader,FileReader,File}
import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers

import mimir._;
import mimir.sql._;
import mimir.parser._;
import mimir.algebra._;
import mimir.optimizer._;
import mimir.ctables._;
import mimir.exec._;
import net.sf.jsqlparser.statement.{Statement}


object SimpleDemoScript extends Specification with FileMatchers {
	def stmts(f: File): List[Statement] = {
		val p = new MimirJSqlParser(new FileReader(f))
		var ret = List[Statement]();
		var s: Statement = null;

		do{
			s = p.Statement()
			if(s != null) {
				ret = s :: ret;
			}
		} while(s != null)
		ret.reverse
	}
	def stmt(s: String) = {
		new MimirJSqlParser(new StringReader(s)).Statement()
	}
	def select(s: String) = {
		db.convert(
			stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
		)
	}
	def query(s: String) = {
		val query = select(s)
		db.query(query)
	}
	def explainRow(s: String, t: String) = {
		val query = db.convert(
			stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
		)
		db.explainRow(query, RowIdPrimitive(t))
	}
	def explainCell(s: String, t: String, a:String) = {
		val query = db.convert(
			stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
		)
		db.explainCell(query, RowIdPrimitive(t), a)
	}
	def lens(s: String) =
		db.createLens(stmt(s).asInstanceOf[mimir.sql.CreateLens])
	def update(s: Statement) = 
		db.backend.update(s.toString())
	def parser = new ExpressionParser(db.lenses.modelForLens)
	def expr = parser.expr _
	def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]
	def f = FloatPrimitive(_:Double).asInstanceOf[PrimitiveValue]
	def str = StringPrimitive(_:String).asInstanceOf[PrimitiveValue]

	val tempDBName = "tempDBDemoScript"
	val productDataFile = new File("../test/data/Product.sql");
	val reviewDataFiles = List(
			new File("../test/data/ratings1.csv"),
			new File("../test/data/ratings2.csv"),
			new File("../test/data/ratings3.csv")
		)

	val db = new Database(tempDBName, new JDBCBackend("sqlite", tempDBName));

	// The demo spec uses cumulative tests --- Each stage depends on the stages that
	// precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from 
	// automatically parallelizing testing.
	sequential

	"The Basic Demo" should {
		"Be able to open the database" >> {
			val dbFile = new File(new File("databases"), tempDBName)
			if(dbFile.exists()){ dbFile.delete(); }
			dbFile.deleteOnExit();
		    db.backend.open();
			db.initializeDBForMimir();
			dbFile must beAFile
		}

		"Run the Load Product Data Script" >> {
			stmts(productDataFile).map( update(_) )
			db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)
		}

		"Load CSV Files" >> {
			db.loadTable(reviewDataFiles(0))
			db.loadTable(reviewDataFiles(1))
			db.loadTable(reviewDataFiles(2))
			query("SELECT * FROM RATINGS1;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1RAW;").allRows.flatten must contain( str("4.5"), str("A3"), str("4.0"), str("6.4") )
			query("SELECT * FROM RATINGS2;").allRows must have size(3)

		}

    "Create and Query Type Inference Lens with NULL values" >> {
      lens("""
				CREATE LENS null_test
				  AS SELECT * FROM RATINGS3
				  WITH TYPE_INFERENCE(.5)
           					 					 					 					 			""")
      lens("""
				CREATE LENS null_test1
				  AS SELECT * FROM RATINGS3
				  WITH MISSING_VALUE('C')
           					 					 					 					 					 			""")
      val results0 = query("SELECT * FROM null_test;").allRows
      results0 must have size(3)
      results0(2) must contain(str("P34235"), NullPrimitive(), f(4.0))
      query("SELECT * FROM null_test1;").allRows must have size(3)
    }

		"Create and Query Type Inference Lenses" >> {
			lens("""
				CREATE LENS RATINGS1TYPED 
				  AS SELECT * FROM RATINGS1RAW 
				  WITH TYPE_INFERENCE(0.5)
			""")
			lens("""
				CREATE LENS RATINGS2TYPED 
				  AS SELECT * FROM RATINGS2RAW
				  WITH TYPE_INFERENCE(0.5)
			""")
			query("SELECT * FROM RATINGS1TYPED;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1TYPED;").allRows.flatten must contain(eachOf(f(4.5), f(4.0), f(6.4), NullPrimitive()))
			query("SELECT * FROM RATINGS1TYPED WHERE RATING IS NULL").allRows must have size(1)
			query("SELECT * FROM RATINGS1TYPED WHERE RATING > 4;").allRows must have size(2)
			query("SELECT * FROM RATINGS2TYPED;").allRows must have size(3)
			Typechecker.schemaOf(
				InlineVGTerms.optimize(select("SELECT * FROM RATINGS2TYPED;"))
			).map(_._2) must be equalTo List(Type.TString, Type.TFloat, Type.TFloat)
		}

		"Compute Aggregate Queries" >> {
			query("""
				SELECT EVALUATION, SUM(NUM_RATINGS)
				FROM RATINGS2TYPED
				WHERE EVALUATION > 3.0
				GROUP BY EVALUATION;
															""").allRows must have size(2)
		}

		"Create and Query Domain Constraint Repair Lenses" >> {
			lens("""
				CREATE LENS RATINGS1FINAL 
				  AS SELECT * FROM RATINGS1TYPED 
				  WITH MISSING_VALUE('RATING')
			""")
			val result1 = query("SELECT RATING FROM RATINGS1FINAL").allRows.flatten
			result1 must have size(4)
			result1 must contain(eachOf( f(4.5), f(4.0), f(6.4), i(4) ) )
			val result2 = query("SELECT RATING FROM RATINGS1FINAL WHERE RATING < 5").allRows.flatten
			result2 must have size(3)
		}

		"Show Determinism Correctly" >> {
			lens("""
				CREATE LENS PRODUCT_REPAIRED 
				  AS SELECT * FROM PRODUCT
				  WITH MISSING_VALUE('BRAND')
			""")
			val result1 = query("SELECT ID, BRAND FROM PRODUCT_REPAIRED")
			val result1Determinism = result1.mapRows( r => (r(0).asString, r.deterministicCol(1)) )
			result1Determinism must contain(eachOf( ("P123", false), ("P125", true), ("P34235", true) ))

			val result2 = query("SELECT ID, BRAND FROM PRODUCT_REPAIRED WHERE BRAND='HP'")
			val result2Determinism = result2.mapRows( r => (r(0).asString, r.deterministicCol(1), r.deterministicRow) )
			result2Determinism must contain(eachOf( ("P123", false, false), ("P34235", true, true) ))
		}

		"Create and Query Schema Matching Lenses" >> {
			lens("""
				CREATE LENS RATINGS2FINAL 
				  AS SELECT * FROM RATINGS2TYPED 
				  WITH SCHEMA_MATCHING(PID string, RATING float, REVIEW_CT float)
			""")
			val result1 = query("SELECT RATING FROM RATINGS2FINAL").allRows.flatten
			result1 must have size(3)
			result1 must contain(eachOf( f(121.0), f(5.0), f(4.0) ) )
		}

		"Obtain Row Explanations for Simple Queries" >> {
			val expl = explainRow("""
					SELECT * FROM RATINGS2FINAL WHERE RATING > 3
				""", "1")
			expl.toString must contain("I assumed that NUM_RATINGS maps to RATING")		
		}

		"Obtain Cell Explanations for Simple Queries" >> {
			val expl1 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "2", "RATING")
			expl1.toString must contain("I made a best guess estimate for this data element, which was originally NULL")		
		}
		"Guard Data-Dependent Explanations for Simple Queries" >> {
			val expl2 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "1", "RATING")
			expl2.toString must not contain("I made a best guess estimate for this data element, which was originally NULL")		
		}

		"Query a Union of lenses" >> {
			val result1 = query("""
				SELECT PID FROM RATINGS1FINAL 
					UNION ALL 
				SELECT PID FROM RATINGS2FINAL
			""").allRows.flatten
			result1 must have size(7)
			result1 must contain(eachOf( 
				str("P123"), str("P124"), str("P125"), str("P325"), str("P2345"), 
				str("P34234"), str("P34235")
			))

			val result2 = query("""
				SELECT PID FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) allratings
			""").allRows.flatten
			result2 must have size(7)
			result2 must contain(eachOf( 
				str("P123"), str("P124"), str("P125"), str("P325"), str("P2345"), 
				str("P34234"), str("P34235")
			))
		}

		"Query a Filtered Union of lenses" >> {
			val result = query("""
				SELECT pid FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r
				WHERE rating > 4;
			""").allRows.flatten
			result must have size(4)
			result must contain(eachOf( 
				str("P123"), str("P125"), str("P325"), str("P34234")
			))
		}

		"Query a Join of a Union of Lenses" >> {
			val result0 = query("""
				SELECT p.name, r.rating FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
			""").allRows.flatten
			result0 must have size(12)
			result0 must contain(eachOf( 
				str("Apple 6s, White"),
				str("Sony to inches"),
				str("Apple 5s, Black"),
				str("Samsung Note2"),
				str("Dell, Intel 4 core"),
				str("HP, AMD 2 core")
			))

			val result0tokenTest = query("""
				SELECT p.name, r.rating FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
			""")
			var result0tokens = List[RowIdPrimitive]()
			result0tokenTest.open()
			while(result0tokenTest.getNext()){ 
				result0tokens = result0tokenTest.provenanceToken :: result0tokens
			}
			result0tokens.map(_.asString) must contain(allOf(
				"3|right|6", 
				"2|right|5", 
				"2|left|4",
				"1|right|3", 
				"3|left|2", 
				"1|left|1"
			))

			val explain0 = explainCell("""
				SELECT p.name, r.rating FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				""", "1|right|3", "RATING")
			explain0.reasons.map(_.model) must contain(eachOf(
				"RATINGS2FINAL",
				"RATINGS2TYPED"
			))

			val result1 = query("""
				SELECT name FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
				WHERE rating > 4;
			""").allRows.flatten
			result1 must have size(6)
			result1 must contain(eachOf( 
				str("Apple 6s, White"),
				str("Sony to inches"),
				str("Apple 5s, Black"),
				str("Samsung Note2"),
				str("Dell, Intel 4 core"),
				str("HP, AMD 2 core")
			))

			val result2 = query("""
				SELECT name FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id
				  AND rating > 4;
			""").allRows.flatten
			result2 must have size(3)
			result2 must contain(eachOf( 
				str("Apple 6s, White"),
				str("Samsung Note2"),
				str("Dell, Intel 4 core")
			))

			
		}

		"Query MIMIR_LENSES" >> {
			val q1 = query("""SELECT * FROM MIMIR_LENSES;""").allRows.flatten
			q1 must have size(40)
		}

		"Missing Value Best Guess Debugging" >> {
			// Regression check for issue #81
			val q3 = select("""
				SELECT * FROM RATINGS1FINAL r, Product p
				WHERE r.pid = p.id;
			""")
			val q3compiled = db.compiler.compile(q3)
			q3compiled.open()

			// Preliminaries: This isn't required for correctness, but the test case depends on it.
			q3compiled must beAnInstanceOf[NonDetIterator]

			//Test another level down the heirarchy too
			val q3dbquery = q3compiled.asInstanceOf[NonDetIterator].src
			q3dbquery must beAnInstanceOf[ResultSetIterator]

			// Again, the internal schema must explicitly state that the column is a rowid
			q3dbquery.asInstanceOf[ResultSetIterator].visibleSchema must havePair ( "MIMIR_ROWID_0" -> Type.TRowId )
			// And the returned object had better conform
			q3dbquery.provenanceToken must beAnInstanceOf[RowIdPrimitive]


			val result3 = query("""
				SELECT * FROM RATINGS1FINAL r, Product p
				WHERE r.pid = p.id;
			""").allRows
			result3 must have size(3)


			val result4 = query("""
				SELECT * FROM (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
			""").allRows
			result4 must have size(6)
		}
	}
}
