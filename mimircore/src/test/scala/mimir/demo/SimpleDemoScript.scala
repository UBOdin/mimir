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
		db.check(query);
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
		db.update(s.toString())
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
			db.query("SELECT * FROM PRODUCT;").allRows must have size(6)
		}

		"Load CSV Files" >> {
			db.loadTable(reviewDataFiles(0))
			db.loadTable(reviewDataFiles(1))
			db.loadTable(reviewDataFiles(2))
			query("SELECT * FROM RATINGS1;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1;").allRows.flatten must contain( str("4.5"), str("A3"), str("4.0"), str("6.4") )
			query("SELECT * FROM RATINGS2;").allRows must have size(3)

		}

		"Compute Aggregate Queries" >> {
			query("SELECT EVALUATION, SUM(NUM_RATINGS) FROM RATINGS2 WHERE EVALUATION > 3.0 GROUP BY EVALUATION;")
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
      query("SELECT * FROM null_test WHERE EVALUATION IS NULL;").allRows.flatten must contain(str("P34235"), NullPrimitive(), f(4.0))
      query("SELECT * FROM null_test;").allRows.flatten must have size(9)
      query("SELECT * FROM null_test1;").allRows.flatten must have size(9)
    }


		"Create and Query Type Inference Lenses" >> {
			lens("""
				CREATE LENS RATINGS1TYPED 
				  AS SELECT * FROM RATINGS1 
				  WITH TYPE_INFERENCE(0.5)
			""")
			lens("""
				CREATE LENS RATINGS2TYPED 
				  AS SELECT * FROM RATINGS2
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

		"Obtain Column Explanations for Simple Queries" >> {
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

			val explain0 = explainCell("""
				SELECT p.name, r.rating FROM Product p, (
					SELECT * FROM RATINGS1FINAL 
						UNION ALL 
					SELECT * FROM RATINGS2FINAL
				) r
				""", "3.1.right", "RATING")
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

		"Missing Value Best Guess Debugging" >> {
			// Regression check for issue #81
			val q3 = select("""
				SELECT * FROM RATINGS1FINAL r, Product p
				WHERE r.pid = p.id;
			""")
			val q3compiled = db.compiler.compile(q3)
			q3compiled.open()

			// Preliminaries: This isn't required for correctness, but the test case depends on it.
			q3compiled must beAnInstanceOf[ProjectionResultIterator]

			val q3provquery = q3compiled.asInstanceOf[ProjectionResultIterator].src 
			// Again, we don't strictly need the type of the inlined result iterator to be this, but the test case needs to be able to inspect the insides
			q3provquery must beAnInstanceOf[NDInlineResultIterator]
			val rowidIdx1 = q3provquery.schema.indexWhere(_._1.equals("ROWID_MIMIR_1"))

			//Mimir synthesizes a rowID column for the missing value lens.  That column had better pretend to be an integer
			q3provquery.schema(rowidIdx1)._2 must be equalTo Type.TRowId
			//That column had better be an integer too.
			q3provquery(rowidIdx1) must beAnInstanceOf[RowIdPrimitive]

			//Test another level down the heirarchy too
			val q3dbquery = q3provquery.asInstanceOf[NDInlineResultIterator].src
			q3dbquery must beAnInstanceOf[ResultSetIterator]
			val rowidIdx2 = q3dbquery.schema.indexWhere(_._1.equals("ROWID_MIMIR_1"))

			// Again, the internal schema must explicitly state that the column is a rowid
			q3dbquery.asInstanceOf[ResultSetIterator].visibleSchema must contain ( ("ROWID_MIMIR_1", Type.TRowId) )
			// The external schema too!
			q3dbquery.schema(rowidIdx2)._2 must be equalTo Type.TRowId
			// And the returned object had better conform
			q3dbquery(rowidIdx2) must beAnInstanceOf[RowIdPrimitive]


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
