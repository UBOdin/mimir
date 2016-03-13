package mimir.demo;

import java.io.{StringReader,BufferedReader,FileReader,File}
import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers

import mimir._;
import mimir.sql._;
import mimir.parser._;
import mimir.algebra._;
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
	def query(s: String) = {
		val query = db.convert(
			stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
		)
		db.check(query);
		db.query(query)
	}
	def explainRow(s: String, t: String) = {
		val query = db.convert(
			stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
		)
		db.explainRow(query, RowIdPrimitive(t))
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
			new File("../test/data/ratings2.csv")
		)

	val db = new Database(tempDBName, new JDBCBackend("sqlite", tempDBName));

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
			query("SELECT * FROM RATINGS1;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1;").allRows.flatten must contain( str("4.5"), str("4.0"), str("6.4") )
			query("SELECT * FROM RATINGS2;").allRows must have size(3)

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
			query("SELECT RATING FROM RATINGS1TYPED;").allRows.flatten must contain( 
				f(4.5), f(4.0), f(6.4) 
			)
			query("SELECT * FROM RATINGS2TYPED;").allRows must have size(3)
		}

		"Create and Query Domain Constraint Repair Lenses" >> {
			lens("""
				CREATE LENS RATINGS1FINAL 
				  AS SELECT * FROM RATINGS1TYPED 
				  WITH MISSING_VALUE('RATING')
			""")
			val result = query("SELECT RATING FROM RATINGS1FINAL").allRows.flatten
			result must have size(4)
			result must contain(eachOf( f(4.5), f(4.0), f(6.4), i(4) ) )
		}

		"Create and Query Schema Matching Lenses" >> {
			lens("""
				CREATE LENS RATINGS2FINAL 
				  AS SELECT * FROM RATINGS2TYPED 
				  WITH SCHEMA_MATCHING(PID string, RATING float, REVIEW_CT float)
			""")
			val result = query("SELECT RATING FROM RATINGS2FINAL").allRows.flatten
			result must have size(3)
			result must contain(eachOf( f(121.0), f(5.0), f(4.0) ) )
		}

		"Obtain Explanations for Simple Queries" >> {
			val expl = explainRow("""
					SELECT * FROM RATINGS2FINAL WHERE RATING > 3
				""", "1")
			expl.toString must contain("I assumed that NUM_RATINGS maps to RATING")		
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
	}
}
