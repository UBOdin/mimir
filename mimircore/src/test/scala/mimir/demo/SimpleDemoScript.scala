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
	def query(s: String) = 
		db.query(db.convert(stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]))
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

		"Create and Query DCR Lenses" >> {
			lens("""
				CREATE LENS RATINGS1FINAL 
				  AS SELECT * FROM RATINGS1TYPED 
				  WITH MISSING_VALUE('RATING')
			""")
			val result = query("SELECT RATING FROM RATINGS1FINAL").allRows.flatten
			result must have size(4)
			result must contain(eachOf( f(4.5), f(4.0), f(6.4), i(4) ) )
		}
	}
}
