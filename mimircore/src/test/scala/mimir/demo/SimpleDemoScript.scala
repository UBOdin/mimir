package mimir.demo;

import java.io.{StringReader,BufferedReader,FileReader,File}
import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers

import mimir._;
import mimir.sql._;
import mimir.parser._;
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
			query("SELECT * FROM RATINGS2;").allRows must have size(3)
		}

		"Create Type Inference Lenses" >> {
			lens("CREATE LENS RATINGS1TYPED AS SELECT * FROM RATINGS1 WITH TYPE_INFERENCE(0.5)")
			lens("CREATE LENS RATINGS2TYPED AS SELECT * FROM RATINGS2 WITH TYPE_INFERENCE(0.5)")
			query("SELECT * FROM RATINGS1TYPED;").allRows must have size(4)
			query("SELECT * FROM RATINGS2TYPED;").allRows must have size(3)
		}
	}
}
