package mimir.demo

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
import mimir.util._;
import net.sf.jsqlparser.statement.{Statement}

/**
  * Created by ahuber on 8/18/2016.
  */
object SimpleDemoScriptAgg extends Specification with FileMatchers {
  //Return a list of all db/sql statements
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
    db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
  }
  def query(s: String) = {
    val query = select(s)
    db.query(query)
  }
  def explainRow(s: String, t: String) = {
    val query = db.sql.convert(
      stmt(s).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
    )
    db.explainRow(query, RowIdPrimitive(t))
  }
  def explainCell(s: String, t: String, a:String) = {
    val query = db.sql.convert(
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
  val inventoryDataFile = new File("../test/data/Product_Inventory.sql")
  val inventoryDataMVFile = new File("../test/data/Product_Inventory_MV.sql")
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

    "Run the Load Product Data Scripts" >> {
      stmts(productDataFile).map( update(_) )
      db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)

      stmts(inventoryDataFile).map( update(_) )
      db.backend.resultRows("SELECT * FROM PRODUCT_INVENTORY;") must have size(6)

      stmts(inventoryDataMVFile).map( update(_) )
      db.backend.resultRows("SELECT * FROM PRODUCT_INVENTORY_MV;") must have size(6)
    }

    "Load CSV Files" >> {
      db.loadTable(reviewDataFiles(0))
      db.loadTable(reviewDataFiles(1))
      db.loadTable(reviewDataFiles(2))
      query("SELECT * FROM RATINGS1;").allRows must have size(4)
      query("SELECT RATING FROM RATINGS1RAW;").allRows.flatten must contain( str("4.5"), str("A3"), str("4.0"), str("6.4") )
      query("SELECT * FROM RATINGS2;").allRows must have size(3)

    }

    "Use Sane Types in Lenses" >> {
      var oper = select("SELECT * FROM RATINGS2")
      Typechecker.typeOf(Var("NUM_RATINGS"), oper) must be oneOf(Type.TInt, Type.TFloat, Type.TAny)
    }

    "Create and Query Type Inference Lens with NULL values" >> {
      lens("""
				CREATE LENS null_test
				  AS SELECT * FROM RATINGS3
				  WITH MISSING_VALUE('C')
           			""")
      val results0 =
        LoggerUtils.debug(List(
          // "mimir.exec.Compiler",
          // "mimir.sql.sqlite.MimirCast$"
        ), () => {
          query("SELECT * FROM RATINGS3;").allRows
        })
      results0 must have size(3)
      results0(2) must contain(str("P34235"), NullPrimitive(), f(4.0))
      query("SELECT * FROM null_test;").allRows must have size(3)
    }


    "Create and Query Type Inference Lenses" >> {
      query("SELECT * FROM RATINGS1;").allRows must have size(4)
      query("SELECT RATING FROM RATINGS1;").allRows.flatten must contain(eachOf(f(4.5), f(4.0), f(6.4), NullPrimitive()))
      query("SELECT * FROM RATINGS1 WHERE RATING IS NULL").allRows must have size(1)
      query("SELECT * FROM RATINGS1 WHERE RATING > 4;").allRows must have size(2)
      query("SELECT * FROM RATINGS2;").allRows must have size(3)
      Typechecker.schemaOf(
        InlineVGTerms.optimize(select("SELECT * FROM RATINGS2;"))
      ).map(_._2) must be equalTo List(Type.TString, Type.TFloat, Type.TFloat)
    }

    "Compute Non-Deterministic Aggregate Queries" >> {
      lens("""
				CREATE LENS PRODUCT_Inventory_MV_REPAIRED
				  AS SELECT * FROM PRODUCT_Inventory_MV
				  WITH MISSING_VALUE('QUANTITY')
           			""")
      val result = query("SELECT SUM(QUANTITY) FROM PRODUCT_INVENTORY_MV_REPAIRED;").allRows.flatten
      result must have size(1)
    }

  }
}
