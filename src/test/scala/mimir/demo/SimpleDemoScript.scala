package mimir.demo

import java.io.{BufferedReader, File, FileReader, StringReader}
import java.sql.SQLException

import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import mimir._
import mimir.sql._
import mimir.parser._
import mimir.algebra._
import mimir.optimizer._
import mimir.ctables._
import mimir.exec._
import mimir.util._
import mimir.test._
import net.sf.jsqlparser.statement.Statement


object SimpleDemoScript
	extends SQLTestSpecification("tempDBDemoScript")
	with FileMatchers
{
	// The demo spec uses cumulative tests --- Each stage depends on the stages that
	// precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
	// automatically parallelizing testing.
	sequential

	val productDataFile = new File("test/data/Product.sql");
	val reviewDataFiles = List(
			new File("test/data/ratings1.csv"),
			new File("test/data/ratings2.csv"),
			new File("test/data/ratings3.csv"),
			new File("test/data/userTypes.csv")
		)

	"The Basic Demo" should {
		"Be able to open the database" >> {
			db // force the DB to be loaded
			dbFile must beAFile
		}

		"Run the Load Product Data Script" >> {
			stmts(productDataFile).map( update(_) )
			db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)
		}

		"Load CSV Files" >> {
			reviewDataFiles.foreach( db.loadTable(_) )
			query("SELECT * FROM RATINGS1;") { _.toSeq must have size(4) }
			db.query(db.adaptiveSchemas.viewFor("RATINGS1_DH", "DATA").get.project("RATING")) { 
				_.map { _(0) }.toSeq must contain( str("4.5"), str("A3"), str("4.0"), str("6.4") )
			}
			query("SELECT * FROM RATINGS2;") { _.toSeq must have size(3) }
			query("SELECT PID FROM RATINGS2;") { _.map { _(0) } must contain((_:PrimitiveValue).isInstanceOf[StringPrimitive]).forall }
		}



		"Use Sane Types in Lenses" >> {
			var oper = select("SELECT * FROM RATINGS2")
			db.typechecker.typeOf(Var("NUM_RATINGS"), oper) must be oneOf(TInt(), TFloat(), TAny())
		}

    "Create and Query Type Inference Adaptive Schema with NULL values" >> {
      update("""
				CREATE LENS null_test
				  AS SELECT * FROM RATINGS3
				  WITH MISSING_VALUE('EVALUATION')
 			""")
      query("SELECT * FROM null_test;"){ _.toSeq must have size(3) }

			LoggerUtils.debug(
				// "mimir.exec.Compiler",
				// "mimir.sql.sqlite.MimirCast$"
			){
      	query("SELECT * FROM RATINGS3;"){ result =>
		      val results0 = result.toSeq
		      results0 must have size(3)
		      results0(2).tuple must contain(str("P34235"), NullPrimitive(), f(4.0))
      	}
	    }
    }


		"Create and Query Type Inference Adaptive Schemas" >> {
			db.adaptiveSchemas.create("NEW_TYPES_TI", "TYPE_INFERENCE", db.table("USERTYPES"), Seq(FloatPrimitive(.9))) 
			db.views.create("NEW_TYPES", db.adaptiveSchemas.viewFor("NEW_TYPES_TI", "DATA").get)
      query("SELECT * FROM new_types;"){ _.toSeq must have size(3) }
			query("SELECT * FROM RATINGS1;"){ _.toSeq must have size(4) }
			query("SELECT RATING FROM RATINGS1;"){ _.map { _(0) }.toSeq must contain(eachOf(f(4.5), f(4.0), f(6.4), NullPrimitive())) }
			query("SELECT * FROM RATINGS1 WHERE RATING IS NULL"){ _.toSeq must have size(1) }
			query("SELECT * FROM RATINGS1 WHERE RATING > 4;"){ _.toSeq must have size(2) }
			query("SELECT * FROM RATINGS2;"){ _.toSeq must have size(3) }
			db.schemaOf(select("SELECT * FROM RATINGS2;")).
				map(_._2).map(Type.rootType _) must be equalTo List(TString(), TFloat(), TFloat())
		}

		"Create and Query Domain Constraint Repair Lenses" >> {
			LoggerUtils.trace(
			  // "mimir.lenses.BestGuessCache"
				// "mimir.exec.Compiler"
			){
				update("""
					CREATE LENS RATINGS1FINAL 
					  AS SELECT * FROM RATINGS1 
					  WITH MISSING_VALUE('RATING')
				""")
			}
			val nullRow = querySingleton("SELECT ROWID() FROM RATINGS1 WHERE RATING IS NULL").asLong

			query("""
				SELECT RATING FROM RATINGS1FINAL WHERE RATING < 5
			"""){ _.toSeq must have size(3) }

			queryOneColumn("SELECT PID FROM RATINGS1") { _.toSeq must not contain(NullPrimitive()) }
			queryOneColumn("SELECT PID FROM RATINGS1FINAL") { _.toSeq must not contain(NullPrimitive()) }
		}
		"Show Determinism Correctly" >> {
			update("""
				CREATE LENS PRODUCT_REPAIRED 
				  AS SELECT * FROM PRODUCT
				  WITH MISSING_VALUE('BRAND')
			""")
			
			query("SELECT ID, BRAND FROM PRODUCT_REPAIRED") { result =>
				result.toSeq.map { r => 
					(r("ID").asString, r.isColDeterministic("BRAND")) 
				} must contain(
					("P123", false), 
					("P125", true), 
					("P34235", true)
				)
			}

			query("SELECT ID, BRAND FROM PRODUCT_REPAIRED WHERE BRAND='HP'") { result =>
				result.toSeq.map { r => 
					(r("ID").asString, r.isColDeterministic("BRAND"), r.isDeterministic) 
				} must contain( 
					("P34235", true, true) 
				)
			}
			
		}

		"Create and Query Schema Matching Lenses" >> {
			update("""
				CREATE LENS RATINGS2FINAL 
				  AS SELECT * FROM RATINGS2 
				  WITH SCHEMA_MATCHING('PID string', 'RATING float', 'REVIEW_CT float')
			""")
			query("SELECT RATING FROM RATINGS2FINAL") { result =>
				val result1 = result.toList.map { _(0).asDouble }.toSeq 
				result1 must have size(3)
				result1 must contain(eachOf( 121.0, 5.0, 4.0 ) )
			}
		}

		"Obtain Row Explanations for Simple Queries" >> {
			val expl = 
				LoggerUtils.trace(
						// "mimir.ctables.CTExplainer"
				) {
					explainRow("""
							SELECT * FROM RATINGS2FINAL WHERE RATING > 3
						""", "2")
				}

			expl.toString must contain("I assumed that NUM_RATINGS maps to RATING")		
		}

		
		"Obtain Cell Explanations for Simple Queries" >> {
			val expl1 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "3", "RATING")
			expl1.toString must contain("I used a classifier to guess that RATINGS1FINAL.RATING =")		
		}
		"Obtain Cell Explanations for Queries with WHERE clauses" >> {
			val expl1 = explainCell("""
					SELECT * FROM RATINGS1FINAL WHERE RATING > 0
				""", "3", "RATING")
			expl1.toString must contain("I used a classifier to guess that RATINGS1FINAL.RATING =")		
		}
		"Guard Data-Dependent Explanations for Simple Queries" >> {
			val expl2 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "2", "RATING")
			expl2.toString must not contain("I used a classifier to guess that RATINGS1FINAL.RATING =")		
		}

		"Query a Union of Lenses (projection first)" >> {
			query("""
				SELECT PID FROM RATINGS1FINAL
					UNION ALL
				SELECT PID FROM RATINGS2FINAL
			"""){ result => 
				val result1 = result.toSeq.map { _(0).asString } 
				result1 must have size(7)
				result1 must contain(
					"P123", "P124", "P125", "P325", "P2345",
					"P34234", "P34235"
				)
			}
		}

		"Query a Union of Lenses (projection last)" >> {
			
			query("""
				SELECT PID FROM (
					SELECT * FROM RATINGS1FINAL
						UNION ALL
					SELECT * FROM RATINGS2FINAL
				) allratings
			"""){ result =>
				val result2 =result.toSeq.map { _(0).asString }

				result2 must have size(7)
				result2 must contain(
					"P123", "P124", "P125", "P325", "P2345",
					"P34234", "P34235"
				)
			}
		}

		"Query a Filtered Union of lenses" >> {
			query("""
				SELECT pid FROM (
					SELECT * FROM RATINGS1FINAL
						UNION ALL
					SELECT * FROM RATINGS2FINAL
				) r
				WHERE rating >= 4;
			"""){ 
				_.toSeq.map { _(0).asString } must contain(
					"P123", "P2345", "P125", "P325", "P34234"
				)
			}
		}

		"Query a Join of a Union of Lenses" >> {
		  LoggerUtils.debug(
				// "mimir.exec.Compiler"
			) {
				query("""
					SELECT p.name, r.rating FROM (
						SELECT * FROM RATINGS1FINAL
							UNION ALL
						SELECT * FROM RATINGS2FINAL
					) r, Product p
					WHERE r.pid = p.id;
				"""){ result =>
					val result0 = result.toSeq.map { _(0).asString } 
					result0 must have size(6)
					result0 must contain(
						"Apple 6s, White",
						"Sony to inches",
						"Apple 5s, Black",
						"Samsung Note2",
						"Dell, Intel 4 core",
						"HP, AMD 2 core"
					)
				}
			}
			
			val result0tokens = query("""
				SELECT p.name, r.rating FROM (
					SELECT * FROM RATINGS1FINAL
						UNION ALL
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
			"""){ 
				_.toSeq.map { _.provenance.asString } must contain(
					"4|1|6",
					"3|1|5",
					"3|0|4",
					"2|1|3",
					"4|0|2",
					"2|0|1"
				)
			}
			
			
			
			val explain0 = 
				LoggerUtils.trace(
					// "mimir.ctables.CTExplainer"
				){ 
					explainCell("""
						SELECT p.name, r.rating FROM (
							SELECT * FROM RATINGS1FINAL 
								UNION ALL 
							SELECT * FROM RATINGS2FINAL
						) r, Product p
						""", "3|1|4", "RATING")
				}
			explain0.reasons.map(_.model.name.replaceAll(":.*", "")) must contain(eachOf(
				"RATINGS2FINAL"//,
				//"RATINGS2"
			))

			query("""
				SELECT name FROM (
					SELECT * FROM RATINGS1FINAL
						UNION ALL
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id;
				WHERE rating > 4;
			"""){ result =>
				val result1 = result.toSeq.map { _(0).asString } 
				result1 must have size(6)
				result1 must contain(
					"Apple 6s, White",
					"Sony to inches",
					"Apple 5s, Black",
					"Samsung Note2",
					"Dell, Intel 4 core",
					"HP, AMD 2 core"
				)
			}

			query("""
				SELECT name FROM (
					SELECT * FROM RATINGS1FINAL
						UNION ALL
					SELECT * FROM RATINGS2FINAL
				) r, Product p
				WHERE r.pid = p.id
				  AND rating >= 4;
			"""){ result =>
				val result2 = result.toSeq.map { _(0).asString } 
				result2 must have size(6)
				result2 must contain(
					"Apple 6s, White",
					"Samsung Note2",
					"Dell, Intel 4 core",
					"Sony to inches"
				)
			}
		}
	}
}
