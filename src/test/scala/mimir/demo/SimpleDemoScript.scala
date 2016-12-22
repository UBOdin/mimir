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
	val inventoryDataFile = new File("test/data/Product_Inventory.sql")
	val reviewDataFiles = List(
			new File("test/data/ratings1.csv"),
			new File("test/data/ratings2.csv"),
			new File("test/data/ratings3.csv"),
			new File("test/data/userTypes.csv"),
			new File("test/data/cureSource.csv"),
			new File("test/data/cureLocations.csv")
		)

	"The Basic Demo" should {
		"Be able to open the database" >> {
			db // force the DB to be loaded
			dbFile must beAFile
		}

		"Run the Load Product Data Script" >> {
			stmts(productDataFile).map( update(_) )
			db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)

			stmts(inventoryDataFile).map( update(_) )
			db.backend.resultRows("SELECT * FROM PRODUCT_INVENTORY;") must have size(6)
		}

		"Load CSV Files" >> {
			db.loadTable(reviewDataFiles(0))
			db.loadTable(reviewDataFiles(1))
			db.loadTable(reviewDataFiles(2))
			db.loadTable(reviewDataFiles(3))
			db.loadTable(reviewDataFiles(4))
			db.loadTable(reviewDataFiles(5))
			query("SELECT * FROM RATINGS1;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1_RAW;").allRows.flatten must contain( str("4.5"), str("A3"), str("4.0"), str("6.4") )
			query("SELECT * FROM RATINGS2;").allRows must have size(3)
		}



		"Use Sane Types in Lenses" >> {
			var oper = select("SELECT * FROM RATINGS2")
			Typechecker.typeOf(Var("NUM_RATINGS"), oper) must be oneOf(TInt(), TFloat(), TAny())
		}

    "Create and Query Type Inference Lens with NULL values" >> {
      update("""
				CREATE LENS null_test
				  AS SELECT * FROM RATINGS3
				  WITH MISSING_VALUE('EVALUATION')
 			""")
      query("SELECT * FROM null_test;").allRows must have size(3)

      val results0 =
				LoggerUtils.debug(List(
					// "mimir.exec.Compiler",
					// "mimir.sql.sqlite.MimirCast$"
				), () => {
	      	query("SELECT * FROM RATINGS3;").allRows.toList
		    })
      results0 must have size(3)
      results0(2) must contain(str("P34235"), NullPrimitive(), f(4.0))
    }


		"Create and Query Type Inference Lenses" >> {
			update("""
				CREATE LENS new_types
				  AS SELECT * FROM USERTYPES
				  WITH Type_Inference(.9)
					 			""")
			query("SELECT * FROM new_types;").allRows must have size(3)
			query("SELECT * FROM RATINGS1;").allRows must have size(4)
			query("SELECT RATING FROM RATINGS1;").allRows.flatten must contain(eachOf(f(4.5), f(4.0), f(6.4), NullPrimitive()))
			query("SELECT * FROM RATINGS1 WHERE RATING IS NULL").allRows must have size(1)
			query("SELECT * FROM RATINGS1 WHERE RATING > 4;").allRows must have size(2)
			query("SELECT * FROM RATINGS2;").allRows must have size(3)
			db.bestGuessSchema(select("SELECT * FROM RATINGS2;")).
				map(_._2).map(Typechecker.baseType _) must be equalTo List(TString(), TFloat(), TFloat())
		}

		"Compute Deterministic Aggregate Queries" >> {
			val q0 = query("""
				SELECT SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
			""").allRows.flatten
			q0 must have size(1)
			q0 must contain(i(92))

			val q1 = query("""
				SELECT COMPANY, SUM(QUANTITY)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
			""").allRows.flatten
			q1 must have size(6)
			q1 must contain( str("Apple"), i(9), str("HP"), i(69), str("Sony"), i(14) )

			val q2 = query("""
				SELECT COMPANY, MAX(PRICE)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
										 															""").allRows.flatten
			q2 must have size(6)
			q2 must contain( str("Apple"), f(13.00), str("HP"), f(102.74), str("Sony"), f(38.74) )

			val q3 = query("""
				SELECT COMPANY, AVG(PRICE)
				FROM PRODUCT_INVENTORY
				GROUP BY COMPANY;
										 															""").allRows.flatten

			q3 must have size(6)
			q3 must contain( str("Apple"), f(12.5), str("HP"), f(64.41333333333334), str("Sony"), f(38.74) )

			val q4 = query("""SELECT COMPANY, MIN(QUANTITY)FROM PRODUCT_INVENTORY GROUP BY COMPANY;""").allRows.flatten
			q4 must have size(6)
			q4 must contain( str("Apple"), i(4), str("HP"), i(9), str("Sony"), i(14) )

			val q5 = query("""
				SELECT COUNT(*)
				FROM PRODUCT_INVENTORY;
			""").allRows.flatten
			q5 must have size(1)
			q5 must contain( i(6) )

			val q6 = query("""
				SELECT COUNT(DISTINCT COMPANY)
				FROM PRODUCT_INVENTORY;
			""").allRows.flatten
			q6 must have size(1)
			q6 must contain( i(3) )

			val q7a = query("""
				SELECT COUNT(*)
				FROM PRODUCT_INVENTORY
				WHERE COMPANY = 'Apple';
			""").allRows.flatten
			q7a must have size(1)
			q7a must contain( i(2) )

			val q7b = query("""
				SELECT COUNT(DISTINCT COMPANY)
				FROM PRODUCT_INVENTORY
				WHERE COMPANY = 'Apple';
			""").allRows.flatten
			q7b must have size(1)
			q7b must contain( i(1) )

			val q8 = query("""
				SELECT P.COMPANY, P.QUANTITY, P.PRICE
				FROM (SELECT COMPANY, MAX(PRICE) AS COST
					FROM PRODUCT_INVENTORY
					GROUP BY COMPANY)subq, PRODUCT_INVENTORY P
				WHERE subq.COMPANY = P.COMPANY AND subq.COST = P.PRICE;
										 										 										 										 															""").allRows.flatten
			q8 must have size(9)
			q8 must contain( str("Apple"), i(5), f(13.00), str("HP"), i(37), f(102.74), str("Sony"), i(14), f(38.74) )

			val q9 = query("""
				SELECT P.COMPANY, P.PRICE
				FROM (SELECT AVG(PRICE) AS A FROM PRODUCT_INVENTORY)subq, PRODUCT_INVENTORY P
				WHERE PRICE > subq.A;
										 										 										 										 										 															""").allRows.flatten
			q9 must have size(4)
			q9 must contain( str("HP"), f(65.00), str("HP"), f(102.74) )

			val q10 = query("""
				SELECT MIN(subq2.B)
				FROM (SELECT P.PRICE AS B FROM (SELECT AVG(QUANTITY) AS A FROM PRODUCT_INVENTORY)subq, PRODUCT_INVENTORY P
				WHERE P.QUANTITY > subq.A)subq2;
																										""").allRows.flatten
			q10 must have size(1)
			q10 must contain( f(65.00) )
		}

		"Create and Query Domain Constraint Repair Lenses" >> {
			LoggerUtils.trace(List(
				 // "mimir.lenses.BestGuessCache"
				// "mimir.exec.Compiler"
			), () => {
			update("""
				CREATE LENS RATINGS1FINAL 
				  AS SELECT * FROM RATINGS1 
				  WITH MISSING_VALUE('RATING')
			""")
			})
			val nullRow = query("SELECT ROWID FROM RATINGS1 WHERE RATING IS NULL").
											allRows.head(0).asLong

			val result1guesses =
				db.backend.resultRows("SELECT MIMIR_KEY_0, MIMIR_DATA FROM "+
						db.bestGuessCache.cacheTableForModel(db.models.getModel("RATINGS1FINAL:WEKA:RATING"), 0))

			result1guesses.map( x => (x(0), x(1))).toList must contain((IntPrimitive(nullRow), FloatPrimitive(4.5)))

			val result1 =
				LoggerUtils.debug(List(
						// "mimir.exec.Compiler"
					),() => query("SELECT RATING FROM RATINGS1FINAL").allRows.flatten
				)

			result1 must have size(4)
			result1 must contain(eachOf( f(4.5), f(4.0), f(4.5), f(6.4) ) )
			val result2 = query("SELECT RATING FROM RATINGS1FINAL WHERE RATING < 5").allRows.flatten
			result2 must have size(3)

			queryOneColumn("SELECT PID FROM RATINGS1") must not contain(NullPrimitive())
			queryOneColumn("SELECT PID FROM RATINGS1FINAL") must not contain(NullPrimitive())
		}
		"Show Determinism Correctly" >> {
			update("""
				CREATE LENS PRODUCT_REPAIRED 
				  AS SELECT * FROM PRODUCT
				  WITH MISSING_VALUE('BRAND')
			""")
			val result1 = query("SELECT ID, BRAND FROM PRODUCT_REPAIRED")
			val result1Determinism = result1.mapRows( r => (r(0).asString, r.deterministicCol(1)) )
			result1Determinism must contain(eachOf( ("P123", false), ("P125", true), ("P34235", true) ))

			val result2 = query("SELECT ID, BRAND FROM PRODUCT_REPAIRED WHERE BRAND='HP'")
			val result2Determinism = result2.mapRows( r => (r(0).asString, r.deterministicCol(1), r.deterministicRow) )
			result2Determinism must contain(eachOf( ("P34235", true, true) ))
		}

		"Create and Query Schema Matching Lenses" >> {
			update("""
				CREATE LENS RATINGS2FINAL 
				  AS SELECT * FROM RATINGS2 
				  WITH SCHEMA_MATCHING('PID string', 'RATING float', 'REVIEW_CT float')
			""")
			val result1 = query("SELECT RATING FROM RATINGS2FINAL").allRows.flatten
			result1 must have size(3)
			result1 must contain(eachOf( f(121.0), f(5.0), f(4.0) ) )
		}

		"Obtain Row Explanations for Simple Queries" >> {
			val expl = 
				LoggerUtils.trace(List(
						// "mimir.ctables.CTExplainer"
					), () => {
						explainRow("""
								SELECT * FROM RATINGS2FINAL WHERE RATING > 3
							""", "1")
					})

			expl.toString must contain("I assumed that NUM_RATINGS maps to RATING")		
		}

		"Obtain Cell Explanations for Simple Queries" >> {
			val expl1 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "2", "RATING")
			expl1.toString must contain("I used a classifier to guess that RATING=")		
		}
		"Obtain Cell Explanations for Queries with WHERE clauses" >> {
			val expl1 = explainCell("""
					SELECT * FROM RATINGS1FINAL WHERE RATING > 0
				""", "2", "RATING")
			expl1.toString must contain("I used a classifier to guess that RATING=")		
		}
		"Guard Data-Dependent Explanations for Simple Queries" >> {
			val expl2 = explainCell("""
					SELECT * FROM RATINGS1FINAL
				""", "1", "RATING")
			expl2.toString must not contain("I used a classifier to guess that RATING=")		
		}

		"Query a Union of Lenses (projection first)" >> {
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
		}

		"Query a Union of Lenses (projection last)" >> {
			val result2 =
			// LoggerUtils.debug("mimir.lenses.BestGuessCache", () => {
			// LoggerUtils.debug("mimir.algebra.ExpressionChecker", () => {
				query("""
					SELECT PID FROM (
						SELECT * FROM RATINGS1FINAL
							UNION ALL
						SELECT * FROM RATINGS2FINAL
					) allratings
				""").allRows.flatten
			// })
			// })
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
			result must have size(5)
			result must contain(eachOf(
				str("P123"), str("P2345"), str("P125"), str("P325"), str("P34234")
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

			val explain0 = 
				LoggerUtils.trace(List(
					// "mimir.ctables.CTExplainer"
				), () => 
					explainCell("""
						SELECT p.name, r.rating FROM (
							SELECT * FROM RATINGS1FINAL 
								UNION ALL 
							SELECT * FROM RATINGS2FINAL
						) r, Product p
						""", "1|right|3", "RATING")
				)
			explain0.reasons.map(_.model.replaceAll(":.*", "")) must contain(eachOf(
				"RATINGS2FINAL",
				"RATINGS2"
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
			result2 must have size(4)
			result2 must contain(eachOf(
				str("Apple 6s, White"),
				str("Samsung Note2"),
				str("Dell, Intel 4 core"),
				str("Sony to inches")
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
			q3compiled must beAnInstanceOf[NonDetIterator]

			//Test another level down the heirarchy too
			val q3dbquery = q3compiled.asInstanceOf[NonDetIterator].src
			q3dbquery must beAnInstanceOf[ResultSetIterator]

			// Again, the internal schema must explicitly state that the column is a rowid
			q3dbquery.asInstanceOf[ResultSetIterator].visibleSchema must havePair ( "MIMIR_ROWID_0" -> TRowId() )
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

		"CURE tests" >> {

			var startQ1:Long = System.nanoTime();
			val result1 = query("""SELECT * FROM cureSource;""").allRows().foreach((x) => {})
			var endQ1:Long = System.nanoTime();
			println("Query over normal Type Inference Took: "+((endQ1 - startQ1)/1000000) + " MILLISECONDS")

/*
			var schema:String = ""
			db.getTableSchema("userTypeTime") match {
				case Some(x) => x.map((te) => {
					schema += "'" + te._1 + "',"
				})
				case None => throw new SQLException("Table does not exist in db!")
			}

			schema = schema.substring(0,schema.length-1)
			//			println(schema)
*/

			update("""CREATE LENS MV1 as SELECT * FROM cureSource WITH MISSING_VALUE('IMO_CODE');""")
			update("""CREATE LENS MV2 as SELECT * FROM cureLocations WITH MISSING_VALUE('IMO_CODE');""")

			val q = """SELECT * FROM MV1 AS source JOIN MV2 AS locations ON source.IMO_CODE = locations.IMO_CODE;"""

			var startQ2:Long = System.nanoTime();
			val result2 = query(q).allRows().foreach((x) => {})
			var endQ2:Long = System.nanoTime();
			println("Cure Query Took: " +((endQ2 - startQ2)/1000000) + " MILLISECONDS")

			true
		}
	}
}
/* CREATE LENS mvTest as SELECT * FROM userTypeTime WITH MISSING_VALUE('DATE','MONTH','CONSIGNEE_DECLARED','CONSIGNEE_DECLARED_ADDRESS','SHIPPER_DECLARED','SHIPPER_ADDRESS','NOTIFY_NAME','NOTIFY_ADDRESS','CARRIER_CODE','CARRIER','BILL_MASTER_CARRIER','MASTER_CONSIGNEE_UNIFIED','MASTER_SHIPPER','MASTER_NOTIFY','CONSIGNEE_UNIFIED','SHIPPER_UNIFIED','CONSIGNEE_S_STATE','CONSIGNEE_S_CITY','CONSIGNEE_S_ZIP_CODE','BILL_OF_LADING_NBR','MASTER_HOUSE','MODE_OF_TRANSPORT','ESTIMATED_DATE','IN_BOND_ENTRY_TYPE','SHORT_CONTAINER_DESCRIPTION','IMO_CODE','HIGH_CUBE','BILL_MASTER','HS_1','HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE','PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY','FINAL_DESTINATION','COUNTRY_OF_ORIGIN','WORLD_REGION_BY_COUNTRY_OF_ORIGIN','PLACE_OF_RECEIPT','COUNTRY_BY_PLACE_OF_RECEIPT','WORLD_REGION_BY_PLACE_OF_RECEIPT','QUANTITY','QUANTITY_UNIT','WEIGHT','WEIGHT_UNIT','MEASURE','MEASURE_UNIT','CONTAINER_QUANTITY','METRIC_TONS','CONTAINER_1','PIECES_1','DESCRIPTION_1','HARMONIZED_1','MARKS_NUMBERS_1','HS_2','CONTAINER_2','PIECES_2','DESCRIPTION_2','HARMONIZED_2','MARKS_NUMBERS_2','HS_3','CONTAINER_3','PIECES_3','DESCRIPTION_3','HARMONIZED_3','MARKS_NUMBERS_3','HS_4','CONTAINER_4','PIECES_4','DESCRIPTION_4','HARMONIZED_4','MARKS_NUMBERS_4','HS_5','CONTAINER_5','PIECES_5','DESCRIPTION_5','HARMONIZED_5','MARKS_NUMBERS_5');
*/
// CREATE LENS mvTest2 as SELECT * FROM userTypeTime WITH MISSING_VALUE('DATE','MONTH','CONSIGNEE_DECLARED','CONSIGNEE_DECLARED_ADDRESS','SHIPPER_DECLARED','SHIPPER_ADDRESS','NOTIFY_NAME','NOTIFY_ADDRESS','CARRIER_CODE','CARRIER','BILL_MASTER_CARRIER','MASTER_CONSIGNEE_UNIFIED','MASTER_SHIPPER','MASTER_NOTIFY','CONSIGNEE_UNIFIED','SHIPPER_UNIFIED','CONSIGNEE_S_STATE','CONSIGNEE_S_CITY','CONSIGNEE_S_ZIP_CODE','BILL_OF_LADING_NBR','MASTER_HOUSE','MODE_OF_TRANSPORT','ESTIMATED_DATE','IN_BOND_ENTRY_TYPE','SHORT_CONTAINER_DESCRIPTION','IMO_CODE','HIGH_CUBE','BILL_MASTER','HS_1','HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE','PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY','FINAL_DESTINATION','COUNTRY_OF_ORIGIN','WORLD_REGION_BY_COUNTRY_OF_ORIGIN','PLACE_OF_RECEIPT','COUNTRY_BY_PLACE_OF_RECEIPT','WORLD_REGION_BY_PLACE_OF_RECEIPT','QUANTITY','QUANTITY_UNIT','WEIGHT','WEIGHT_UNIT','MEASURE','MEASURE_UNIT','CONTAINER_QUANTITY','METRIC_TONS','CONTAINER_1','PIECES_1','DESCRIPTION_1','HARMONIZED_1','MARKS_NUMBERS_1','HS_2','CONTAINER_2','PIECES_2','DESCRIPTION_2','HARMONIZED_2','MARKS_NUMBERS_2','HS_3','CONTAINER_3');
// fails
// CREATE LENS MV4 as SELECT * FROM userTypeTime WITH MISSING_VALUE('DATE','MONTH','CONSIGNEE_DECLARED','CONSIGNEE_DECLARED_ADDRESS','SHIPPER_DECLARED','SHIPPER_ADDRESS','NOTIFY_NAME','NOTIFY_ADDRESS','CARRIER_CODE','CARRIER','BILL_MASTER_CARRIER','MASTER_CONSIGNEE_UNIFIED','MASTER_SHIPPER','MASTER_NOTIFY','CONSIGNEE_UNIFIED','SHIPPER_UNIFIED','CONSIGNEE_S_STATE','CONSIGNEE_S_CITY','CONSIGNEE_S_ZIP_CODE','BILL_OF_LADING_NBR');
// passes
// CREATE LENS MV5 as SELECT * FROM userTypeTime WITH MISSING_VALUE('MASTER_HOUSE','MODE_OF_TRANSPORT','ESTIMATED_DATE','IN_BOND_ENTRY_TYPE','SHORT_CONTAINER_DESCRIPTION','IMO_CODE','HIGH_CUBE','BILL_MASTER','HS_1','HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE','PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY','FINAL_DESTINATION','COUNTRY_OF_ORIGIN','WORLD_REGION_BY_COUNTRY_OF_ORIGIN','PLACE_OF_RECEIPT','COUNTRY_BY_PLACE_OF_RECEIPT','WORLD_REGION_BY_PLACE_OF_RECEIPT','QUANTITY','QUANTITY_UNIT','WEIGHT','WEIGHT_UNIT','MEASURE','MEASURE_UNIT','CONTAINER_QUANTITY','METRIC_TONS','CONTAINER_1','PIECES_1','DESCRIPTION_1','HARMONIZED_1','MARKS_NUMBERS_1','HS_2','CONTAINER_2','PIECES_2','DESCRIPTION_2','HARMONIZED_2','MARKS_NUMBERS_2','HS_3','CONTAINER_3');
// fails
// CREATE LENS MV6 as SELECT * FROM userTypeTime WITH MISSING_VALUE('MASTER_HOUSE','MODE_OF_TRANSPORT','ESTIMATED_DATE','IN_BOND_ENTRY_TYPE','SHORT_CONTAINER_DESCRIPTION','IMO_CODE','HIGH_CUBE','BILL_MASTER','HS_1','HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE','PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY');
// fails
// CREATE LENS MV7 as SELECT * FROM userTypeTime WITH MISSING_VALUE('FINAL_DESTINATION','COUNTRY_OF_ORIGIN','WORLD_REGION_BY_COUNTRY_OF_ORIGIN','PLACE_OF_RECEIPT','COUNTRY_BY_PLACE_OF_RECEIPT','WORLD_REGION_BY_PLACE_OF_RECEIPT','QUANTITY','QUANTITY_UNIT','WEIGHT','WEIGHT_UNIT','MEASURE','MEASURE_UNIT','CONTAINER_QUANTITY','METRIC_TONS','CONTAINER_1','PIECES_1','DESCRIPTION_1','HARMONIZED_1','MARKS_NUMBERS_1','HS_2','CONTAINER_2','PIECES_2','DESCRIPTION_2','HARMONIZED_2','MARKS_NUMBERS_2','HS_3','CONTAINER_3');
// passes
// CREATE LENS MV8 as SELECT * FROM userTypeTime WITH MISSING_VALUE('MASTER_HOUSE','MODE_OF_TRANSPORT','ESTIMATED_DATE','IN_BOND_ENTRY_TYPE','SHORT_CONTAINER_DESCRIPTION','IMO_CODE','HIGH_CUBE','BILL_MASTER','HS_1');
// passes
// CREATE LENS MV9 as SELECT * FROM userTypeTime WITH MISSING_VALUE('HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE','PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY');
// fails
// CREATE LENS MV10 as SELECT * FROM userTypeTime WITH MISSING_VALUE('HS_DESCRIPTION','FOREIGN_DESTINATION','US_REGION','WORLD_REGION_BY_PORT_OF_DEPARTURE','COUNTRY_BY_PORT_OF_DEPARTURE');
// passes
// CREATE LENS MV11 as SELECT * FROM userTypeTime WITH MISSING_VALUE('PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL','VESSEL','VESSEL_COUNTRY');
// fails
// CREATE LENS MV12 as SELECT * FROM userTypeTime WITH MISSING_VALUE('PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT','PORT_OF_ARRIVAL');
// fails
// CREATE LENS MV13 as SELECT * FROM userTypeTime WITH MISSING_VALUE('VESSEL','VESSEL_COUNTRY');
// passes
// CREATE LENS MV14 as SELECT * FROM userTypeTime WITH MISSING_VALUE('PORT_OF_DEPARTURE','STATE_OF_ARRIVAL_PORT');
// fails
// CREATE LENS MV15 as SELECT * FROM userTypeTime WITH MISSING_VALUE('PORT_OF_DEPARTURE');
// passes
// CREATE LENS MV16 as SELECT * FROM userTypeTime WITH MISSING_VALUE('STATE_OF_ARRIVAL_PORT');
// fails! Is a completely null column


// CREATE LENS MV3 as SELECT * FROM userTypeTime WITH MISSING_VALUE('PIECES_3','DESCRIPTION_3','HARMONIZED_3','MARKS_NUMBERS_3','HS_4','CONTAINER_4','PIECES_4','DESCRIPTION_4','HARMONIZED_4','MARKS_NUMBERS_4','HS_5','CONTAINER_5','PIECES_5','DESCRIPTION_5','HARMONIZED_5','MARKS_NUMBERS_5');
// passes