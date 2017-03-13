package mimir.adaptive

import java.io._

import org.specs2.specification._
import org.specs2.mutable._

import mimir.algebra._
import mimir.test._
import mimir.util._

object DiscalaAbadiSpec
  extends SQLTestSpecification("DiscalaAbadi")
  with BeforeAll
{
  def beforeAll =
  {
    loadCSV("SHIPPING", new File("test/data/cureSource.csv"))
  }

  sequential

  "The Discala-Abadi Normalizer" should {

    "Support schema creation" >> {
      update("""
        CREATE ADAPTIVE SCHEMA SHIPPING
          AS SELECT * FROM SHIPPING
          WITH DISCALA_ABADI()
      """)

      querySingleton("""
        SELECT COUNT(*) FROM MIMIR_DA_FDG_SHIPPING
      """).asLong must be greaterThan(20l)
      querySingleton("""
        SELECT NAME FROM MIMIR_ADAPTIVE_SCHEMAS
      """) must be equalTo(StringPrimitive("SHIPPING"))
      querySingleton("""
        SELECT NAME FROM MIMIR_MODELS
      """) must be equalTo(StringPrimitive("MIMIR_DA_CHOSEN_SHIPPING:MIMIR_FD_PARENT"))
    }

    "Create a sane root attribute" >> {
      query("""
        SELECT ATTR_NODE FROM MIMIR_DA_SCH_SHIPPING
        WHERE ATTR_NAME = 'ROOT'
      """).allRows.toSeq must haveSize(1)
      query("""
        SELECT ATTR_NODE FROM MIMIR_DA_SCH_SHIPPING
        WHERE ATTR_NAME = 'ROOT'
          AND ATTR_NODE >= 0
      """).allRows.toSeq must haveSize(0)

      val spanningTree = 
        DiscalaAbadiNormalizer.spanningTreeLens(db, 
          MultilensConfig("SHIPPING", db.getTableOperator("SHIPPING"), Seq())
        )
      LoggerUtils.debug(
        List(
          // "mimir.exec.Compiler"
        ), () =>
        db.query(
          Project(Seq(ProjectArg("TABLE_NODE", Var("TABLE_NODE"))),
            Select(Comparison(Cmp.Gt, Arithmetic(Arith.Add, Var("TABLE_NODE"), IntPrimitive(1)), IntPrimitive(0)),
              OperatorUtils.makeDistinct(
                Project(Seq(ProjectArg("TABLE_NODE", Var("MIMIR_FD_PARENT"))),
                  spanningTree
                )
              )
            )
          )
        ).allRows.flatten
      ) must not contain(IntPrimitive(-1))

    }

    "Create a schema that can be queried" >> {
      val tables = 
        db.query(
          OperatorUtils.projectDownToColumns(
            Seq("TABLE_NAME", "SOURCE"),
            OperatorUtils.makeUnion(
              db.adaptiveSchemas.tableCatalogs
            )
          )
        ).mapRows { row => 
          (row(0).asString, row(1).asString, row.deterministicRow)
        }
      tables must contain( eachOf( 
        ("ROOT", "SHIPPING", true),
        ("CONTAINER_1", "SHIPPING", false)
      ))

      val attrs =
        db.query(
          Sort(Seq(SortColumn(Var("TABLE_NAME"), true), SortColumn(Var("IS_KEY"), false)),
            OperatorUtils.projectDownToColumns(
              Seq("TABLE_NAME", "ATTR_NAME", "IS_KEY"),
              OperatorUtils.makeUnion(
                db.adaptiveSchemas.attrCatalogs
              )
            )
          )
        ).mapRows { row => 
          (row(0).asString, row(1).asString, row(2).asInstanceOf[BoolPrimitive].v)
        } 
      attrs must contain( eachOf( 
        ("ROOT","MONTH",false),
        ("BILL_OF_LADING_NBR","QUANTITY",false)
      ) )
      attrs.map( row => (row._1, row._2) ) must not contain( ("ROOT", "ROOT") )
    }

    "Allocate all attributes to some relation" >> {
      skipped("Will, you need to look at this")
      val attrs =
        db.query(
          Sort(Seq(SortColumn(Var("TABLE_NAME"), true), SortColumn(Var("IS_KEY"), false)),
            OperatorUtils.projectDownToColumns(
              Seq("TABLE_NAME", "ATTR_NAME", "IS_KEY"),
              OperatorUtils.makeUnion(
                db.adaptiveSchemas.attrCatalogs
              )
            )
          )
        ).mapRows { row => 
          (row(1).asString)
        } 
      attrs.toSet must be equalTo(
        db.getTableOperator("SHIPPING").schema.map(_._1).toSet
      )
    }

    "Allow native SQL queries over the catalog tables" >> {
      val tables =
        LoggerUtils.debug(
          List(
            // "mimir.exec.Compiler"
          ), () => {
            query("""
              SELECT TABLE_NAME, SOURCE FROM MIMIR_SYS_TABLES
            """).mapRows { row => (row(0), row(1)) }
        }) 

      tables must contain( (StringPrimitive("ROOT"), StringPrimitive("SHIPPING")) )
      tables must contain( (StringPrimitive("MIMIR_VIEWS"), StringPrimitive("RAW")) )
      tables must contain( (StringPrimitive("SHIPPING"), StringPrimitive("RAW")) )

      val attrs = 
        query("""
          SELECT TABLE_NAME, ATTR_NAME FROM MIMIR_SYS_ATTRS
        """).mapRows { row => (row(0), row(1)) }

      attrs must contain( (StringPrimitive("ROOT"), StringPrimitive("MONTH")) )
      attrs must contain( (StringPrimitive("BILL_OF_LADING_NBR"), StringPrimitive("QUANTITY")) )

      val attrStrings = 
        LoggerUtils.debug(
          List(
            // "mimir.exec.Compiler"
          ), () => {
            query("""
              SELECT ATTR_NAME FROM MIMIR_SYS_ATTRS
              WHERE SOURCE = 'SHIPPING'
                AND TABLE_NAME = 'ROOT'
            """).mapRows( _.rowString )
        })
      attrStrings must contain(
        "'FOREIGN_DESTINATION' (This row may be invalid)"
      )
    }

    "Be introspectable" >> {
      val baseQuery = """
          SELECT ATTR_NAME, ROWID() FROM MIMIR_SYS_ATTRS
          WHERE SOURCE = 'SHIPPING'
            AND TABLE_NAME = 'BILL_OF_LADING_NBR'
        """

      val attrStrings = 
        LoggerUtils.debug(
          List(
            // "mimir.exec.Compiler"
          ), () => {
            query(baseQuery).mapRows( _.rowString )
        })
      attrStrings must contain(
        "'QUANTITY','47|21|45|left|right|right' (This row may be invalid)"
      )

      val explanation =
        explainRow(baseQuery, "47|21|45|left|right|right")

      explanation.reasons.map(_.reason).head must contain("there were 2 options for MIMIR_FD_PARENT")
    }


    "Create queriable relations" >> {
      query("""
        SELECT QUANTITY FROM SHIPPING.BILL_OF_LADING_NBR"""
      ).mapRows( row => row(0) ) must contain(StringPrimitive("1"))
    }

  }


}