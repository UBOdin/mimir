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
      """){ 
        _.toSeq must haveSize(1)
      }
      query("""
        SELECT ATTR_NODE FROM MIMIR_DA_SCH_SHIPPING
        WHERE ATTR_NAME = 'ROOT'
          AND ATTR_NODE >= 0
      """){
        _.toSeq must haveSize(0)
      }

      val spanningTree = 
        DiscalaAbadiNormalizer.spanningTreeLens(db, 
          MultilensConfig("SHIPPING", db.getTableOperator("SHIPPING"), Seq())
        )
      LoggerUtils.debug(
          // "mimir.exec.Compiler"
      ){
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
        ){ _.map { _("TABLE_NODE") }.toSeq must not contain(IntPrimitive(-1)) }
      }

    }

    "Create a schema that can be queried" >> {
      db.query(
        OperatorUtils.projectDownToColumns(
          Seq("TABLE_NAME", "SCHEMA_NAME"),
          OperatorUtils.makeUnion(
            db.adaptiveSchemas.tableCatalogs
          )
        )
      ) { _.map { row => 
          (
            row("TABLE_NAME").asString, 
            row("SCHEMA_NAME").asString,
            row.isDeterministic
          )
        } must contain( 
          ("ROOT", "SHIPPING", true),
          ("CONTAINER_1", "SHIPPING", false)
        )
      }

      db.query(
        Sort(Seq(SortColumn(Var("TABLE_NAME"), true), SortColumn(Var("IS_KEY"), false)),
          OperatorUtils.projectDownToColumns(
            Seq("TABLE_NAME", "ATTR_NAME", "IS_KEY"),
            OperatorUtils.makeUnion(
              db.adaptiveSchemas.attrCatalogs
            )
          )
        )
      ){ results =>
        val attrs = results.map { row => 
          (
            row("TABLE_NAME").asString, 
            row("ATTR_NAME").asString,
            row("IS_KEY").asInstanceOf[BoolPrimitive].v
          )
        }.toSeq 
        attrs must contain( eachOf( 
          ("ROOT","MONTH",false),
          ("BILL_OF_LADING_NBR","QUANTITY",false)
        ) )
        attrs.map( row => (row._1, row._2) ) must not contain( ("ROOT", "ROOT") )
      }
    }

    "Allocate all attributes to some relation" >> {
      db.query(
        Sort(Seq(SortColumn(Var("TABLE_NAME"), true), SortColumn(Var("IS_KEY"), false)),
          OperatorUtils.projectDownToColumns(
            Seq("TABLE_NAME", "ATTR_NAME", "IS_KEY"),
            OperatorUtils.makeUnion(
              db.adaptiveSchemas.attrCatalogs
            )
          )
        )
      ){ _.map { row => 
          row("ATTR_NAME").asString
        }.toSet must be equalTo(
          db.getTableOperator("SHIPPING").schema.map(_._1).toSet
        )
      }
    }

    "Allow native SQL queries over the catalog tables" >> {
      LoggerUtils.debug(
        // "mimir.exec.Compiler"
      ) {
        query("""
          SELECT TABLE_NAME, SCHEMA_NAME FROM MIMIR_SYS_TABLES
        """){ results =>
          val tables = results.map { row => (row("TABLE_NAME").asString, row("SCHEMA_NAME").asString) }.toSeq 

          tables must contain( ("ROOT", "SHIPPING") )
          tables must contain( ("MIMIR_VIEWS", "BACKEND") )
          tables must contain( ("SHIPPING", "BACKEND") )
        }
      } 


      
      query("""
        SELECT TABLE_NAME, ATTR_NAME FROM MIMIR_SYS_ATTRS
      """) { results =>
        val attrs = results.map { row => (row("TABLE_NAME").asString, row("ATTR_NAME").asString) }.toSeq 
        attrs must contain( ("ROOT", "MONTH") )
        attrs must contain( ("BILL_OF_LADING_NBR", "QUANTITY") )
      }

      LoggerUtils.debug(
        // "mimir.exec.Compiler"
      ) {
        query("""
          SELECT ATTR_NAME FROM MIMIR_SYS_ATTRS
          WHERE SCHEMA_NAME = 'SHIPPING'
            AND TABLE_NAME = 'ROOT'
        """) { results =>
          val attrStrings = results.map { row => (row("ATTR_NAME").asString, row.isDeterministic) }.toSeq 
          attrStrings must contain(
            ("FOREIGN_DESTINATION", false)
          )
        }
      }
    }

    "Be introspectable" >> {
      val baseQuery = """
          SELECT ATTR_NAME, ROWID() AS ID FROM MIMIR_SYS_ATTRS
          WHERE SCHEMA_NAME = 'SHIPPING'
            AND TABLE_NAME = 'BILL_OF_LADING_NBR'
        """

      
      LoggerUtils.debug(
        // "mimir.exec.Compiler"
      ){
        query(baseQuery){ results =>
          val attrStrings = results.map { row => 
            (
              row("ATTR_NAME").asString, 
              row("ID").asString, 
              row.isDeterministic
            ) 
          }.toSeq 
          attrStrings must contain(
            ("QUANTITY","47|21|45|0|1|1",false)
          )
        }
      }

      val explanation =
        explainRow(baseQuery, "47|21|45|0|1|1")

      explanation.reasons.map(_.reason).head must contain("QUANTITY could be organized under any of BILL_OF_LADING_NBR")
    }


    "Create queriable relations" >> {
      queryOneColumn("""
        SELECT QUANTITY FROM SHIPPING.BILL_OF_LADING_NBR"""
      ){ _.toSeq must contain(StringPrimitive("1")) }
    }

  }


}