package mimir.ctables;

import java.io.{StringReader,FileReader}

import org.specs2.mutable._

import mimir._
import mimir.ctables._
import mimir.ctables.vgterm._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.optimizer._
import mimir.exec._
import mimir.provenance._
import mimir.models._
import mimir.test._

object CTPercolatorSpec 
  extends Specification 
  with RAParsers
{
  
  val schema = Map[String,Seq[(ID,Type)]](
    ("R", Seq( 
      ID("A") -> TInt(),
      ID("B") -> TInt()
    )),
    ("S", Seq( 
      ID("C") -> TInt(),
      ID("D") -> TFloat()
    ))
  )

  def table(name: String): Operator =
    Table(ID(name), ID(name), schema(name), Seq())

  def modelLookup(model: ID) = UniformDistribution
  def schemaLookup(table: String) = schema(table).toList
  def ack(
    idx: Int = 1, 
    args: Seq[Expression] = Seq(RowIdVar())
  ): Expression = IsAcknowledged(UniformDistribution, idx, args)

  def project(cols: List[(String,String)], src: Operator): Operator =
    Project(cols.map( { case (name,e) => ProjectArg(ID(name), expr(e))}), src) 

  def percolite(x:Operator) = 
    CTPercolator.percolateLite(x, modelLookup(_))

  "The Percolator (Lite)" should {

    "Handle Base Relations" in {
      percolite(
        table("R")
      ) must be equalTo ((
        table("R"),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("true")
        ),
        expr("true")
      ))
    }

    "Handle Deterministic Projection" in {
      percolite(
        table("R")
          .project("A")
      ) must be equalTo ((
        table("R")
          .project("A"),
        Map( 
          ID("A") -> expr("true")
        ),
        expr("true")
      ))
    }

    "Handle Data-Independent Non-Deterministic Projection" in {
      percolite(
        table("R")
          .map( 
            "A" -> Var(ID("A")), 
            "B" -> VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq())
          )
      ) must be equalTo ((
        table("R")
          .map( 
            "A" -> Var(ID("A")), 
            "B" -> VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq())
          ),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> ack()
        ),
        expr("true")
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection 1" in {
      percolite(
        table("R")
          .map(
            "A" -> Var(ID("A")),
            "B" -> Conditional(IsNullExpression(Var(ID("B"))), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var(ID("B")))
          )
      ) must be equalTo ((
        table("R")
          .map(
            "A" -> Var(ID("A")),
            "B" -> Conditional(IsNullExpression(Var(ID("B"))), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var(ID("B"))),
            "MIMIR_COL_DET_B"
                -> Conditional(IsNullExpression(Var(ID("B"))), ack(), BoolPrimitive(true))
          ),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("MIMIR_COL_DET_B")
        ),
        expr("true")
      ))
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite(
        table("R")
          .filterParsed("{{X_1[ROWID]}} = 3")
      ) must be equalTo ((
        table("R")
          .filterParsed("{{X_1[ROWID]}} = 3"),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("true")
        ),
        ack()
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection 2" in {
      percolite(
        table("R")
          .map(
            "A" -> expr("A"),
            "B" -> expr("IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END")
          )
          .filterParsed("B = 3")
      ) must be equalTo ((
        table("R")
          .map(
            "A" -> expr("A"),
            "B" -> expr("IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END"),
            "MIMIR_COL_DET_B"
                -> Var(ID("B")).isNull.thenElse(ack()) (BoolPrimitive(true))
          )
          .filterParsed("B = 3")
          .mapParsed(
            "A" -> "A",
            "B" -> "B",
            "MIMIR_COL_DET_B" -> "MIMIR_COL_DET_B",
            "MIMIR_ROW_DET" -> "MIMIR_COL_DET_B"
          ),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("MIMIR_COL_DET_B")
        ),
        expr("MIMIR_ROW_DET")
      ))
    }
    "Handle Deterministic Joins" in {
      percolite(
        table("R").join(table("S"))
      ) must be equalTo ((
        table("R").join(table("S")),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("true"),
          ID("C") -> expr("true"),
          ID("D") -> expr("true")
        ),
        expr("true")
      ))
    }
    "Handle Non-Deterministic Joins" in {
      percolite(
        table("R")
          .mapParsed( "A" -> "{{X_1[ROWID,A]}}" )
          .join(table("S"))
      ) must be equalTo ((
        table("R")
          .map( 
            "A" -> expr("{{X_1[ROWID,A]}}"),
            "MIMIR_COL_DET_A" -> ack( args = Seq(RowIdVar(), Var(ID("A"))) )
          )
          .join(table("S")),
        Map( 
          ID("A") -> Var(ID("MIMIR_COL_DET_A")),
          ID("C") -> expr("true"),
          ID("D") -> expr("true")
        ),
        expr("true")
      ))
    }
    "Handle Non-Deterministic Joins With Row Non-Determinism" in {
      percolite(
        table("R")
          .filterParsed("B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END")
          .join(
            table("S")
              .filterParsed("C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END")
          )
      ) must be equalTo ((
        table("R")
          .filterParsed("B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END")
          .map(
            "A" -> Var(ID("A")),
            "B" -> Var(ID("B")), 
            "MIMIR_ROW_DET" 
                -> Var(ID("A")).lt(3).thenElse( 
                                    ack( idx = 1, args = Seq(Var(ID("A"))) ) 
                                  ) ( 
                                    BoolPrimitive(true)
                                  )
          )
          .mapParsedNoInline(
            "A" -> "A",
            "B" -> "B",
            "MIMIR_ROW_DET_LEFT" -> "MIMIR_ROW_DET"
          )
          .join(
            table("S")
              .filterParsed("C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END")
              .map(
                "C" -> Var(ID("C")),
                "D" -> Var(ID("D")), 
                "MIMIR_ROW_DET" 
                    -> Var(ID("D")).gt(5).thenElse( 
                                        ack( idx = 2, args = Seq(Var(ID("D"))) )
                                      ) ( 
                                        BoolPrimitive(true)
                                      )

              )
              .mapParsedNoInline(
                "C" -> "C",
                "D" -> "D",
                "MIMIR_ROW_DET_RIGHT" -> "MIMIR_ROW_DET"
              )
          ),
        Map( 
          ID("A") -> expr("true"),
          ID("B") -> expr("true"),
          ID("C") -> expr("true"),
          ID("D") -> expr("true")
        ),
        expr("MIMIR_ROW_DET_LEFT AND MIMIR_ROW_DET_RIGHT")
      ))
    }
    "Percolate projections over non-deterministic rows" >> {
      percolite(
        table("R")
          .filterParsed("IF A < 5 THEN {{X_1[A]}} ELSE A END > 5")
          .project("A", "B")
      ) must be equalTo ((
        table("R")
          .filterParsed("IF A < 5 THEN {{X_1[A]}} ELSE A END > 5")
          .map( 
            "A" -> Var(ID("A")), 
            "B" -> Var(ID("B")),
            "MIMIR_ROW_DET" ->
              Var(ID("A"))
                .lt(5)
                .thenElse( 
                  ack( args = Seq(Var(ID("A"))) ) 
                )(
                  BoolPrimitive(true)
                )
          )
          .projectNoInline("A", "B", "MIMIR_ROW_DET"), 
        Map(
          ID("A") -> expr("true"),
          ID("B") -> expr("true")
        ),
        expr("MIMIR_ROW_DET")
      ))
    }
    "Handle Deterministic Aggregates" in {
      CTPercolator.percolateLite(
        Project(
          List(
            ProjectArg(ID("COMPANY"), Var(ID("PRODUCT_INVENTORY_COMPANY"))),
            ProjectArg(ID("SUM_2"), Var(ID("MIMIR_AGG_SUM_2")))
          ),
          Aggregate(
            List(Var(ID("PRODUCT_INVENTORY_COMPANY"))), 
            List(AggFunction(ID("sum"), false, List(Var(ID("PRODUCT_INVENTORY_QUANTITY"))), ID("MIMIR_AGG_SUM_2"))),
            Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), List( 
                (ID("PRODUCT_INVENTORY_ID"), TString()), 
                (ID("PRODUCT_INVENTORY_COMPANY"), TString()), 
                (ID("PRODUCT_INVENTORY_QUANTITY"), TInt()), 
                (ID("PRODUCT_INVENTORY_PRICE"), TFloat()) 
              ), List())
        )),
        modelLookup(_)
      ) must be equalTo( (
        Project(
          List(
            ProjectArg(ID("COMPANY"), Var(ID("PRODUCT_INVENTORY_COMPANY"))),
            ProjectArg(ID("SUM_2"), Var(ID("MIMIR_AGG_SUM_2")))
          ),
          Aggregate(
            List(Var(ID("PRODUCT_INVENTORY_COMPANY"))), 
            List(AggFunction(ID("sum"), false, List(Var(ID("PRODUCT_INVENTORY_QUANTITY"))), ID("MIMIR_AGG_SUM_2"))),
            Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), List( 
                (ID("PRODUCT_INVENTORY_ID"), TString()), 
                (ID("PRODUCT_INVENTORY_COMPANY"), TString()), 
                (ID("PRODUCT_INVENTORY_QUANTITY"), TInt()), 
                (ID("PRODUCT_INVENTORY_PRICE"), TFloat()) 
              ), List())
        )),
        Map(
          ID("COMPANY") -> expr("true"),
          ID("SUM_2") -> expr("true")
        ),
        expr("true")
      ))
    }

  }
}