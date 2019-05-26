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
import mimir.optimizer.operator._
import mimir.exec._
import mimir.provenance._
import mimir.models._
import mimir.test._

object OperatorDeterminismSpec 
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

  def percolite(x:Operator): Operator = 
    PullUpConstants(
      SimpleOptimizeExpressions(
        InlineProjections(
          PullUpConstants(
            OperatorDeterminism.compile(x, modelLookup(_))
          )
        )
      )
    )

  def ucol(x:String) =
    OperatorDeterminism.mimirColDeterministicColumn(ID(x))

  def urow =
    OperatorDeterminism.mimirRowDeterministicColumnName

  val TRUE = BoolPrimitive(true)
  val FALSE = BoolPrimitive(false)

  "The Percolator (Lite)" should {

    "Handle Base Relations" in {
      percolite(
        table("R")
      ) must be equalTo (
        table("R").mapByID(
          ID("A") -> Var(ID("A")),
          ID("B") -> Var(ID("B")),
          ucol("A") -> TRUE,
          ucol("B") -> TRUE,
          urow -> TRUE
        )
      )
    }

    "Handle Deterministic Projection" in {
      percolite(
        table("R")
          .project("A")
      ) must be equalTo (
        table("R").
          mapByID(
            ID("A") -> Var(ID("A")),
            ucol("A") -> TRUE,
            urow -> TRUE
          )
      )
    }

    "Handle Data-Independent Non-Deterministic Projection 1" in {
      percolite(
        table("R")
          .map( 
            "A" -> Var(ID("A")), 
            "B" -> VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq())
          )
      ) must be equalTo (
        table("R")
          .mapByID( 
            ID("A") -> Var(ID("A")), 
            ID("B") -> VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()),
            ucol("A") -> TRUE,
            ucol("B") -> ack(),
            urow -> TRUE
          )
      )
    }
    "Handle Data-Dependent Non-Deterministic Projection 2" in {
      percolite(
        table("R")
          .map(
            "A" -> Var(ID("A")),
            "B" -> Conditional(IsNullExpression(Var(ID("B"))), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var(ID("B")))
          )
      ) must be equalTo (
        table("R")
          .mapByID(
            ID("A") -> Var(ID("A")),
            ID("B") -> Conditional(IsNullExpression(Var(ID("B"))), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var(ID("B"))),
            ucol("A") -> TRUE,
            ucol("B") -> Conditional(IsNullExpression(Var(ID("B"))), ack(), BoolPrimitive(true)),
            urow -> TRUE
          )
      )
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite(
        table("R")
          .filterParsed("{{X_1[ROWID]}} = 3")
      ) must be equalTo (
        table("R")
          .filterParsed("{{X_1[ROWID]}} = 3")
          .mapByID(
            ID("A") -> Var(ID("A")),
            ID("B") -> Var(ID("B")),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            urow -> ack()
          )
      )
    }
    "Handle Data-Dependent Non-Deterministic Projection 3" in {
      percolite(
        table("R")
          .map(
            "A" -> expr("A"),
            "B" -> expr("IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END")
          )
          .filterParsed("B = 3")
      ) must be equalTo (
        table("R")
          .mapByID(
            ID("A") -> expr("A"),
            ID("B") -> expr("IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END"),
            ucol("B") -> Var(ID("B")).isNull.thenElse(ack()) (BoolPrimitive(true))
          )
          .filterParsed("B = 3")
          .mapByID(
            ID("A") -> Var(ID("A")),
            ID("B") -> Var(ID("B")),
            ucol("A") -> TRUE,
            ucol("B") -> Var(ucol("B")),
            urow -> Var(ucol("B"))
          )
      )
    }
    "Handle Deterministic Joins" in {
      percolite(
        table("R").join(table("S"))
      ) must be equalTo (
        table("R")
          .join(table("S"))
          .mapByID(
            ID("A") -> Var(ID("A")),
            ID("B") -> Var(ID("B")),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            ID("C") -> Var(ID("C")),
            ID("D") -> Var(ID("D")),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> TRUE
          )
      )
    }
    "Handle Non-Deterministic Joins" in {
      percolite(
        table("R")
          .mapParsed( "A" -> "{{X_1[ROWID,A]}}" )
          .join(table("S"))
      ) must be equalTo (
        table("R")
          .join(table("S"))
          .mapByID(
            ID("A") -> expr("{{X_1[ROWID,A]}}"),
            ucol("A") -> ack( args = Seq(RowIdVar(), Var(ID("A"))) ),
            ID("C") -> Var(ID("C")),
            ID("D") -> Var(ID("D")),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> TRUE
          )
      )
    }
    "Handle Non-Deterministic Joins With Row Non-Determinism" in {
      percolite(
        table("R")
          .filterParsed("B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END")
          .join(
            table("S")
              .filterParsed("C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END")
          )
      ) must be equalTo (
        table("R")
          .filterParsed("IF A < 3 THEN B < {{X_1[A]}} ELSE B < 3 END")
          .join(
            table("S")
              .filterParsed("IF D > 5 THEN C < {{X_2[D]}} ELSE C < 5 END")
          )
          .mapByID(
            ID("A") -> Var(ID("A")),
            ID("B") -> Var(ID("B")),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            ID("C") -> Var(ID("C")),
            ID("D") -> Var(ID("D")),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> 
              ExpressionUtils.makeAnd(
                Var(ID("A")).lt(3).thenElse( 
                                ack( idx = 1, args = Seq(Var(ID("A"))) ) 
                              ) ( 
                                BoolPrimitive(true)
                              ),
                Var(ID("D")).gt(5).thenElse( 
                                ack( idx = 2, args = Seq(Var(ID("D"))) )
                              ) ( 
                                BoolPrimitive(true)
                              )
              )
          )
      )
    }
    "Percolate projections over non-deterministic rows" >> {
      percolite(
        table("R")
          .filterParsed("IF A < 5 THEN {{X_1[A]}} ELSE A END > 5")
          .project("A", "B")
      ) must be equalTo (
        table("R")
          .filterParsed("IF A < 5 THEN {{X_1[A]}} > 5 ELSE A > 5 END")
          .mapByID( 
            ID("A") -> Var(ID("A")), 
            ID("B") -> Var(ID("B")),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            urow ->
              Var(ID("A"))
                .lt(5)
                .thenElse( 
                  ack( args = Seq(Var(ID("A"))) ) 
                )(
                  BoolPrimitive(true)
                )
          )
      )
    }
    "Handle Deterministic Aggregates" in {
      percolite(
        Project(
          Seq(
            ProjectArg(ID("COMPANY"), Var(ID("PRODUCT_INVENTORY_COMPANY"))),
            ProjectArg(ID("SUM_2"), Var(ID("MIMIR_AGG_SUM_2")))
          ),
          Aggregate(
            Seq(Var(ID("PRODUCT_INVENTORY_COMPANY"))), 
            Seq(AggFunction(ID("sum"), false, Seq(Var(ID("PRODUCT_INVENTORY_QUANTITY"))), ID("MIMIR_AGG_SUM_2"))),
            Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), Seq( 
                (ID("PRODUCT_INVENTORY_ID"), TString()), 
                (ID("PRODUCT_INVENTORY_COMPANY"), TString()), 
                (ID("PRODUCT_INVENTORY_QUANTITY"), TInt()), 
                (ID("PRODUCT_INVENTORY_PRICE"), TFloat()) 
              ), Seq())
        ))
      ) must be equalTo(
        Project(
          Seq(
            ProjectArg(ID("COMPANY"), Var(ID("PRODUCT_INVENTORY_COMPANY"))),
            ProjectArg(ID("SUM_2"), Var(ID("MIMIR_AGG_SUM_2"))),
            ProjectArg(ucol("COMPANY"), TRUE),
            ProjectArg(ucol("SUM_2"), TRUE),
            ProjectArg(urow, TRUE)
          ),
          Aggregate(
            Seq(Var(ID("PRODUCT_INVENTORY_COMPANY"))), 
            Seq(AggFunction(ID("sum"), false, Seq(Var(ID("PRODUCT_INVENTORY_QUANTITY"))), ID("MIMIR_AGG_SUM_2"))),
            Table(ID("PRODUCT_INVENTORY"),ID("PRODUCT_INVENTORY"), Seq( 
                (ID("PRODUCT_INVENTORY_ID"), TString()), 
                (ID("PRODUCT_INVENTORY_COMPANY"), TString()), 
                (ID("PRODUCT_INVENTORY_QUANTITY"), TInt()), 
                (ID("PRODUCT_INVENTORY_PRICE"), TFloat()) 
              ), Seq())
        ))
      )
    }

  }
}