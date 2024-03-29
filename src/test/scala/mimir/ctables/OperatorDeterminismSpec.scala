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
          ID("A") -> Var("A"),
          ID("B") -> Var("B"),
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
            ID("A") -> Var("A"),
            ucol("A") -> TRUE,
            urow -> TRUE
          )
      )
    }

    "Handle Data-Independent Non-Deterministic Projection 1" in {
      percolite(
        table("R")
          .map( 
            "A" -> Var("A"), 
            "B" -> VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq())
          )
      ) must be equalTo (
        table("R")
          .mapByID( 
            ID("A") -> Var("A"), 
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
            "A" -> Var("A"),
            "B" -> Conditional(IsNullExpression(Var("B")), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var("B"))
          )
      ) must be equalTo (
        table("R")
          .mapByID(
            ID("A") -> Var("A"),
            ID("B") -> Conditional(IsNullExpression(Var("B")), VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()), Var("B")),
            ucol("A") -> TRUE,
            ucol("B") -> Conditional(IsNullExpression(Var("B")), ack(), BoolPrimitive(true)),
            urow -> TRUE
          )
      )
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite(
        table("R")
          .filter(VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()).eq(IntPrimitive(3)))
      ) must be equalTo (
        table("R")
          .filter(VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()).eq(IntPrimitive(3)))
          .mapByID(
            ID("A") -> Var("A"),
            ID("B") -> Var("B"),
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
            "B" -> Var("B").isNull
                               .thenElse { VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()) }
                                         { Var("B") }
          )
          .filterParsed("B = 3")
      ) must be equalTo (
        table("R")
          .mapByID(
            ID("A") -> expr("A"),
            ID("B") -> Var("B").isNull
                                   .thenElse { VGTerm(ID("X"), 1, Seq(RowIdVar()), Seq()) }
                                             { Var("B") },
            ucol("B") -> Var("B").isNull.thenElse(ack()) (BoolPrimitive(true))
          )
          .filterParsed("B = 3")
          .mapByID(
            ID("A") -> Var("A"),
            ID("B") -> Var("B"),
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
            ID("A") -> Var("A"),
            ID("B") -> Var("B"),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            ID("C") -> Var("C"),
            ID("D") -> Var("D"),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> TRUE
          )
      )
    }
    "Handle Non-Deterministic Joins" in {
      percolite(
        table("R")
          .map( "A" -> VGTerm(ID("X"), 1, Seq(RowIdVar(), Var("A")), Seq()) )
          .join(table("S"))
      ) must be equalTo (
        table("R")
          .join(table("S"))
          .mapByID(
            ID("A") -> VGTerm(ID("X"), 1, Seq(RowIdVar(), Var("A")), Seq()),
            ucol("A") -> ack( args = Seq(RowIdVar(), Var("A")) ),
            ID("C") -> Var("C"),
            ID("D") -> Var("D"),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> TRUE
          )
      )
    }
    "Handle Non-Deterministic Joins With Row Non-Determinism" in {
      percolite(
        table("R")
          .filter(
            Var("B").lt { 
              Var("A").lt(3)
                      .thenElse { VGTerm(ID("X"), 1, Seq(Var("A")), Seq())}
                                { IntPrimitive(3) }
            }
          )
          .join(
            table("S")
              .filter(
                Var("C").lt { 
                  Var("D").gt(5)
                          .thenElse { VGTerm(ID("X"), 2, Seq(Var("D")), Seq())}
                                    { IntPrimitive(5) }
                }
              )
          )
      ) must be equalTo (
        table("R")
          .filter(
            Var("A").lt(3)
                    .thenElse { 
                      Var("B").lt { 
                        VGTerm(ID("X"), 1, Seq(Var("A")), Seq())
                      } 
                    } { Var("B").lt(3) }
          )
          .join(
            table("S")
              .filter(
                Var("D").gt(5)
                        .thenElse { 
                          Var("C").lt { 
                            VGTerm(ID("X"), 2, Seq(Var("D")), Seq())
                          } 
                        } { Var("C").lt(5) }
              )
          )
          .mapByID(
            ID("A") -> Var("A"),
            ID("B") -> Var("B"),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            ID("C") -> Var("C"),
            ID("D") -> Var("D"),
            ucol("C") -> TRUE,
            ucol("D") -> TRUE,
            urow -> 
              ExpressionUtils.makeAnd(
                Var("A").lt(3).thenElse( 
                                ack( idx = 1, args = Seq(Var("A")) ) 
                              ) ( 
                                BoolPrimitive(true)
                              ),
                Var("D").gt(5).thenElse( 
                                ack( idx = 2, args = Seq(Var("D")) )
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
          .filter(
            Var("A").lt(5)
                    .thenElse { VGTerm(ID("X"), 1, Seq(Var("A")), Seq()) }
                              { Var("A") }
                    .gt(5)
          )
          .project("A", "B")
      ) must be equalTo (
        table("R")
          .filter(
            Var("A").lt(5)
                    .thenElse { VGTerm(ID("X"), 1, Seq(Var("A")), Seq()).gt(5) }
                              { Var("A").gt(5) }
          )
          .mapByID( 
            ID("A") -> Var("A"), 
            ID("B") -> Var("B"),
            ucol("A") -> TRUE,
            ucol("B") -> TRUE,
            urow ->
              Var("A")
                .lt(5)
                .thenElse( 
                  ack( args = Seq(Var("A")) ) 
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
            ProjectArg(ID("COMPANY"), Var("PRODUCT_INVENTORY_COMPANY")),
            ProjectArg(ID("SUM_2"), Var("MIMIR_AGG_SUM_2"))
          ),
          Aggregate(
            Seq(Var("PRODUCT_INVENTORY_COMPANY")), 
            Seq(AggFunction(ID("sum"), false, Seq(Var("PRODUCT_INVENTORY_QUANTITY")), ID("MIMIR_AGG_SUM_2"))),
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
            ProjectArg(ID("COMPANY"), Var("PRODUCT_INVENTORY_COMPANY")),
            ProjectArg(ID("SUM_2"), Var("MIMIR_AGG_SUM_2")),
            ProjectArg(ucol("COMPANY"), TRUE),
            ProjectArg(ucol("SUM_2"), TRUE),
            ProjectArg(urow, TRUE)
          ),
          Aggregate(
            Seq(Var("PRODUCT_INVENTORY_COMPANY")), 
            Seq(AggFunction(ID("sum"), false, Seq(Var("PRODUCT_INVENTORY_QUANTITY")), ID("MIMIR_AGG_SUM_2"))),
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