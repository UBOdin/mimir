package mimir.ctables;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.ctables._
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
  
  val schema = Map[String,Map[String,Type]](
    ("R", Map( 
      ("R_A", TInt()),
      ("R_B", TInt()),
      ("R_C", TInt())
    )),
    ("S", Map( 
      ("S_C", TInt()),
      ("S_D", TFloat())
    ))
  )

  def modelLookup(model: String) = UniformDistribution
  def schemaLookup(table: String) = schema(table).toList
  def ack(
    idx: Int = 1, 
    args: Seq[Expression] = Seq(RowIdVar())
  ): Expression = VGTermAcknowledged(UniformDistribution, idx, args)

  def project(cols: List[(String,String)], src: Operator): Operator =
    Project(cols.map( { case (name,e) => ProjectArg(name, expr(e))}), src) 

  def percolite(x:String) = 
    CTPercolator.percolateLite(oper(x))

  "The Percolator (Lite)" should {

    "Handle Base Relations" in {
      percolite("R(A, B)") must be equalTo ((
        oper("R(A, B)"),
        Map( 
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("true")
      ))
    }

    "Handle Deterministic Projection" in {
      percolite("PROJECT[A <= A](R(A, B))") must be equalTo ((
        oper("PROJECT[A <= A](R(A, B))"),
        Map( 
          ("A", expr("true"))
        ),
        expr("true")
      ))
    }

    "Handle Data-Independent Non-Deterministic Projection" in {
      percolite("PROJECT[A <= A, B <= {{X_1[ROWID]}}](R(A, B))") must be equalTo ((
        oper("PROJECT[A <= A, B <= {{X_1[ROWID]}}](R(A, B))"),
        Map( 
          ("A", expr("true")),
          ("B", ack())
        ),
        expr("true")
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection 1" in {
      percolite("""
        PROJECT[A <= A, 
                B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END
               ](R(A, B))""") must be equalTo ((
        oper("""
          PROJECT[A <= A, 
                  B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                  MIMIR_COL_DET_B <= 
                       IF B IS NULL THEN B_ACK ELSE TRUE END
                ](R(A, B))""",
          Map("B_ACK" -> ack())
        ),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("true")
      ))
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite("SELECT[{{X_1[ROWID]}} = 3](R(A, B))") must be equalTo ((
        oper("SELECT[{{X_1[ROWID]}} = 3](R(A, B))"),
        Map( 
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        ack()
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection 2" in {
      percolite("""
        SELECT[B = 3](
          PROJECT[A <= A, 
                  B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END
                 ](R(A, B)))""") must be equalTo ((
        oper("""
        PROJECT[A <= A, B <= B, MIMIR_COL_DET_B <= MIMIR_COL_DET_B, MIMIR_ROW_DET <= MIMIR_COL_DET_B](
          SELECT[B = 3](
            PROJECT[A <= A, 
                    B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                    MIMIR_COL_DET_B <= 
                         IF B IS NULL THEN B_ACK ELSE TRUE END
                  ](R(A, B))))""",
          Map("B_ACK" -> ack())
        ),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("MIMIR_ROW_DET")
      ))
    }
    "Handle Deterministic Joins" in {
      percolite("""
        JOIN(R(A,B), S(C,D))
      """) must be equalTo ((
        oper("""
          JOIN(R(A,B), S(C,D))          
        """),
        Map( 
          ("A", expr("true")),
          ("B", expr("true")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("true")
      ))
    }
    "Handle Non-Deterministic Joins" in {
      percolite("""
        JOIN(
          PROJECT[A <= {{X_1[ROWID,A]}}](R(A,B)), 
          S(C,D)
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= {{X_1[ROWID,A]}}, MIMIR_COL_DET_A <= A_ACK](R(A,B)), 
            S(C,D)
          )
        """, Map("A_ACK" -> ack( args = Seq(RowIdVar(), Var("A")) ))),
        Map( 
          ("A", Var("MIMIR_COL_DET_A")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("true")
      ))
    }
    "Handle Non-Deterministic Joins With Row Non-Determinism" in {
      percolite("""
        JOIN(
          SELECT[B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END](R(A,B)), 
          SELECT[C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END](S(C,D))
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= A, B <= B, MIMIR_ROW_DET_LEFT <= MIMIR_ROW_DET](
              PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= IF A < 3 THEN LHS_ACK ELSE TRUE END](
                SELECT[B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END](R(A,B)))), 
            PROJECT[C <= C, D <= D, MIMIR_ROW_DET_RIGHT <= MIMIR_ROW_DET](
              PROJECT[C <= C, D <= D, MIMIR_ROW_DET <= IF D > 5 THEN RHS_ACK ELSE TRUE END](
                SELECT[C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END](S(C,D))))
          )
        """, Map(
          "LHS_ACK" -> ack( idx = 1, args = Seq(Var("A")) ),
          "RHS_ACK" -> ack( idx = 2, args = Seq(Var("D")) )
        )),
        Map( 
          ("A", expr("true")),
          ("B", expr("true")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("MIMIR_ROW_DET_LEFT AND MIMIR_ROW_DET_RIGHT")
      ))
    }
    "Percolate projections over non-deterministic rows" >> {
      percolite("""
        PROJECT[A <= A, B <= B](
          SELECT[IF A < 5 THEN {{X_1[A]}} ELSE A END > 5](R(A,B))
        )
      """) must be equalTo ((
        oper("""
          PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= MIMIR_ROW_DET](
            PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= IF A < 5 THEN A_ACK ELSE TRUE END](
              SELECT[IF A < 5 THEN {{X_1[A]}} ELSE A END > 5](R(A,B))
            )
          )
        """, Map(
          "A_ACK" -> ack( args = Seq(Var("A")) )
        )),
        Map(
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("MIMIR_ROW_DET")
      ))
    }
    "Handle Deterministic Aggregates" in {
      CTPercolator.percolateLite(
        Project(
          List(
            ProjectArg("COMPANY", Var("PRODUCT_INVENTORY_COMPANY")),
            ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))
          ),
          Aggregate(
            List(Var("PRODUCT_INVENTORY_COMPANY")), 
            List(AggFunction("SUM", false, List(Var("PRODUCT_INVENTORY_QUANTITY")), "MIMIR_AGG_SUM_2")),
            Table("PRODUCT_INVENTORY", List( 
                ("PRODUCT_INVENTORY_ID", TString()), 
                ("PRODUCT_INVENTORY_COMPANY", TString()), 
                ("PRODUCT_INVENTORY_QUANTITY", TInt()), 
                ("PRODUCT_INVENTORY_PRICE", TFloat()) 
              ), List())
        ))
      ) must be equalTo( (
        Project(
          List(
            ProjectArg("COMPANY", Var("PRODUCT_INVENTORY_COMPANY")),
            ProjectArg("SUM_2", Var("MIMIR_AGG_SUM_2"))
          ),
          Aggregate(
            List(Var("PRODUCT_INVENTORY_COMPANY")), 
            List(AggFunction("SUM", false, List(Var("PRODUCT_INVENTORY_QUANTITY")), "MIMIR_AGG_SUM_2")),
            Table("PRODUCT_INVENTORY", List( 
                ("PRODUCT_INVENTORY_ID", TString()), 
                ("PRODUCT_INVENTORY_COMPANY", TString()), 
                ("PRODUCT_INVENTORY_QUANTITY", TInt()), 
                ("PRODUCT_INVENTORY_PRICE", TFloat()) 
              ), List())
        )),
        Map(
          "COMPANY" -> expr("true"),
          "SUM_2" -> expr("true")
        ),
        expr("true")
      ))
    }

  }
}