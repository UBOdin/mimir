package mimir.exec;

import java.io._
import org.specs2.specification._
import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test._
import mimir.models._
import org.specs2.specification.core.Fragments

object TupleBundlerSpec
  extends SQLTestSpecification("TupleBundler")
  with BeforeAll 
{

  sequential

  def beforeAll = 
  {
    update("CREATE TABLE R(A int, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE LENS R_CLASSIC AS SELECT * FROM R WITH KEY_REPAIR(A)")
    // update("CREATE LENS R_FASTPATH AS SELECT * FROM R WITH KEY_REPAIR(A, ENABLE(FAST_PATH))")
  }

  val rand = new Random(42)
  val numSamples = 10
  val sampler = new TupleBundler(db, (0 until numSamples).map { _ => rand.nextInt })

  def conf(bv: Long): Double = sampler.confidence(bv)

  "Tuple Bundle Evaluation" should {
    "Compile sanely" >> {

      val q1 = 
        // db.compiler.optimize(
          sampler.compileFlat(select("""
            SELECT * FROM R_CLASSIC WHERE B = 2
          """))._1
        // )
      q1.schema.map(_._1) must contain( eachOf("A", "MIMIR_SAMPLE_0_B", "MIMIR_SAMPLE_2_C", "MIMIR_WORLD_BITS" ) )
    }

    "Project Query" >> {
      val q1 =
        sampler.compileFlat(select("""
          SELECT A, B FROM R_CLASSIC
        """))._1

      val r1 =
        db.query(q1).mapRows( x => 
          (
            x(0).asLong.toInt, 
            (1 until 11).map { i => x(i).asLong.toInt }.toSet
          ) 
        ).toMap

      r1 must contain(eachOf( (2 -> Set(2)), (4 -> Set(2)) ))
      r1(1) should contain(eachOf( 4, 2, 3 ))

    }


    "Select-Project Query" >> {
      val q1 =
        sampler.compileFlat(select("""
          SELECT A FROM R_CLASSIC WHERE B = 2
        """))._1

      q1.schema.map(_._1) must beEqualTo(Seq("A", "MIMIR_WORLD_BITS"))

      val r1 =
        db.query(q1).mapRows( x => (x(0).asLong.toInt -> x(1).asLong) ).toMap

      r1.keys should contain( eachOf(1, 2, 4) )
      if(r1 contains 2){
        r1(2) must be equalTo ((1l << numSamples)-1l)
      }

      conf(r1(1)) should beBetween(0.0, 0.6)
      conf(r1(2)) must beEqualTo(1.0)
      conf(r1(4)) must beEqualTo(1.0)
    }

  }
}
