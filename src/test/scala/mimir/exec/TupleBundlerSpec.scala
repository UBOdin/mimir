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

    "Create Sane Results" >> {
      val q1 =
        sampler.compileFlat(select("""
          SELECT A FROM R_CLASSIC WHERE B = 2
        """))._1

      q1.schema.map(_._1) must beEqualTo(Seq("A", "MIMIR_WORLD_BITS"))

      val r1 =
        db.query(q1).mapRows( x => (x(0).asLong.toInt -> x(1).asLong.toInt) ).toMap

      print(r1.keys)
      r1.keys must contain( 4 )
      if(r1 contains 2){
        r1(2) must be equalTo ((1 << numSamples)-1)
      }
      ok

    }

  }
}
