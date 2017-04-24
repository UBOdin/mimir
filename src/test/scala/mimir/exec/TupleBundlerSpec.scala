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
  val sampler = new TupleBundler(db, (0 until numSamples).map { _ => rand.nextLong })
  val allWorlds = sampler.fullBitVector

  def conf(bv: Long): Double = WorldBits.confidence(bv, numSamples)

  def sanityCheckDisjointWorldset(disjointWorlds: Seq[Long])
  {
    val worlds = disjointWorlds.map( WorldBits.worlds(_, numSamples) ).toSeq

    // All samples should be represented
    worlds.flatten should beEqualTo((0 until numSamples).toSet)

    // Shouldn't be seeing any pairs that exist in the same world
    for(i <- 0 until worlds.size){
      for(j <- 0 until worlds.size){
        if(i != j){
          (worlds(i) & worlds(j)) must beEmpty
        }
      }
    }
  }

  "Tuple Bundle Evaluation" should {
    "Compile sanely" >> {

      val q1 = 
        // db.compiler.optimize(
          sampler.compileFlat(select("""
            SELECT * FROM R_CLASSIC WHERE B = 2
          """))._1
        // )
      q1.schema.map(_._1) must contain(eachOf(
        "A", 
        "MIMIR_SAMPLE_0_B", 
        "MIMIR_SAMPLE_2_C", 
        "MIMIR_WORLD_BITS"
      ))
    }

    "Evaluate Project Queries" >> {
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

      // Deterministic rows.  Should always be there
      r1 must contain(eachOf( (2 -> Set(2)), (4 -> Set(2)) ))

      // Nondeterministic rows.  Not a huge deal if this next case breaks.  
      // Poke the PRNG above or add more samples until this works again.
      r1(1) should contain(eachOf( 4, 2, 3 ))

    }

    "Evaluate Select-Project Queries" >> {
      val q1 =
        sampler.compileFlat(select("""
          SELECT A FROM R_CLASSIC WHERE B = 2
        """))._1

      q1.schema.map(_._1) must beEqualTo(Seq("A", "MIMIR_WORLD_BITS"))

      val r1 =
        db.query(q1).mapRows( x => (x(0).asLong.toInt -> x(1).asLong) ).toMap

      r1.keys should contain( eachOf(1, 2, 4) )
      if(r1 contains 2){
        r1(2) must be equalTo ( allWorlds )
      }

      // Nondeterministic rows.  Not a huge deal if this next case breaks.  
      // Poke the PRNG above or add more samples until this works again.
      conf(r1(1)) should beBetween(0.0, 0.6)

      // Deterministic rows.  Should always be there
      conf(r1(2)) must beEqualTo(1.0)
      conf(r1(4)) must beEqualTo(1.0)
    }

    "Evaluate Deterministic Aggregate Queries without Group-Bys" >> {
      val q1 = 
        sampler.compileFlat(select("""
          SELECT SUM(A) AS A FROM R_CLASSIC
        """))._1

      // This test assumes that compileFlat just adds a WORLDS_BITS column
      q1.schema.map(_._1) must beEqualTo(Seq("A", "MIMIR_WORLD_BITS"))

      val r1 =
        db.query(q1).mapRows( x => (x(0).asLong.toInt, x(1).asLong) ).toSeq


      // This particular result should be deterministic
      r1 must haveSize(1)
      r1(0)._1 must beEqualTo( 7 )
      r1(0)._2 must beEqualTo( allWorlds )
    }


    "Evaluate Aggregate Queries without Group-Bys" >> {
      val q1 = 
        sampler.compileFlat(select("""
          SELECT SUM(B) AS B FROM R_CLASSIC
        """))._1

      // This test assumes that compileFlat just splits 'B' into samples and 
      // adds a world bits column.
      q1.schema.map(_._1) must beEqualTo(
        (0 until numSamples).map(sampler.colNameInSample("B", _)).toSeq ++
        Seq("MIMIR_WORLD_BITS")
      )

      // Extract into (Seq(B values), worldBits)
      val r1 =
        db.query(q1).mapRows { x => (
          (0 until numSamples).map { x(_).asLong.toInt }.toSeq,
          x(numSamples).asLong
        )}.toSeq

      // This particular result *row* should be deterministic
      r1 must haveSize(1)
      r1(0)._2 must beEqualTo( allWorlds )
      // A = 1 -> B = { 2, 3, 4 }
      // A = 2 -> B = 2
      // A = 4 -> B = 2
      // SUM = either 6, 7, or 8
      r1(0)._1.toSet should beEqualTo( Set( 6, 7, 8 ) )
    }

    "Evaluate Aggregate Queries with Nondeterministic Aggregates" >> {
      val q1 = 
        sampler.compileFlat(select("""
          SELECT A, SUM(B) AS B FROM R_CLASSIC GROUP BY A
        """))._1

      // This test assumes that compileFlat just adds a WORLDS_BITS column
      q1.schema.map(_._1) must beEqualTo(Seq("A")++
        (0 until numSamples).map(sampler.colNameInSample("B", _)).toSeq ++
        Seq("MIMIR_WORLD_BITS")
      )

      val r1 =
        db.query(q1).mapRows { x => (
          x(0).asLong.toInt -> (
            (1 until (numSamples+1)).map { x(_).asLong.toInt }.toSeq,
            x(numSamples+1).asLong
          )
        )}.toMap

      // A is deterministic.  We should see all possibilities
      r1.keys must contain(eachOf( 1, 2, 4 ))

      // Thus each row should also be present in all worlds
      r1(1)._2 must beEqualTo( allWorlds )
      r1(2)._2 must beEqualTo( allWorlds )
      r1(4)._2 must beEqualTo( allWorlds )

      // B should only be nondeterministic for one table
      r1(1)._1.toSet should beEqualTo( Set( 2, 3, 4 ) )
      r1(2)._1.toSet should beEqualTo( Set( 2 ) )
      r1(4)._1.toSet should beEqualTo( Set( 2 ) )
    }

    "Evaluate Aggregate Queries with Nondeterministic Group-Bys" >> {
      val q1 = 
        sampler.compileFlat(select("""
          SELECT B, SUM(A) AS A FROM R_CLASSIC GROUP BY B
        """))._1

      q1.schema.map(_._1) must beEqualTo(Seq("B")++
        (0 until numSamples).map(sampler.colNameInSample("A", _)).toSeq ++
        Seq("MIMIR_WORLD_BITS")
      )

      val r1 =
        db.query(q1).mapRows { x => 
          val b = x(0).asLong.toInt
          val worlds = WorldBits.worlds(x(numSamples+1).asLong, numSamples)
          val a = worlds.map { i => x(i + 1).asLong.toInt }

          ( b -> (a, worlds) )
        }.toMap

      r1.keys must contain( 2 )
      r1.keys should contain(eachOf( 3, 4 ))
      r1(2)._1.toSet should be equalTo( Set(6, 7) )
      r1(3)._1.toSet should be equalTo( Set(1) )
      r1(4)._1.toSet should be equalTo( Set(1) )

      r1(2)._2.size should be equalTo(numSamples)
      r1(3)._2.size should be between(0, numSamples / 2)
      r1(4)._2.size should be between(0, numSamples / 2)

    }
  }
}
