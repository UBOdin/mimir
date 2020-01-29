package mimir.exec

import org.specs2.mutable._
import mimir.algebra._
import mimir.test.SQLTestSpecification

class EvalInlinedSpec extends SQLTestSpecification("EvalInlinedSpec")
{

  def test(e: String, scope: Map[ID, PrimitiveValue] = Map()): PrimitiveValue = 
  {
    val parsed = expr(e)
    val compiledScope = scope.mapValues { v => ( v.getType, {_:Unit => v} ) }
    val compiler = new EvalInlined[Unit](compiledScope, db)
    val compiled = compiler.compile(parsed)
    return compiled( () )
  }


  "The Lambda Compiler" >> {

    "Handle Int/Float Addition / Type Escalation" >> {

      test("0.1 + 1") must be equalTo FloatPrimitive(1.1)
      test("CAST('0.1' AS float) + CAST('1' AS int)") must be equalTo FloatPrimitive(1.1)
      test("CAST('1' AS int) + CAST('0.1' AS float)") must be equalTo FloatPrimitive(1.1)

    }

  }

}
