package mimir.algebra

import mimir.test._

object FunctionSpec extends SQLTestSpecification("LoadCSV")
{

  val test_data = "           <...>-2263  [000] ...2   268.359128: sched_switch: prev_comm=app_process prev_pid=2263 prev_prio=120 prev_state=R+ ==> next_comm=mmcqd/1 next_pid=120 next_prio=120"

  def eval_re(needle: String, haystack: String): PrimitiveValue =
    db.interpreter.eval(expr("RE_EXTRACT(N, H)"), Map( "N" -> StringPrimitive(needle), "H" -> StringPrimitive(haystack) ).get(_))

  "RE_EXTRACT" should {

    "Match Strings" >> {
      eval_re("   [0-9]+\\.[0-9]+", test_data) must be equalTo( StringPrimitive("   268.359128") )
      eval_re("[0-9]+\\.[0-9]+", test_data) must be equalTo( StringPrimitive("268.359128") )
    }

    "Match Groups" >> {
      eval_re("   ([0-9]+\\.[0-9]+)", test_data) must be equalTo( StringPrimitive("268.359128") )
    }

  }

}