package mimir.algebra.function;

import mimir.algebra._

object UtilityFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.registerFold("SEQ_MIN", """
      IF CURR < NEXT THEN CURR ELSE NEXT END
    """)

    fr.registerFold("SEQ_MAX", """
      IF CURR > NEXT THEN CURR ELSE NEXT END
    """)

    fr.register(
      "JULIANDAY",
      (args) => ???,
      (_) => TInt()
    )
    
    fr.register("STRFTIME",(_) => ???, (_) => TInt())
    
  }

}