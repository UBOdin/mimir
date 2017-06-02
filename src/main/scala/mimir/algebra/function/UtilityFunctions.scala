package mimir.algebra.function;

import mimir.algebra._

object UtilityFunctions
{

  def register()
  {
    FunctionRegistry.registerFold("SEQ_MIN", """
      IF CURR < NEXT THEN CURR ELSE NEXT END
    """)

    FunctionRegistry.registerFold("SEQ_MAX", """
      IF CURR > NEXT THEN CURR ELSE NEXT END
    """)

    FunctionRegistry.registerNative(
      "JULIANDAY",
      (args) => ???,
      (_) => TInt()
    )
  }

}