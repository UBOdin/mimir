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
    
    fr.register("WEB", 
      {  
        case Seq(StringPrimitive(url)) => StringPrimitive(mimir.util.HTTPUtils.get(url))
      },
      (x: Seq[Type]) => TString()
    )
    
    fr.register("WEBJSON", 
      {  
        case Seq(StringPrimitive(url)) => StringPrimitive(mimir.util.HTTPUtils.getJson(url).toString())
        case Seq(StringPrimitive(url), StringPrimitive(path)) => StringPrimitive(mimir.util.HTTPUtils.getJson(url, Some(path)).toString())
      },
      (x: Seq[Type]) => TString()
    )
    
  }

}