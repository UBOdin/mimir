package mimir.algebra.function;

import mimir.algebra._

object UtilityFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.registerFold(ID("seq_min"), """
      IF CURR < NEXT THEN CURR ELSE NEXT END
    """)

    fr.registerFold(ID("seq_max"), """
      IF CURR > NEXT THEN CURR ELSE NEXT END
    """)

    fr.register(
      ID("julianday"),
      (args) => ???,
      (_) => TInt()
    )
    
    fr.register(ID("strftime"),(_) => ???, (_) => TInt())
    
    fr.register(ID("web"), 
      {  
        case Seq(StringPrimitive(url)) => StringPrimitive(mimir.util.HTTPUtils.get(url))
      },
      (x: Seq[Type]) => TString()
    )
    
    fr.register(ID("webjson"), 
      {  
        case Seq(StringPrimitive(url)) => StringPrimitive(mimir.util.HTTPUtils.getJson(url).toString())
        case Seq(StringPrimitive(url), StringPrimitive(path)) => StringPrimitive(mimir.util.HTTPUtils.getJson(url, Some(path)).toString())
      },
      (x: Seq[Type]) => TString()
    )
    
    fr.register(
      ID("monotonically_increasing_id"),
      (_) => ???,
      (_) => TRowId()
    )
    
    
  }

}