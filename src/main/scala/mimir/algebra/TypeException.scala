package mimir.algebra;

case class TypeException(found: Type, expected: Type, 
                    detail:String, context:Option[Expression] = None) 
   extends Exception(
    "Type Mismatch ["+detail+
     "]: found "+found.toString+
    ", but expected "+expected.toString+(
      context match {
        case None => ""
        case Some(expr) => " "+expr.toString
      }
    )
);