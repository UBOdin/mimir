package mimir.algebra;

class NullTypeException(found: Type, expected: Type, 
                    detail:String, context:Option[Expression] = None) 
   extends TypeException(
    found, expected, detail, context
);