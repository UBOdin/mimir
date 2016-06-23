package mimir.sql;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import mimir.parser.*;

public class CastOperation extends Function
{
  public CastOperation(Expression target, String type)
  {
    super();
    super.setName("CAST");
    super.setParameters(
      new ExpressionList(
        Arrays.asList((Expression)target, (Expression)new StringValue(type))
    ));
  }

  public String toString()
  {
    List<Expression> exprs = getParameters().getExpressions();
    return "CAST("+exprs.get(0).toString()+" AS "+((StringValue)exprs.get(1)).toRawString()+")";
  }
}