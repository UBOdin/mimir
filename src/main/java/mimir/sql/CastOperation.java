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

  public void setTarget(Expression target)
  {
    getParameters().getExpressions().set(0, target);
  }

  public Expression getTarget()
  {
    return getParameters().getExpressions().get(0);
  }

  public void setType(String type)
  {
    getParameters().getExpressions().set(1, new StringValue(type));
  }

  public String getType()
  {
    return ((StringValue)getParameters().getExpressions().get(1)).toRawString();
  }

  public String toString()
  {
    List<Expression> exprs = getParameters().getExpressions();
    return "CAST("+exprs.get(0).toString()+" AS "+((StringValue)exprs.get(1)).toRawString()+")";
  }
}