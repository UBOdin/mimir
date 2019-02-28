package mimir.sql;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Compare implements Statement{
  private SelectBody target;
  private SelectBody expected;

  public Compare(SelectBody target, SelectBody expected)
  {
    this.target = target;
    this.expected = expected;
  }

  public SelectBody getTarget() {
    return target;
  }
  public SelectBody getExpected() {
    return expected;
  }
  public void setTarget(SelectBody target)
  {
    this.target = target;
  }
  public void setExpected(SelectBody expected)
  {
    this.expected = expected;
  }


  @Override
  public void accept(StatementVisitor statementVisitor) {
    Select sel = new Select();
    sel.setSelectBody(expected);
    statementVisitor.visit(sel);
  }
}