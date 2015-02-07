package mimir.sql;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Explain implements Statement{
	private SelectBody selectBody;
	private String column;

	public SelectBody getSelectBody() {
		return selectBody;
	}

	public void setSelectBody(SelectBody selectBody) {
		this.selectBody = selectBody;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		Select sel = new Select();
		sel.setSelectBody(selectBody);
		statementVisitor.visit(sel);
	}
	
}
