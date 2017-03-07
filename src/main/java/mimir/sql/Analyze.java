package mimir.sql;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Analyze implements Statement{
	private SelectBody selectBody;
	private String column;
	private PrimitiveValue rowId;

	public Analyze(SelectBody selectBody, PrimitiveValue rowid, String column)
	{
		this.selectBody = selectBody;
		this.column = column;
		this.rowId = rowId;
	}

	public SelectBody getSelectBody() 
	{
		return selectBody;
	}
	public void setSelectBody(SelectBody selectBody) {
		this.selectBody = selectBody;
	}
	
	public String getColumn()
	{
		return column;
	}
	public void setColumn(String column)
	{
		this.column = column;
	}

	public PrimitiveValue getRowId()
	{
		return rowId;
	}
	public void setRowId(PrimitiveValue rowid)
	{
		this.rowId = rowId;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {
		Select sel = new Select();
		sel.setSelectBody(selectBody);
		statementVisitor.visit(sel);
	}
	
}
