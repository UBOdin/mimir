package mimir.sql;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;


public class CreateView implements Statement{

	private Table table;
	private SelectBody selectBody;
	private List nNullColumns ;

	@Override
	public void accept(StatementVisitor statementVisitor) {
		Select sel = new Select();
		sel.setSelectBody(selectBody);
		statementVisitor.visit(sel);
	}
	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public SelectBody getSelectBody() {
		return selectBody;
	}

	public void setSelectBody(SelectBody body) {
		selectBody = body;
	}

	public List getnNullColumns() {
		return nNullColumns;
	}

	public void setnNullColumns(List nNullColumns) {
		this.nNullColumns = nNullColumns;
	}


}
