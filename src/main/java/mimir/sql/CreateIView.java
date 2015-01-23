package mimir.sql;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;


public class CreateIView implements Statement{

	private Table table;
	private SelectBody selectBody;
	private List<Module> modules = new ArrayList<Module>();

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

	public void addModule(Module m) {
		modules.add(m);
	}

	public List<Module> getModules() {
		return modules;
	}

	public static class Module {
		public final String name;
		public final List<Expression> args;
		public Module(String name, List<Expression> args)
		{
			this.name = name;
			this.args = args;
		}
	}

}
