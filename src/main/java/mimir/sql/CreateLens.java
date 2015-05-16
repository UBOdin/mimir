package mimir.sql;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;


public class CreateLens implements Statement {

	private String name;
	private SelectBody body;
	private String type;
	public final List<Expression> args;


	public CreateLens(String name, SelectBody body, String type, List<Expression> args)
	{
		this.name = name;
		this.body = body;
		this.type = type;
		this.args = args;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) 
	{
		Select sel = new Select();
		sel.setSelectBody(body);
		statementVisitor.visit(sel);
	}
	public String getName() 
	{
		return name;
	}

	public void setName(String name) 
	{
		this.name = name;
	}

	public SelectBody getSelectBody() 
	{
		return body;
	}

	public void setSelectBody(SelectBody body) 
	{
		this.body = body;
	}

	public String getType()
	{
		return type;
	}
	public List<Expression> getArgs()
	{
		return args;
	}

}
