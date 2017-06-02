package mimir.sql;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;


public class ProvenanceStatement implements Statement {

	private Select select;
	
	public ProvenanceStatement(Select select)
	{
		this.select = select;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) 
	{
		statementVisitor.visit(select);
	}
	
	public Select getSelect() 
	{
		return select;
	}

	public void setSelect(Select select) 
	{
		this.select = select;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder("PROVENANCE OF ( "+select.toString()+" )");
		return sb.toString();
	}

	public boolean equals(Object o)
	{
		if(!(o instanceof ProvenanceStatement)){ return false; }
		return toString().equals(o.toString());
	}

}
