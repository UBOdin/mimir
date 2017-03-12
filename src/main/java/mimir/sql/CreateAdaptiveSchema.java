package mimir.sql;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;


public class CreateAdaptiveSchema implements Statement {

    private String name;
    private SelectBody selectBody;
    private String type;
    private List<Expression> args;

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
        return selectBody;
    }

    public void setSelectBody(SelectBody body) 
    {
        selectBody = body;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public List<Expression> getArgs()
    {
        return args;
    }

    public void setArgs(List<Expression> args)
    {
        this.args = args;
    }


    @Override
    public void accept(StatementVisitor statementVisitor) {
        Select sel = new Select();
        sel.setSelectBody(selectBody);
        statementVisitor.visit(sel);
    }

}
