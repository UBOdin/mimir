package mimir.sql;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class ProvenanceSelect extends PlainSelect {
	public ProvenanceSelect(PlainSelect plainSelect){
		super();
		this.setDistinct(plainSelect.getDistinct());
		this.setSelectItems(plainSelect.getSelectItems());
		this.setInto(plainSelect.getInto());
		this.setFromItem(plainSelect.getFromItem());
		this.setJoins(plainSelect.getJoins());
		this.setWhere(plainSelect.getWhere());
		this.setGroupByColumnReferences(plainSelect.getGroupByColumnReferences());
		this.setOrderByElements(plainSelect.getOrderByElements());
		this.setHaving(plainSelect.getHaving());
		this.setLimit(plainSelect.getLimit());
		this.setTop(plainSelect.getTop());
	}
	
	public String toString()
	{
		return "PROVENANCE OF ( "+super.toString()+" )";
	}
}
