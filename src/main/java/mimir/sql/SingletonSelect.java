package mimir.sql;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SingletonSelect extends PlainSelect {
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder("SELECT ");
    String sep = "";
    for(SelectItem si : getSelectItems()){
      sb.append(sep);
      sb.append(si.toString());
      sep = ", ";
    }
    if(getWhere() != null){
      sb.append(" WHERE ");
      sb.append(getWhere().toString());
    }
    return sb.toString();
  }
}
