package mimir.sql;

import java.io.File;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;

public class Load implements Statement {
  private File file;
  private String table;

  public Load(String file){ this(new File(file)); }
  public Load(File file){ this(file, null); }
  public Load(String file, String table){ this(new File(file), table); }
  public Load(File file, String table)
  { 
    this.file = file; 
    this.table = table;
  }

  public File getFile() {
    return this.file;
  }

  public void setFile(File file) {
    this.file = file;
  }
  
  public String getTable() {
    return this.table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public void accept(StatementVisitor statementVisitor) { return; } 
  
}
