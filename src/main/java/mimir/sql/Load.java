package mimir.sql;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;

public class Load implements Statement {
  private File file;
  private String table;
  private String format;
  private List<PrimitiveValue> formatArgs = new ArrayList<>();

  public Load(String file){ this(new File(file)); }
  public Load(File file){ this(file, null); }
  public Load(String file, String table){ this(new File(file), table); }
  public Load(File file, String table){ this(file, table, null); }
  public Load(String file, String table, String format){ this(new File(file), table, format); }
  public Load(File file, String table, String format)
  { 
    this.file = file; 
    this.table = table;
    this.format = format;
  }

  public String getFormat() {
    return this.format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public List<PrimitiveValue> getFormatArgs() {
    return this.formatArgs;
  }

  public void setFormatArgs(List<PrimitiveValue> formatArgs) {
    this.formatArgs = formatArgs;
  }

  public void addFormatArg(PrimitiveValue arg) {
    this.formatArgs.add(arg);
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
