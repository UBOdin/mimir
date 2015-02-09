package mimir.exec;

import java.sql._;
import mimir.sql.JDBCUtils;
import mimir.algebra._;
import mimir.algebra.Type._;

class ResultSetIterator(src: ResultSet) extends ResultIterator
{
  
  val meta = src.getMetaData();
  var extract: List[() => PrimitiveValue] = 
    (0 until meta.getColumnCount()).map( (i) => {
      JDBCUtils.convertSqlType(meta.getColumnType(i+1)) match {
        case TString => 
          () => { 
            new StringPrimitive(src.getString(i+1))
          }
        
        case TFloat => 
          () => { 
            new FloatPrimitive(src.getDouble(i+1))
          }
        
        case TInt => 
          () => { 
            new IntPrimitive(src.getLong(i+1))
          }
      }
    }).toList
  var schema: List[(String,Type.T)] = 
    (0 until meta.getColumnCount()).map( (i) => (
      meta.getColumnName(i+1),
      JDBCUtils.convertSqlType(meta.getColumnType(i+1))
    ) ).toList
  var isFirst = true;
  
  def apply(v: Int): PrimitiveValue = {
    val ret = extract(v)()
    if(src.wasNull()){ return new NullPrimitive(); }
    else { return ret; }
  }
  def numCols: Int = extract.length
  
  def open() = { 
    while(src.isBeforeFirst()){ src.next(); }
  }
  
  def getNext(): Boolean =
  {
    if(isFirst) { isFirst = false; } 
    else        { src.next(); }
    if(src.isAfterLast()){ return false; }
    return true;
  }
  
  def close() = { 
    src.close();
  }
  
  def deterministicRow() = true;
  def deterministicCol(v: Int) = true;
  def missingRows() = false;

}