package mimir.exec;

import java.sql._;
import mimir.util.JDBCUtils;
import mimir.algebra._;
import mimir.algebra.Type._;
import mimir.provenance._;

class ResultSetIterator(
  val src: ResultSet, 
  val visibleSchema: Map[String,Type.T], 
  visibleColumns: List[Int], 
  provenanceTokenColumns: List[Int]
) extends ResultIterator
{
  val meta = src.getMetaData();
  val schema: List[(String,Type.T)] = 
    visibleColumns.map( (i) => {
      // println("Visible: "+visibleSchema)
      val colName = meta.getColumnName(i+1).toUpperCase();
      (
        colName,
        visibleSchema.getOrElse(colName, 
          JDBCUtils.convertSqlType(meta.getColumnType(i+1))
        )
      )
    }).toList
  val extract: List[() => PrimitiveValue] =
    schema.map(_._2).zipWithIndex.map( t => {
      () => JDBCUtils.convertField(t._1, src, t._2+1)

    })
      
  var isFirst = true;
  var empty = false;
  
  def apply(v: Int): PrimitiveValue = {
    val ret = extract(v)()
    if(src.wasNull()){ return new NullPrimitive(); }
    else { return ret; }
  }
  def numCols: Int = schema.length
  
  def open() = {
    if(!src.isBeforeFirst) empty = true
    while(src.isBeforeFirst()){ src.next(); }
  }
  
  def getNext(): Boolean =
  {
    if(empty) { false }
    else if(isFirst) { isFirst = false; true }
    else { src.next(); }
//    if(src.isAfterLast()){ return false; }
//    return true;
  }
  
  def close() = { 
    src.close();
  }
  
  def deterministicRow() = true;
  def deterministicCol(v: Int) = true;
  def missingRows() = false;
  def provenanceToken() = 
    Eval.eval(Provenance.rowIdVal(
      provenanceTokenColumns.map( 
        (col) => RowIdPrimitive(src.getString(col+1))
      )
    )).asInstanceOf[RowIdPrimitive]
}
