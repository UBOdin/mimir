package mimir.exec;

import java.sql._;
import java.util.{GregorianCalendar, Calendar};
import mimir.sql.JDBCUtils;
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
          colName match {
            case mimir.ctables.CTPercolator.ROWID_KEY => TRowId
            case _ => JDBCUtils.convertSqlType(meta.getColumnType(i+1))
          }
        )
      )
    }).toList
  val extract: List[() => PrimitiveValue] =
    schema.map(_._2).zipWithIndex.map( {
      case (t, colIdx) =>
        val col = visibleColumns(colIdx)
        t match {
          case TString =>
            () => {
              new StringPrimitive(src.getString(col+1))
            }

          case TFloat =>
            () => {
              new FloatPrimitive(src.getDouble(col + 1))
            }

          case TInt => 
            () => {
              new IntPrimitive(src.getLong(col + 1))
            }

          case TRowId =>
            () => {
              new RowIdPrimitive(src.getString(col + 1))
            }

          case TDate =>
            () => {
              val calendar = Calendar.getInstance()
              try {
                calendar.setTime(src.getDate(col + 1))
              } catch {
                case e: SQLException =>
                  calendar.setTime(Date.valueOf(src.getString(col + 1)))
                case e: NullPointerException =>
                  new NullPrimitive
              }
              new DatePrimitive(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE))
            }

          case TAny =>
            () => { NullPrimitive() }
    }}).toList
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