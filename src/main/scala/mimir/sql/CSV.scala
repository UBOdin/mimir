package mimir.sql;

import java.sql.{ResultSet,ResultSetMetaData,Blob,Clob,SQLFeatureNotSupportedException,NClob,Ref,RowId,SQLXML,Statement,Time,Timestamp,SQLWarning,SQLException};
import java.net.URL
import java.io._;
import java.util.{Calendar};
import collection.JavaConversions._;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.parser.CCJSqlParser;

import edu.buffalo.cse562.Schema
import edu.buffalo.cse562.optimizer.Optimizer
import edu.buffalo.cse562.eval.{PlanCompiler,Operator}
import edu.buffalo.cse562.data.Datum

import mimir.algebra.Type;

class CSVMetaData(nameIDs: java.util.Map[String, Int], sch: List[Int]) extends ResultSetMetaData
{
  val names = nameIDs.toList.sortBy(_._2)( new Ordering[Int]() {
    override def compare(x: Int, y: Int) = { x compareTo y }
  }).map(_._1).toList
  
  def getCatalogName(x: Int): String = "Joe"
  def getColumnClassName(x: Int): String = "Joe"
  def getColumnCount(): Int = sch.length
  def getColumnDisplaySize(x: Int): Int = 20
  def getColumnLabel(x: Int): String = names(x)
  def getColumnName(x: Int): String = names(x)
  def getColumnType(x: Int): Int = sch(x)
  def getColumnTypeName(x: Int): String = sch(x) match {
    case java.sql.Types.BOOLEAN => "BOOLEAN"
    case java.sql.Types.INTEGER => "INTEGER"
    case java.sql.Types.DECIMAL => "DECIMAL"
    case java.sql.Types.DATE    => "DATE"
    case java.sql.Types.CHAR    => "CHAR"
  }
  def getPrecision(x: Int): Int = 100
  def getScale(x: Int): Int = 100
  def getSchemaName(x: Int): String = "Joe"
  def getTableName(x: Int): String = "Jim"
  def isAutoIncrement(x: Int): Boolean = false
  def isCaseSensitive(x: Int): Boolean = true
  def isCurrency(x: Int): Boolean = false
  def isDefinitelyWritable(x: Int): Boolean = false
  def isNullable(x: Int): Int = ResultSetMetaData.columnNullable
  def isReadOnly(x: Int): Boolean = true
  def isSearchable(x: Int): Boolean = true
  def isSigned(x: Int): Boolean = true
  def isWritable(x: Int): Boolean = false

  def isWrapperFor(x: Class[_]): Boolean = false
  def unwrap[T](x: Class[T]): T = sys.error("Unimplemented: Unwrap on CSV Result Set")
}


class CSVResults(src: Operator, names: java.util.Map[String, Int], sch: List[Int]) extends ResultSet 
{
  var done = false;
  var row: Array[Datum] = null;
  var idx = 0;
  
  def feature(msg: String) = 
    throw new SQLFeatureNotSupportedException(msg);
  def absolute(row: Int): Boolean = feature("absolute")
  def afterLast(): Unit = feature("afterLast")
  def beforeFirst(): Unit = feature("beforeFirst")
  def cancelRowUpdates(): Unit = feature("cancelRowUpdates")
  def clearWarnings(): Unit = feature("clearWarnings")
  def close(): Unit = { done = true; } 
  def deleteRow(): Unit = feature("deleteRow")
  def findColumn(columnLabel: String): Int = names.get(columnLabel)
  def first(): Boolean = feature("first")
  def getArray(columnIndex: Int): java.sql.Array = feature("array")
  def getArray(columnLabel: String): java.sql.Array = feature("array")
  def getAsciiStream(columnIndex: Int): InputStream = feature("getAsciiStream")
  def getAsciiStream(columnLabel: String): InputStream = feature("getAsciiStream")
  def getBigDecimal(columnIndex: Int): java.math.BigDecimal = feature("getBigDecimal")
  def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = feature("getBigDecimal")
  def getBigDecimal(columnLabel: String): java.math.BigDecimal = feature("getBigDecimal")
  def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = feature("getBigDecimal")
  def getBinaryStream(columnIndex: Int): InputStream = feature("getBinaryStream")
  def getBinaryStream(columnLabel: String): InputStream = feature("getBinaryStream")
  def getBlob(columnIndex: Int): Blob = feature("getBlob")
  def getBlob(columnLabel: String): Blob = feature("getBlob")
  def	getBoolean(columnIndex: Int): Boolean = row(columnIndex-1).toBool()
  def getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))
  def getByte(columnIndex: Int): Byte = row(columnIndex-1).toInt().toByte
  def getByte(columnLabel: String): Byte = getByte(findColumn(columnLabel))
  def getBytes(columnIndex: Int): Array[Byte] = row(columnIndex-1).toString().getBytes()
  def getBytes(columnLabel: String): Array[Byte] = getBytes(findColumn(columnLabel))
  def getCharacterStream(columnIndex: Int): Reader = feature("getCharacterStream")
  def getCharacterStream(columnLabel: String): Reader = feature("getCharacterStream")
  def getClob(columnIndex: Int): Clob = feature("getClob")
  def getClob(columnLabel: String): Clob = feature("getClob")
  def getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY
  def getCursorName(): String = "Bob"
  def getDate(columnIndex: Int): java.sql.Date = {
    row(columnIndex-1) match {
      case d: Datum.Date => 
        return java.sql.Date.valueOf(d.toString())
    }
  }
  def getDate(columnIndex: Int, cal: java.util.Calendar): java.sql.Date = getDate(columnIndex-1);
  def getDate(columnLabel: String): java.sql.Date = getDate(findColumn(columnLabel));
  def getDate(columnLabel: String, cal: java.util.Calendar): java.sql.Date = getDate(findColumn(columnLabel), cal);
  def getDouble(columnIndex: Int): Double = row(columnIndex-1).toFloat()
  def getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))
  def getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  def getFetchSize(): Int = 1
  def getFloat(columnIndex: Int): Float = row(columnIndex-1).toInt()
  def getFloat(columnLabel: String): Float = getFloat(findColumn(columnLabel))
  def getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT
  def getInt(columnIndex: Int): Int = row(columnIndex-1).toInt()
  def getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))
  def getLong(columnIndex: Int): Long = row(columnIndex-1).toInt()
  def getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))
  def getMetaData(): ResultSetMetaData = new CSVMetaData(names,sch)
  def getNCharacterStream(columnIndex: Int): Reader = feature("getNCharacterStream")
  def getNCharacterStream(columnLabel: String): Reader = feature("getNCharacterStream")
  def getNClob(columnIndex: Int): NClob = feature("getNClob")
  def getNClob(columnLabel: String): NClob = feature("getNClob")
  def getNString(columnIndex: Int): String = feature("getNString")
  def getNString(columnLabel: String): String = feature("getNString")
  def getObject(columnIndex: Int): Object = feature("getObject")
  def getObject[T](columnIndex: Int, ty: Class[T]): T = feature("getObject")
  def getObject(columnIndex: Int, map: java.util.Map[String, Class[_]]): Object = feature("getObject")
  def getObject(columnLabel: String): Object = feature("getObject")
  def getObject[T](columnLabel: String, ty: Class[T]): T = feature("getObject")
  def getObject(columnLabel: String, map: java.util.Map[String, Class[_]]): Object = feature("getObject")
  def getRef(columnIndex: Int): Ref = feature("getRef")
  def getRef(columnLabel: String): Ref = feature("getRef")
  def getRow(): Int = idx
  def getRowId(columnIndex: Int): RowId = feature("getRowId")
  def getRowId(columnLabel: String): RowId = feature("getRowId")
  def getShort(columnIndex: Int): Short = row(columnIndex-1).toInt().toShort
  def getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))
  def getSQLXML(columnIndex: Int): SQLXML = feature("getSQLXML")
  def getSQLXML(columnLabel: String): SQLXML = feature("getSQLXML")
  def getStatement(): Statement = null
  def getString(columnIndex: Int): String = row(columnIndex-1).toString()
  def getString(columnLabel: String): String = getString(findColumn(columnLabel))
  def getTime(columnIndex: Int): Time = feature("getTime")
  def getTime(columnIndex: Int, cal: Calendar): Time = feature("getTime")
  def getTime(columnLabel: String): Time = feature("getTime")
  def getTime(columnLabel: String, cal: Calendar): Time = feature("getTime")
  def getTimestamp(columnIndex: Int): Timestamp = feature("getTimestamp")
  def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = feature("getTimestamp")
  def getTimestamp(columnLabel: String): Timestamp = feature("getTimestamp")
  def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = feature("getTimestamp")
  def getType(): Int = ResultSet.TYPE_FORWARD_ONLY
  def getUnicodeStream(columnIndex: Int): InputStream = feature("getUnicodeStream")
  def getUnicodeStream(columnLabel: String): InputStream = feature("getUnicodeStream")
  def getURL(columnIndex: Int): URL = feature("getURL")
  def getURL(columnLabel: String): URL = feature("getURL")
  def getWarnings(): SQLWarning = null
  def insertRow(): Unit = feature("insertRow")
  def isAfterLast(): Boolean = (row == null)
  def isBeforeFirst(): Boolean = false;
  def isClosed(): Boolean = (done)
  def isFirst(): Boolean = (idx == 0)
  def isLast(): Boolean = src.done()
  def last(): Boolean = feature("last")
  def moveToCurrentRow(): Unit = feature("moveToCurrentRow")
  def moveToInsertRow(): Unit = feature("moveToInsertRow")
  def next(): Boolean = { row = src.read(); return row != null }
  def previous(): Boolean = feature("previous")
  def refreshRow(): Unit = {}
  def relative(rows: Int): Boolean = feature("relative")
  def rowDeleted(): Boolean = false
  def rowInserted(): Boolean = false
  def rowUpdated(): Boolean = false
  def setFetchDirection(direction: Int): Unit = feature("setFetchDirection")
  def setFetchSize(rows: Int): Unit = feature("setFetchSize")
  def updateArray(columnIndex: Int, x: java.sql.Array): Unit = feature("updateArray")
  def updateArray(columnLabel: String, x: java.sql.Array): Unit = feature("updateArray")
  def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = feature("updateAsciiStream")
  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = feature("updateAsciiStream")
  def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = feature("updateAsciiStream")
  def updateAsciiStream(columnLabel: String, x: InputStream): Unit = feature("updateAsciiStream")
  def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = feature("updateAsciiStream")
  def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = feature("updateAsciiStream")
  def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = feature("updateBigDecimal")
  def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = feature("updateBigDecimal")
  def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = feature("updateBinaryStream")
  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = feature("updateBinaryStream")
  def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = feature("updateBinaryStream")
  def updateBinaryStream(columnLabel: String, x: InputStream): Unit = feature("updateBinaryStream")
  def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = feature("updateBinaryStream")
  def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = feature("updateBinaryStream")
  def updateBlob(columnIndex: Int, x: Blob): Unit = feature("updateBlob")
  def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = feature("updateBlob")
  def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = feature("updateBlob")
  def updateBlob(columnLabel: String, x: Blob): Unit = feature("updateBlob")
  def updateBlob(columnLabel: String, inputStream: InputStream): Unit = feature("updateBlob")
  def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = feature("updateBlob")
  def updateBoolean(columnIndex: Int, x: Boolean): Unit = feature("updateBoolean")
  def updateBoolean(columnLabel: String, x: Boolean): Unit = feature("updateBoolean")
  def updateByte(columnIndex: Int, x: Byte): Unit = feature("updateByte")
  def updateByte(columnLabel: String, x: Byte): Unit = feature("updateByte")
  def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = feature("updateBytes")
  def updateBytes(columnLabel: String, x: Array[Byte]): Unit = feature("updateBytes")
  def updateCharacterStream(columnIndex: Int, x: Reader): Unit = feature("updateCharacterStream")
  def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = feature("updateCharacterStream")
  def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = feature("updateCharacterStream")
  def updateCharacterStream(columnLabel: String, reader: Reader): Unit = feature("updateCharacterStream")
  def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = feature("updateCharacterStream")
  def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = feature("updateCharacterStream")
  def updateClob(columnIndex: Int, x: Clob): Unit = feature("updateClob")
  def updateClob(columnIndex: Int, reader: Reader): Unit = feature("updateClob")
  def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = feature("updateClob")
  def updateClob(columnLabel: String, x: Clob): Unit = feature("updateClob")
  def updateClob(columnLabel: String, reader: Reader): Unit = feature("updateClob")
  def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = feature("updateClob")
  def updateDate(columnIndex: Int, x: java.sql.Date): Unit = feature("updateDate")
  def updateDate(columnLabel: String, x: java.sql.Date): Unit = feature("updateDate")
  def updateDouble(columnIndex: Int, x: Double): Unit = feature("updateDouble")
  def updateDouble(columnLabel: String, x: Double): Unit = feature("updateDouble")
  def updateFloat(columnIndex: Int, x: Float): Unit = feature("updateFloat")
  def updateFloat(columnLabel: String, x: Float): Unit = feature("updateFloat")
  def updateInt(columnIndex: Int, x: Int): Unit = feature("updateInt")
  def updateInt(columnLabel: String, x: Int): Unit = feature("updateInt")
  def updateLong(columnIndex: Int, x: Long): Unit = feature("updateLong")
  def updateLong(columnLabel: String, x: Long): Unit = feature("updateLong")
  def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = feature("updateNCharacterStream")
  def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = feature("updateNCharacterStream")
  def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = feature("updateNCharacterStream")
  def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = feature("updateNCharacterStream")
  def updateNClob(columnIndex: Int, nClob: NClob): Unit = feature("updateNClob")
  def updateNClob(columnIndex: Int, reader: Reader): Unit = feature("updateNClob")
  def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = feature("updateNClob")
  def updateNClob(columnLabel: String, nClob: NClob): Unit = feature("updateNClob")
  def updateNClob(columnLabel: String, reader: Reader): Unit = feature("updateNClob")
  def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = feature("updateNClob")
  def updateNString(columnIndex: Int, nString: String): Unit = feature("updateNString")
  def updateNString(columnLabel: String, nString: String): Unit = feature("updateNString")
  def updateNull(columnIndex: Int): Unit = feature("updateNull")
  def updateNull(columnLabel: String): Unit = feature("updateNull")
  def updateObject(columnIndex: Int, x: Object): Unit = feature("updateObject")
  def updateObject(columnIndex: Int, x: Object, scaleOrLength: Int): Unit = feature("updateObject")
  def updateObject(columnLabel: String, x: Object): Unit = feature("updateObject")
  def updateObject(columnLabel: String, x: Object, scaleOrLength: Int): Unit = feature("updateObject")
  def updateRef(columnIndex: Int, x: Ref): Unit = feature("updateRef")
  def updateRef(columnLabel: String, x: Ref): Unit = feature("updateRef")
  def updateRow(): Unit = feature("updateRow")
  def updateRowId(columnIndex: Int, x: RowId): Unit = feature("updateRowId")
  def updateRowId(columnLabel: String, x: RowId): Unit = feature("updateRowId")
  def updateShort(columnIndex: Int, x: Short): Unit = feature("updateShort")
  def updateShort(columnLabel: String, x: Short): Unit = feature("updateShort")
  def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = feature("updateSQLXML")
  def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = feature("updateSQLXML")
  def updateString(columnIndex: Int, x: String): Unit = feature("updateString")
  def updateString(columnLabel: String, x: String): Unit = feature("updateString")
  def updateTime(columnIndex: Int, x: Time): Unit = feature("updateTime")
  def updateTime(columnLabel: String, x: Time): Unit = feature("updateTime")
  def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = feature("updateTimestamp")
  def updateTimestamp(columnLabel: String, x: Timestamp): Unit = feature("updateTimestamp")
  def wasNull(): Boolean = feature("wasNull");
  
  def isWrapperFor(x: Class[_]): Boolean = false
  def unwrap[T](x: Class[T]): T = sys.error("Unimplemented: Unwrap on CSV Result Set")
}

object CSV {
  def apply(dataDir: String, schemaFile: String) = 
  {
    val mkDB = new Schema.Database();
    val mkTranslator = new edu.buffalo.cse562.sql.SqlToRA(mkDB)
    mkDB.dataDir = new File(dataDir);
    val parser = new CCJSqlParser(new FileReader(schemaFile))
    var table: CreateTable = parser.CreateTable();
    while(table != null){
      mkTranslator.loadTableSchema(table);
      table = parser.CreateTable()
    }
    new CSVBackend(mkDB, mkTranslator, new PlanCompiler(mkDB))
  }
}

class CSVBackend(
    db: Schema.Database, 
    translator: edu.buffalo.cse562.sql.SqlToRA,
    compiler: PlanCompiler
) extends Backend {
  def open() = {

  }
  def execute(sel: String): ResultSet = 
  {
    execute(new CCJSqlParser(new StringReader(sel)).Select());
  }
  def execute(sel: String, args: List[String]): ResultSet = 
  {
    throw new SQLException("No support for prepared queries");
  }
  override def execute(sel: Select): ResultSet = 
  {
    var plan = translator.selectToPlan(sel.getSelectBody())
    plan = Optimizer.optimize(plan, db)
    val sch = compiler.computeSchema(plan);
    var i = 0;
    new CSVResults(
      compiler.compile(plan),
      sch.map( (col) => { i += 1; ( col.getName(), i) } ).toMap[String,Int],
      sch.types().map( _ match { 
          case Schema.Type.BOOL => java.sql.Types.BOOLEAN
          case Schema.Type.INT  => java.sql.Types.INTEGER
          case Schema.Type.FLOAT => java.sql.Types.DECIMAL
          case Schema.Type.DATE => java.sql.Types.DATE
          case Schema.Type.STRING => java.sql.Types.CHAR
        }).toList
    )
  }

  def update(op: String): Unit = 
  {
    throw new SQLException("No support for updates on CSV data");
  }
  def update(upd: List[String]): Unit =
  {
    throw new SQLException("No support for updates on CSV data");
  }
  def update(op: String, args: List[String]): Unit = 
  {
    throw new SQLException("No support for updates on CSV data");
  }
  
  def getTableSchema(table: String): Option[List[(String, Type)]] =
  {
    throw new SQLException("No support for schemas on CSV data");
  }

  def getAllTables() =
  {
    throw new SQLException("No support for all tables on CSV data");
  }

  def close() = {

  }

  def specializeQuery(q: mimir.algebra.Operator) = q
}
