package mimir.util

import org.apache.spark.sql.Row
import mimir.algebra._
import mimir.provenance.Provenance
import java.sql.SQLException
import java.util.Calendar
import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import mimir.exec.spark.RAToSpark
import scala.reflect.runtime.universe.{ runtimeMirror}
import org.spark_project.guava.reflect.ClassPath
import org.clapper.classutil.ClassFinder
import java.io.File
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.unix_timestamp

object SparkUtils {
  //TODO:there are a bunch of hacks in this conversion function because type conversion in operator translator
  //  needs to be done correctly
  def convertFunction(t: Type, field: Integer): (Row => PrimitiveValue) =
  {
    val checkNull: ((Row, => PrimitiveValue) => PrimitiveValue) = {
      (r, call) => {
        if(r.isNullAt(field)){ NullPrimitive() }
        else { call }
      }
    }
    
    t match {
      case TAny() =>        if(!ExperimentalOptions.isEnabled("NXNULL")) { (r) => NullPrimitive() } else throw new SQLException(s"Can't extract TAny: $field")
      case TFloat() =>      (r) => checkNull(r, FloatPrimitive(r.getDouble(field)))
      case TInt() =>        (r) => checkNull(r, { 
        try {
          IntPrimitive(r.getLong(field)) 
        } catch {
          case t: Throwable => {
            try {
              IntPrimitive(r.getInt(field)) 
            } catch {
              case t: Throwable => {
                val sval = r.getString(field)
                //TODO: somehow mimir_rowid is sometimes an int and has '-'
                //  from makeRowIDProjectArgs
                try {
                  if(sval.equalsIgnoreCase("-")) IntPrimitive(-1L) 
                  else IntPrimitive(r.getString(field).toLong) 
                }
                catch {
                  case t: Throwable => {
                    NullPrimitive()
                  }
                } 
              }
            }
          }
        } })

      case TString() =>     (r) => checkNull(r, { StringPrimitive(r.getString(field)) })
      case TRowId() =>      (r) => checkNull(r, { RowIdPrimitive(r.getString(field)) })
      case TBool() =>       (r) => checkNull(r, { 
        try {
          BoolPrimitive(r.getInt(field) != 0)
        } catch {
          case t: Throwable => {
            try {
              BoolPrimitive(r.getBoolean(field))
            } catch {
              case t: Throwable => {
                BoolPrimitive(r.getString(field).equalsIgnoreCase("true")) 
              }
            } 
          }
        } })
      case TType() =>       (r) => checkNull(r, { TypePrimitive(Type.fromString(r.getString(field))) })
      case TDate() =>       (r) => { val d = r.getDate(field); if(d == null){ NullPrimitive() } else { convertDate(d) } }
      case TTimestamp() =>  (r) => { 
                                val t = r.getTimestamp(field); 
                                if(t == null){ NullPrimitive() } 
                                else { convertTimestamp(t) } 
                              }
      case TInterval() => (r) => { TextUtils.parseInterval(r.getString(field)) }
      case TUser(t) => convertFunction(TypeRegistry.baseType(t), field)
    }
  }
  
  def convertField(t: Type, results: Row, field: Integer): PrimitiveValue =
  {
    convertFunction(
      t match {
        case TAny() => RAToSpark.getMimirType(results.schema.fields(field).dataType)
        case _ => t
      }, 
      field
    )(results)
  }
  
  def convertDate(time:Long): DatePrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(new Date(time))
    convertDate(cal)
  }
  def convertDate(c: Calendar): DatePrimitive =
    DatePrimitive(c.get(Calendar.YEAR), c.get(Calendar.MONTH)+1, c.get(Calendar.DATE))
  def convertDate(d: Date): DatePrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(d)
    convertDate(cal)
  }
  def convertDate(d: DatePrimitive): Date =
  {
    val cal = Calendar.getInstance()
    cal.set(d.y, d.m, d.d);
    new Date(cal.getTime().getTime());
  }
  def convertTimestamp(time:Long): TimestampPrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(new Timestamp(time))
    convertTimestamp(cal)
  }
  def convertTimestamp(c: Calendar): TimestampPrimitive =
    TimestampPrimitive(c.get(Calendar.YEAR), c.get(Calendar.MONTH)+1, c.get(Calendar.DATE),
                        c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), 
                        c.get(Calendar.MILLISECOND))
  def convertTimestamp(ts: Timestamp): TimestampPrimitive =
  {
    val cal = Calendar.getInstance();
    cal.setTime(ts)
    convertTimestamp(cal)
  }
  def convertTimestamp(ts: TimestampPrimitive): Timestamp =
  {
    val cal = Calendar.getInstance()
    cal.set(ts.y, ts.m, ts.d, ts.hh, ts.mm, ts.ss);
    new Timestamp(cal.getTime().getTime());
  }

  def extractAllRows(results: DataFrame): SparkDataFrameIterable =
    extractAllRows(results, RAToSpark.structTypeToMimirSchema(results.schema).map(_._2))    
  

  def extractAllRows(results: DataFrame, schema: Seq[Type]): SparkDataFrameIterable =
  {
    new SparkDataFrameIterable(results.collect().iterator, schema)
  }
  
  def getSparkKryoClasses() = { 
    val finder = ClassFinder(List(new File(".")))
    val classes = finder.getClasses  // classes is an Iterator[ClassInfo]
    val classMap = ClassFinder.classInfoMap(classes) // runs iterator out, once
    val models = ClassFinder.concreteSubclasses("mimir.models.Model", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    val operators = ClassFinder.concreteSubclasses("mimir.algebra.Operator", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    val expressions = ClassFinder.concreteSubclasses("mimir.algebra.Expression", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    (models ++ operators ++ expressions).toArray
  }
  
  def getDataFrameWithProvFromQuery(db:mimir.Database, query:Operator) : (Seq[(ID, Type)], DataFrame) = {
    val prov = Provenance.compile(query)
    val oper           = prov._1
    val provenanceCols:Seq[ID] = prov._2
    val operWProv = Project(query.columnNames.map { name => ProjectArg(name, Var(name)) } :+
        ProjectArg(Provenance.rowidColnameBase, 
            Function(Provenance.mergeRowIdFunction, provenanceCols.map( Var(_) ) )), oper )
    val dfPreOut = db.compiler.compileToSparkWithoutRewrites(operWProv)
    val dfOutDt = dfPreOut.schema.fields.filter(col => Seq(DateType).contains(col.dataType)).foldLeft(dfPreOut)((init, cur) => init.withColumn(cur.name,unix_timestamp(init(cur.name)).cast(LongType)*1000))
    val dfOut = dfOutDt.schema.fields.filter(col => Seq(TimestampType).contains(col.dataType)).foldLeft(dfOutDt)((init, cur) => init.withColumn(cur.name,init(cur.name).cast(LongType)*1000) )
    (db.typechecker.schemaOf(operWProv).map(el => el._2 match {
      //case TDate() => (el._1, TInt())
      //case TTimestamp() => (el._1, TInt())
      case _ => el
    }), dfOut)
  }
  
  def getDataFrameFromQuery(db:mimir.Database, query:Operator) : (Seq[(ID, Type)], DataFrame) = {
    val dfPreOut = db.compiler.compileToSparkWithRewrites(query)
    val dfOutDt = dfPreOut.schema.fields.filter(col => Seq(DateType).contains(col.dataType)).foldLeft(dfPreOut)((init, cur) => init.withColumn(cur.name,unix_timestamp(init(cur.name)).cast(LongType)*1000))
    val dfOut = dfOutDt.schema.fields.filter(col => Seq(TimestampType).contains(col.dataType)).foldLeft(dfOutDt)((init, cur) => init.withColumn(cur.name,init(cur.name).cast(LongType)*1000) )
    (db.typechecker.schemaOf(query), dfOut)
  }
}



class SparkDataFrameIterable(results: Iterator[Row], schema: Seq[Type]) 
  extends Iterator[Seq[PrimitiveValue]]
{
  def next(): List[PrimitiveValue] = 
  {
    val ret = schema.
          zipWithIndex.
          map( t => SparkUtils.convertField(t._1, results.next(), t._2) ).
          toList
    return ret;
  }

  def hasNext(): Boolean = results.hasNext
  def close(): Unit = {  }
  override def toList() = results.toList.map(row => schema.
          zipWithIndex.
          map(t => SparkUtils.convertField(t._1, row, t._2)))
  
  def flush: Seq[Seq[PrimitiveValue]] = 
  { 
    val ret = toList
    close()
    return ret
  }
}

