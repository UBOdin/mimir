package mimir.util

import org.apache.spark.sql.Row
import mimir.algebra._
import java.sql.SQLException
import java.util.Calendar
import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import mimir.algebra.spark.OperatorTranslation
import scala.reflect.runtime.universe.{ runtimeMirror}
import org.spark_project.guava.reflect.ClassPath
import org.clapper.classutil.ClassFinder
import java.io.File

object SparkUtils {
  //TODO:there are a bunch of hacks in this conversion function because type conversion in operator translator
  //  needs to be done correctly
  def convertFunction(t: Type, field: Integer, dateType: Type = TDate()): (Row => PrimitiveValue) =
  {
    val checkNull: ((Row, => PrimitiveValue) => PrimitiveValue) = {
      (r, call) => {
        if(r.isNullAt(field)){ NullPrimitive() }
        else { call }
      }
    }
    
    t match {
      case TAny() =>        throw new SQLException(s"Can't extract TAny: $field")
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
      case TDate() =>
        dateType match {
          case TDate() =>   (r) => { val d = r.getDate(field); if(d == null){ NullPrimitive() } else { convertDate(d) } }
          case TString() => (r) => { 
              val d = r.getString(field)
              if(d == null){ NullPrimitive() } 
              else { TextUtils.parseDate(d) }
            }
          case _ =>         throw new SQLException(s"Can't extract TDate as $dateType")
        }
      case TTimestamp() => 
        dateType match {
          case TDate() =>   (r) => { 
              val t = r.getTimestamp(field); 
              if(t == null){ NullPrimitive() } 
              else { convertTimestamp(t) } 
            }
          case TString() => (r) => {
              val t = r.getString(field)
              if(t == null){ NullPrimitive() }
              else { TextUtils.parseTimestamp(t) }
            }
          case _ =>         throw new SQLException(s"Can't extract TTimestamp as $dateType")

        }
      case TInterval() => (r) => { TextUtils.parseInterval(r.getString(field)) }
      case TUser(t) => convertFunction(TypeRegistry.baseType(t), field, dateType)
    }
  }
  
  def convertField(t: Type, results: Row, field: Integer, dateType: Type = TString()): PrimitiveValue =
  {
    convertFunction(
      t match {
        case TAny() => OperatorTranslation.getMimirType(results.schema.fields(field).dataType)
        case _ => t
      }, 
      field, 
      dateType
    )(results)
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
    extractAllRows(results, OperatorTranslation.structTypeToMimirSchema(results.schema).map(_._2))    
  

  def extractAllRows(results: DataFrame, schema: Seq[Type]): SparkDataFrameIterable =
  {
    new SparkDataFrameIterable(results.collect().iterator, schema)
  }
  
  def getSparkKryoClasses() = { 
     /*val m = runtimeMirror(classOf[mimir.models.Model].getClassLoader)
     val mm = m.staticPackage("mimir.models")
     val ma = m.staticPackage("mimir.algebra")
    
     (mm.typeSignature.decls.collect {
        case c: scala.reflect.runtime.universe.ClassSymbol  =>
          Class.forName(c.fullName)
      } ++
     ma.typeSignature.decls.collect {
        case c: scala.reflect.runtime.universe.ClassSymbol  =>
          Class.forName(c.fullName)
      } ).map(clazz => {
       println(clazz.getName)
       clazz
     }).toArray*/
     
     
     /*val loader = classOf[mimir.models.Model].getClassLoader//Thread.currentThread().getContextClassLoader()
     val classes = ClassPath.from(loader).getTopLevelClasses().toArray().asInstanceOf[Array[ClassPath.ClassInfo]]
     
     println("---------------------------------------------------")
     println(classes)
     
     classes.flatMap { info => 
      if (info.getName().startsWith("mimir.models") || info.getName().startsWith("mimir.algebra")) {
        println(info.getName)
        Some(Class.forName(info.getName)):Option[Class[_]]
      } else None
    }*/
    
    /*def knownSubclasses(baseClass:scala.reflect.runtime.universe.ClassSymbol): Set[scala.reflect.runtime.universe.ClassSymbol] = {
      val baseClasses = baseClass.knownDirectSubclasses.map(_.asClass)
      baseClasses ++ baseClasses.flatMap(cls => knownSubclasses(cls.asClass)) 
    }
    
    println("---------------------------------------------------")
    val tpe = scala.reflect.runtime.universe.typeOf[mimir.models.Model]
    val clazz = tpe.typeSymbol.asClass
    println(tpe.typeSymbol)
    val tpeo = scala.reflect.runtime.universe.typeOf[mimir.algebra.Operator]
    val clazzo = tpeo.typeSymbol.asClass
    val tpee = scala.reflect.runtime.universe.typeOf[mimir.algebra.Expression]
    val clazze = tpee.typeSymbol.asClass
    val mimirClassSymbols = (knownSubclasses(clazz) ++ knownSubclasses(clazzo) ++ knownSubclasses(clazze))
    println(mimirClassSymbols)
    mimirClassSymbols.map(clazzes => {
      println(clazzes.asClass.fullName)
      Class.forName(clazzes.asClass.fullName)
    }).toArray*/
    
    
    val finder = ClassFinder(List(new File(".")))
    val classes = finder.getClasses  // classes is an Iterator[ClassInfo]
    val classMap = ClassFinder.classInfoMap(classes) // runs iterator out, once
    val models = ClassFinder.concreteSubclasses("mimir.models.Model", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    val operators = ClassFinder.concreteSubclasses("mimir.algebra.Operator", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    val expressions = ClassFinder.concreteSubclasses("mimir.algebra.Expression", classMap).map(clazz => Class.forName(clazz.name)).toSeq
    (models ++ operators ++ expressions).toArray
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

