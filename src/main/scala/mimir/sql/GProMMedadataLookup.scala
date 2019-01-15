package mimir.sql

import java.sql.Connection
import oracle.net.aso.e
import java.sql.ResultSet
import oracle.net.aso.f
import java.sql.DatabaseMetaData
import java.sql.SQLException
import java.util.ArrayList
import mimir.algebra.Typechecker
import org.gprom.jdbc.jna.GProMList
import mimir.algebra.gprom.OperatorTranslation
import org.gprom.jdbc.jna.GProMNode
import mimir.algebra.TInt
import mimir.algebra.TFloat
import mimir.algebra.TBool
import mimir.algebra.TString
import mimir.algebra.Type
import mimir.algebra.TAny
import mimir.algebra.TypePrimitive
import mimir.algebra.IntPrimitive
import mimir.algebra.PrimitiveValue
import mimir.algebra.Eval
import mimir.algebra.VGTerm
import mimir.models.ModelManager
import mimir.Database
import mimir.models.ModelManager
import mimir.algebra.FloatPrimitive
import mimir.algebra.DatePrimitive
import mimir.algebra.TRowId
import mimir.algebra.TUser
import mimir.algebra.TDate
import mimir.algebra.TType
import mimir.algebra.TTimestamp
import mimir.algebra.TimestampPrimitive
import mimir.algebra.TInterval
import mimir.algebra.IntervalPrimitive
import mimir.algebra.StringPrimitive
import mimir.algebra.BoolPrimitive
import mimir.algebra.RowIdPrimitive
import mimir.algebra.NullPrimitive
import mimir.algebra.Expression
import mimir.algebra.PrimitiveValue
import mimir.algebra.PrimitiveValue
import mimir.algebra.RowIdVar
import mimir.algebra.Var
import mimir.ctables.CTables
import mimir.sql.sqlite.VGTermFunctions
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra.function.AggregateRegistry
import mimir.algebra.Arithmetic
import mimir.algebra.Arith
import mimir.algebra.Comparison
import mimir.algebra.Cmp
import scala.collection.JavaConverters._

class GProMMedadataLookup(conn:Connection) extends org.gprom.jdbc.metadata_lookup.sqlite.SQLiteMetadataLookup(conn) 
with LazyLogging
{
  var db: Database = null
  /**
    * @param fName
    * @param stringArray
    * @param numArgs
    * @return
    */
  override def getFuncReturnType( fName:String, args: Array[String],
		  numArgs:Int) : String = {
		org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      logger.debug(s"Metadata lookup: $fName ( ${args.length} args )")
		  try {
  		  fName match{
  		    case "SUM" => "DT_INT"
  		    case "COUNT" => "DT_INT"
  		    case "&" => "DT_INT"
  		    case "MIMIR_MAKE_ROWID" => "DT_STRING"
  		    case _ => {
  		      val argTypes = args.map(arg => getMimirTypeFromGProMDataTypeString(arg)) 
            //logger.debug(s"Metadata lookup: function: $fName(${argSeq.mkString(",")})")
            getGProMDataTypeStringFromMimirType( fName match {
      		    case "sys_op_map_nonnull" => argTypes(0)
      		    case "MIMIR_ENCODED_VGTERM" => db.typechecker.returnTypeOfFunction(VGTermFunctions.bestGuessVGTermFn,argTypes)
      		    case "UNCERT" => argTypes(0)
      		    case "LEAST" => db.typechecker.returnTypeOfFunction("MIN",argTypes) 
      		    case _ => {
      		      db.typechecker.returnTypeOfFunction(fName,argTypes)
      		    }
      		  })
  		    }
  		  }
      } catch {
        case t: Throwable => {
          logger.debug(s"Metadata lookup: Exception: for function: $fName")
          logger.debug(t.toString())
          t.printStackTrace()
          "DT_STRING"// TODO: handle error
        }
      }
		}
	}

	def getMimirTypeFromGProMDataTypeString(gpromTypeString : String) : Type = {
    gpromTypeString match {
        case "DT_VARCHAR2" => new TString()
        case "DT_BOOL" => new TBool()
        case "DT_FLOAT" => new TFloat()
        case "DT_INT" => new TInt()
        case "DT_LONG" => new TInt()
        case "DT_STRING" => new TString()
        case _ => new TAny()
      }
  }

  def getGProMDataTypeStringFromMimirType(mimirType : Type) : String = {
    mimirType match {
        case TString() => "DT_STRING"
        case TBool() => "DT_BOOL"
        case TFloat() => "DT_FLOAT"
        case TInt() => "DT_INT"
        case TRowId() => "DT_STRING"
        case TAny() => "DT_STRING"
        case _ => "DT_STRING"
      }
  }
  
  def logException(e:Exception) :Unit = {
    logger.error(e.toString())
  }
  
  //override all the methods
  /**
    * @param tableName
    * @return
    * @throws SQLException
    */
  override def getKeyInformation(tableName: String): java.util.List[String] =
    getKeyInformation(tableName, null)

  override protected def getKeyInformation(tableName: String,
                                  schema: String): java.util.List[String] = {
    List[String]().asJava//db.backend.getTableSchema(tableName)
  }

  /**
    * @param viewName
    * @return
    */
  override def viewExists(viewName: String): Int = db.views.get(viewName) match {
    case Some(_) => 1 
    case None => 0
  }

  /**
    * @param tableName
    * @return
    */
  override def tableExists(tableName: String): Int = db.tableExists(tableName) match {
    case true => 1 
    case false => 0
  }

  override def tableExistsForTypes(tableName: String, types: Array[String]): Boolean = db.tableSchema(tableName)  match {
    case None => false
    case Some(schema) => {
      types.toSeq.map(el => schema.unzip._2.contains(getMimirTypeFromGProMDataTypeString(el))).fold(true)((in,cur) => in && cur)
    }
  }
  

  /**
    * @param functionName
    * @return
    */
  //override def isWindow(functionName: String): Int

  /**
    * @param functionName
    * @return
    */
  override def isAgg(functionName: String): Int = db.aggregates.isAggregate(functionName) match {
    case true => 1 
    case false => 0
  }

  /**
    * @param oName
    * @param stringArray
    * @param numArgs
    * @return
    */
  override def getOpReturnType(oName: String,
                      stringArray: Array[String],
                      numArgs: Int): String = getGProMDataTypeStringFromMimirType(db.typechecker.weakTypeOf({
                        val primitives = stringArray.map(typStr => getMimirTypeFromGProMDataTypeString(typStr)).map(typ => {
                          typ match {
                            case TInt() => IntPrimitive(0l)
                            case TFloat() => FloatPrimitive(0)
                            case TString() => StringPrimitive("")
                            case TBool() => BoolPrimitive(false)
                            case TRowId() => RowIdPrimitive("0")
                            case TType() => TypePrimitive(TInt())
                          }
                        })
                        oName match {
                          case "+" => new Arithmetic( Arith.Add, primitives(0), primitives(1))
                          case "-" => new Arithmetic( Arith.Sub, primitives(0),primitives(1))
                          case "*" => new Arithmetic( Arith.Mult, primitives(0),primitives(1))
                          case "/" => new Arithmetic( Arith.Div, primitives(0),primitives(1))
                          case "&" => new Arithmetic( Arith.BitAnd, primitives(0),primitives(1))
                          case "|" => new Arithmetic( Arith.BitOr, primitives(0),primitives(1))
                          case "AND" => new Arithmetic( Arith.And, primitives(0) ,primitives(1))
                          case "OR" => new Arithmetic( Arith.Or, primitives(0), primitives(1))
                          case "=" => new Comparison( Cmp.Eq , primitives(0), primitives(1))
                          case "<>" => new Comparison( Cmp.Neq, primitives(0),primitives(1))
                          case ">" => new Comparison( Cmp.Gt, primitives(0),primitives(1)) 
                          case ">=" => new Comparison( Cmp.Gte ,primitives(0),primitives(1))
                          case "<" => new Comparison( Cmp.Lt ,primitives(0),primitives(1))
                          case "<=" => new Comparison( Cmp.Lte , primitives(0),primitives(1))
                          case "LIKE" => new Comparison( Cmp.Like , primitives(0),primitives(1))
                          case "NOT LIKE" => new Comparison( Cmp.NotLike, primitives(0),primitives(1))
                          case x => throw new Exception(s"GProMMedadataLookup: getOpReturnType: Cant Translate Op: $oName")
                      }}))

  /**
    * @param viewName
    * @return the SQL defining viewName
    */
  //override def getViewDefinition(viewName: String): String

  /**
    * @param tableName
    * @return
    */
  //override def getTableDef(tableName: String): String

  
  

  /**
    * @param tableName
    * @return
    */
  override def getAttributeDTs(tableName: String): java.util.List[String] =
    getAttributeDTs(tableName, null)

  override protected def getAttributeDTs(tableName: String,
                                schema: String): java.util.List[String] = db.tableSchema(tableName)  match {
    case None => throw new Exception(s"GProMMedadataLookup: getAttributeDTs: No Table: $tableName")
    case Some(schema) => {
      schema.unzip._2.map(getGProMDataTypeStringFromMimirType(_)).toList.asJava
    }
  }

  /**
    * @param tableName
    * @return
    */
  override def getAttributeNames(tableName: String): java.util.List[String] = 
    getAttributeNames(tableName, null)

  override protected def getAttributeNames(tableName: String,
                                  schema: String): java.util.List[String] = db.tableSchema(tableName)  match {
    case None => throw new Exception(s"GProMMedadataLookup: getAttributeDTs: No Table: $tableName")
    case Some(schema) => {
      schema.unzip._1.toList.asJava
    }
  }

  /**
    *
    * @return
    */
  override def openConnection(): Int = 0

  /**
    *
    * @return
    */
  override def closeConnection(): Int = 0

  /**
    *
    * @param schema
    * @param tableName
    * @param attrName
    * @return
    */
  override def getAttrDefValue(schema: String,
                      tableName: String,
                      attrName: String): String = db.tableSchema(tableName)  match {
    case None => throw new Exception(s"GProMMedadataLookup: getAttributeDTs: No Table: $tableName")
    case Some(schema) => {
      getGProMDataTypeStringFromMimirType(schema.toMap.get(attrName).get)
    }
  }

  override protected def sqlToGpromDT(dt: String): String = {
    if (dt.==("VARCHAR") || dt.==("VARCHAR2")) {
      "DT_STRING"
    }
    if (dt.==("INT") || dt.==("INTEGER")) {
      "DT_INT"
    }
    if (dt.==("DECIMAL")) {
      "DT_INT"
    }
//TODO
    "DT_STRING"
  }

  override protected def listToString(in: java.util.List[String]): String = in.asScala.mkString(",")
  
}
