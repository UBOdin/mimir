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

class GProMMedadataLookup(conn:Connection) extends org.gprom.jdbc.metadata_lookup.sqlite.SQLiteMetadataLookup(conn)
{
  var db: Database = null
  override def getFuncReturnType( fName:String, args: Array[String],
		  numArgs:Int) : String = {
		org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      println(s"Metadata lookup: $fName ( ${args.length} args )")
		  try {
  		  fName match{
  		    case "SUM" => "DT_INT"
  		    case "COUNT" => "DT_INT"
  		    case "&" => "DT_INT"
  		    case "MIMIR_MAKE_ROWID" => "DT_STRING"
  		    case _ => {
  		      val argTypes = args.map(arg => getMimirTypeFromGProMDataTypeString(arg)) 
            //println(s"Metadata lookup: function: $fName(${argSeq.mkString(",")})")
            getGProMDataTypeStringFromMimirType( fName match {
      		    case "sys_op_map_nonnull" => argTypes(0) 
      		    case _ => {
      		      db.typechecker.returnTypeOfFunction(fName,argTypes)
      		    }
      		  })
  		    }
  		  }
      } catch {
        case t: Throwable => {
          println(s"Metadata lookup: Exception: for function: $fName")
          println(t.toString())
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
}
