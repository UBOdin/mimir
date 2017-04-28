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
import mimir.gprom.algebra.OperatorTranslation
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
import mimir.models.ModelManager
import mimir.Database
import mimir.models.ModelManager
import mimir.algebra.FloatPrimitive
import mimir.algebra.DatePrimitive
import mimir.algebra.TRowId
import mimir.algebra.TUser
import mimir.algebra.TDate
import mimir.algebra.TType
import mimir.algebra.TTimeStamp
import mimir.algebra.TimestampPrimitive
import mimir.algebra.StringPrimitive
import mimir.algebra.BoolPrimitive
import mimir.algebra.RowIdPrimitive
import mimir.algebra.NullPrimitive
import mimir.algebra.Expression
import mimir.algebra.PrimitiveValue
import mimir.ctables.VGTerm
import mimir.algebra.PrimitiveValue


class GProMMedadataLookup(conn:Connection) extends org.gprom.jdbc.metadata_lookup.sqlite.SQLiteMetadataLookup(conn)
{
  var db: Database = null
  var operator:mimir.algebra.Operator=null
  var fakeTuple: Map[String, PrimitiveValue] = null
  var gpischm : Seq[OperatorTranslation.MimirToGProMIntermediateSchemaInfo] = null
  def setOper(oper:mimir.algebra.Operator) : Unit = {
    operator = oper
    gpischm = OperatorTranslation.getSchemaForGProM(operator).map(si => {
      new OperatorTranslation.MimirToGProMIntermediateSchemaInfo(si.name, si.alias, si.attrName, si.attrName, si.attrProjectedName, si.attrType, si.attrPosition, si.attrFromClausePosition)
    })
    fakeTuple = generateFakeRow(db.bestGuessSchema(operator))
  }
  override def getFuncReturnType( fName:String, args: GProMList,
		  numArgs:Int) : String = {
		try {
      val argSeq = OperatorTranslation.gpromListToScalaList(args).map(arg => {
        OperatorTranslation.translateGProMExpressionToMimirExpression(new GProMNode.ByReference(arg.getPointer), gpischm)
      })
      
      println(s"Metadata lookup: function: $fName(${argSeq.mkString(",")})")
      val fc = mimir.algebra.Function(fName,argSeq)
      val tp = vgtFunctionType(fc);
      
      val gpt = getGProMDataTypeStringFromMimirType(tp)
      println(s"Metadata lookup: $tp -> $gpt")
      gpt
    } catch {
      case t: Throwable => {
        println(t.toString())
        t.printStackTrace()
        "DT_STRING"// TODO: handle error
      }
    }
	}
  
  def getGProMDataTypeStringFromMimirType(mimirType : Type) : String = {
    mimirType match {
        case TString() => "DT_STRING"
        case TBool() => "DT_BOOL"
        case TFloat() => "DT_FLOAT"
        case TInt() => "DT_INT"
        case _ => "DT_STRING"
      }
  }
  
  def generateFakeRow(schema:Seq[(String, Type)]) : Map[String, PrimitiveValue] = {
    schema.map( se => {
      (se._1, se._2 match {
        case TInt() => IntPrimitive(0)
        case TFloat() => FloatPrimitive(0.0)
        case TDate() => DatePrimitive(0,0,0)
        case TTimeStamp() => TimestampPrimitive(0,0,0,0,0,0)
        case TString() => StringPrimitive("")
        case TBool() => BoolPrimitive(false)
        case TRowId() => RowIdPrimitive("1")
        case TType() => TypePrimitive(TAny())
        case TAny() => NullPrimitive()
        case TUser(name) => StringPrimitive(name)
      })
    }).toMap
  }
  
  def vgtFunctionType(func: Expression) : Type = {
    replaceVGTerms(func) match {
      case prim:PrimitiveValue => prim.getType
      case expr => Typechecker.typeOf(expr,operator)  
     }
  }
  
   def replaceVGTerms(expr: Expression): Expression =
  {
    expr match {
      case mimir.algebra.Function("BEST_GUESS_VGTERM", fargs) => {
        replaceVGTermArg(expr)
      }
      case VGTerm(model, idx, args, hints) => 
        replaceVGTermArg(expr)
      case _ => 
        expr.recur(replaceVGTerms(_))
    }
  }
  
  def replaceVGTermArg(arg : Expression) : PrimitiveValue = {
    arg match {
      case mimir.algebra.Function("BEST_GUESS_VGTERM", fargs) => {
				// Special case BEST_GUESS_VGTERM
        val model = db.models.get(fargs(0).toString().replaceAll("'", ""))
        val idx = fargs(1).asInstanceOf[IntPrimitive].v.toInt;
       
        val vgtArgs =
          model.argTypes(idx).
            zipWithIndex.
            map( arg => replaceVGTermArg(fargs(arg._2+2)))
        val vgtHints = 
          model.hintTypes(idx).
            zipWithIndex.
            map( arg => replaceVGTermArg(fargs(arg._2+vgtArgs.length+2)))
        
        model.bestGuess(idx, vgtArgs, vgtHints)
      }
      case VGTerm(model,idx,vgtArgs, vgtHints) => {
        model.bestGuess(idx, vgtArgs.map(vgarg => replaceVGTermArg(vgarg)), vgtHints.map(vghint => replaceVGTermArg(vghint)))
      }
      case _ => Eval.eval(arg, fakeTuple)
    }
  }
}