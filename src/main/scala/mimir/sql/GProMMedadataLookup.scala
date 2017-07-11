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
  var operator:mimir.algebra.Operator=null
  var fakeTuple: Map[String, PrimitiveValue] = null
  var gpischm : Seq[OperatorTranslation.MimirToGProMIntermediateSchemaInfo] = null
  def setOper(oper:mimir.algebra.Operator) : Unit = {
    operator = oper
    //gpischm = OperatorTranslation.extractTableSchemaForGProM(operator).map(si => {
    gpischm = OperatorTranslation.getSchemaForGProM(operator).map(si => {
      new OperatorTranslation.MimirToGProMIntermediateSchemaInfo(si.name, si.alias, si.attrName, si.attrName, si.attrProjectedName, si.attrType, si.attrPosition, si.attrFromClausePosition)
    })
    fakeTuple = generateFakeRow(db.bestGuessSchema(operator))
  }
  override def getFuncReturnType( fName:String, args: GProMList,
		  numArgs:Int) : String = {
		org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      try {
  		  fName match{
  		    case "SUM" => "DT_INT"
  		    case "COUNT" => "DT_INT"
  		    case "&" => "DT_INT"
  		    case "MIMIR_MAKE_ROWID" => "DT_STRING"
  		    case _ => {
  		      val argSeq = OperatorTranslation.gpromListToScalaList(args).map(arg => {
              OperatorTranslation.translateGProMExpressionToMimirExpression(new GProMNode.ByReference(arg.getPointer), gpischm)
            })
            
            //println(s"Metadata lookup: function: $fName(${argSeq.mkString(",")})")
            val tp = fName match {
      		    
      		    case "sys_op_map_nonnull" => db.typechecker.typeOf(argSeq(0), operator) 
      		    case _ => {
      		      val fc = mimir.algebra.Function(fName,argSeq) 
                vgtFunctionType(fc);
      		    }
      		  }
            
            val gpt = getGProMDataTypeStringFromMimirType(tp)
            //println(s"Metadata lookup: $tp -> $gpt")
            gpt
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
  
  def generateFakeRow(schema:Seq[(String, Type)]) : Map[String, PrimitiveValue] = {
    schema.map( se => {
      (se._1, se._2 match {
        case TInt() => IntPrimitive(0)
        case TFloat() => FloatPrimitive(0.0)
        case TDate() => DatePrimitive(0,0,0)
        case TTimestamp() => TimestampPrimitive(0,0,0,0,0,0,0)
        case TInterval() => IntervalPrimitive(0,0,0,0,0,0,0,0)
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
    replaceExpression(replaceVGTerms(func)) match {
      case TypePrimitive(t) => t
      case prim:PrimitiveValue => prim.getType
      case mimir.algebra.Function("CAST", args) => {
        args match {
          case x :: StringPrimitive(s) :: Nil => Type.toSQLiteType(Integer.parseInt(args(1).toString()))
          case x :: IntPrimitive(i) :: Nil   =>  Type.toSQLiteType(i.toInt)
  	      case x :: TypePrimitive(t)    :: Nil => t
        }
      }
      case mimir.algebra.Function("JSON_GROUP_ARRAY", args) => TString()
      case mimir.algebra.Function("FIRST", Seq(expr)) => db.typechecker.typeOf(expr,operator)  
      case mimir.algebra.Function("DISTINCT", Seq(expr)) => db.typechecker.typeOf(expr,operator)  
      case mimir.algebra.Function("DENSE_RANK", args) => { println(args.mkString(",")); TInt() } 
      case mimir.algebra.Function("ROW_NUMBER", args) => { println(args.mkString(",")); TInt() } 
      case expr => db.typechecker.typeOf(expr,operator)  
      
     }
  }
  
  def replaceExpression(expr:Expression) : Expression = {
    expr match {
      /*case mimir.algebra.Function("CAST", args) => {
        args match {
          case x :: StringPrimitive(s) :: Nil => TypePrimitive(Type.toSQLiteType(Integer.parseInt(args(1).toString())))
          case x :: IntPrimitive(i) :: Nil   =>  TypePrimitive(Type.toSQLiteType(i.toInt))
  	      case x :: TypePrimitive(t)    :: Nil => TypePrimitive(t)
        }
      }*/
      case Var("MIMIR_ROWID" | "MIMIR_ROWID_0") => RowIdPrimitive("1")
      case mimir.algebra.Function("MIMIR_MAKE_ROWID", args) => RowIdPrimitive("1")
      case x => x.recur(replaceExpression(_))
    }  
  }
  
   def replaceVGTerms(expr: Expression): Expression =
  {
    expr match {
      case mimir.algebra.Function(CTables.FN_BEST_GUESS | CTables.FN_IS_ACKED, _) => {
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
        
        val bg = model.bestGuess(idx, vgtArgs, vgtHints)
        bg match {
          case NullPrimitive() => {
            val varType = model.varType(idx, model.argTypes(idx))
            TypePrimitive(varType)
          }
          case _ => bg
        }
        
        /*val varType = model.varType(idx, model.argTypes(idx))
        TypePrimitive(varType)*/
      }
      case VGTerm(name,idx,vgtArgs, vgtHints) => {
        val model = db.models.get(name)
        val bg = model.bestGuess(idx, vgtArgs.map(vgarg => replaceVGTermArg(vgarg)), vgtHints.map(vghint => replaceVGTermArg(vghint)))
        bg match {
          case NullPrimitive() => {
            val varType = model.varType(idx, model.argTypes(idx))
            TypePrimitive(varType)
          }
          case _ => bg
        }
        //val varType = model.varType(idx, model.argTypes(idx))
        //TypePrimitive(varType)
      }
      case mimir.algebra.Function("ACKNOWLEDGED_VGTERM", fargs) => {
        TypePrimitive(TBool())
      }
      case Var("MIMIR_ROWID" | "MIMIR_ROWID_0") => RowIdPrimitive("1")
      case RowIdVar() => RowIdPrimitive("1")
      case mimir.algebra.Function("MIMIR_MAKE_ROWID",_) => RowIdPrimitive("1")
      case _ => db.interpreter.eval(arg, fakeTuple)
    }
  }
}
