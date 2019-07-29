package mimir.exec.spark.udf

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.expressions.{ ScalaUDF, CreateStruct }

import mimir.algebra._
import mimir.models._
import mimir.exec.spark._

case class AckedUDF(oper:Operator, model:Model, idx:Int, args:Seq[org.apache.spark.sql.catalyst.expressions.Expression]) extends MimirUDF {
  val sparkArgs = args.toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => RAToSpark.getSparkType(arg))).toList.toSeq
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    try{
      model.argTypes(idx).
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
    }catch {
      case t: Throwable => throw new Exception(s"AckedUDF Error Extracting Args: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val argList = extractArgs(args.toSeq)
    model.isAcknowledged(idx, argList)
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        new java.lang.Boolean(model.isAcknowledged(idx, Seq()))
      }
      case 1 => (arg0:Any) => {
        val argList = extractArgs(Seq(arg0))
        model.isAcknowledged(idx, argList)
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val argList = extractArgs(Seq(arg0, arg1))
        model.isAcknowledged(idx, argList)
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2))
        model.isAcknowledged(idx, argList)
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3))
        model.isAcknowledged(idx, argList)
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4))
        model.isAcknowledged(idx, argList)
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        model.isAcknowledged(idx, argList)
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        model.isAcknowledged(idx, argList)
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        model.isAcknowledged(idx, argList)
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        model.isAcknowledged(idx, argList)
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        model.isAcknowledged(idx, argList)
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        model.isAcknowledged(idx, argList)
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11))
        model.isAcknowledged(idx, argList)
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        model.isAcknowledged(idx, argList)
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        model.isAcknowledged(idx, argList)
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        model.isAcknowledged(idx, argList)
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        model.isAcknowledged(idx, argList)
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        model.isAcknowledged(idx, argList)
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        model.isAcknowledged(idx, argList)
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        model.isAcknowledged(idx, argList)
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        model.isAcknowledged(idx, argList)
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        model.isAcknowledged(idx, argList)
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        model.isAcknowledged(idx, argList)
      }
      case x => varArgs _
    }  
    ScalaUDF(
      inFunc,
      BooleanType,
      if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
      Some(model.name.id),true,true)
  }
}

