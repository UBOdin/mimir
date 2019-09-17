package mimir.exec.spark.udf

import org.apache.spark.sql.types.{ ArrayType, StringType }
import org.apache.spark.sql.catalyst.expressions.{ ScalaUDF, CreateArray }

import mimir.algebra._
import mimir.models._
import mimir.exec.spark._
import org.apache.spark.sql.catalyst.expressions.KnownNotNull
import org.apache.spark.sql.catalyst.expressions.Literal

case class BestGuessUDF(oper:Operator, model:Model, idx:Int, args:Seq[org.apache.spark.sql.catalyst.expressions.Expression], hints:Seq[org.apache.spark.sql.catalyst.expressions.Expression]) extends MimirUDF {
  val sparkVarType = RAToSpark.getSparkType(model.varType(idx, model.argTypes(idx)))
  val (sparkArgsArgs,sparkArgsArgTypes) = args.zip(model.argTypes(idx)).toList.toSeq.map {
    case (Literal(null,_), _) => None
    case x => Some(x)
  }.flatMap(x => x).unzip
  val (sparkArgsHints,sparkArgsHintTypes) = hints.zip(model.hintTypes(idx)).toList.toSeq.map {
    case (Literal(null,_), _) => None
    case x => Some(x)
  }.flatMap(x => x).unzip
  
  val sparkArgs = (sparkArgsArgs ++ sparkArgsHints)
  val sparkArgTypes = (sparkArgsArgTypes ++ sparkArgsHintTypes).toList.toSeq.map(arg => RAToSpark.getSparkType(arg
      ))
  //println(s"-------------------------> BestGuessUDF: sparkVarType: $sparkVarType\n------------\nsparkArgs:${sparkArgs.mkString("\n")}\n--------------\nsparkArgTypes:${sparkArgTypes.mkString("\n")}")
      
  def extractArgsAndHints(args:Seq[Any]) : (Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
    try{
      val getParamPrimitive:(Type, Any) => PrimitiveValue = if(sparkArgs.length > 22) (t, v) => {
        v match {
          case null => NullPrimitive()
          case _ => Cast(t,StringPrimitive(v.asInstanceOf[String]))
        }
      }
      else getPrimitive _
        val argList =
         sparkArgsArgTypes.
          zipWithIndex.
          map( arg => getParamPrimitive(arg._1, args(arg._2)))
        val hintList = 
         sparkArgsHintTypes.
            zipWithIndex.
            map( arg => getParamPrimitive(arg._1, args(argList.length+arg._2)))
        (argList,hintList)  
    }catch {
      case t: Throwable => throw new Exception(s"BestGuessUDF Error Extracting Args and Hints: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val (argList, hintList) = extractArgsAndHints(args)
    //logger.debug(s"-------------------------> UDF Param overflow: args:${argList.mkString("\n")} hints:${hintList.mkString("\n")}")
    getNative(model.bestGuess(idx, argList, hintList))
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        getNative(model.bestGuess(idx, Seq(), Seq()))
      }
      case 1 => (arg0:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11 ))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case x => varArgs _
    }
    ScalaUDF(
      inFunc,
      sparkVarType,
      if(sparkArgs.length > 22) Seq(CreateArray(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(ArrayType(StringType)) else sparkArgTypes,
      Some(model.name.id),true, true)
  }
}
