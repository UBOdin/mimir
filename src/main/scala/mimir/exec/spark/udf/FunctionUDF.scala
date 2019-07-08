package mimir.exec.spark.udf

case class FunctionUDF(oper:Operator, name:String, function:RegisteredFunction, params:Seq[org.apache.spark.sql.catalyst.expressions.Expression], argTypes:Seq[Type]) extends MimirUDF {
  val sparkArgs = params.toList.toSeq
  val sparkArgTypes = argTypes.map(argT => OperatorTranslation.getSparkType(argT)).toList.toSeq
  val dataType = function match { 
    case NativeFunction(_, _, tc, _) => OperatorTranslation.getSparkType(tc(argTypes)) 
    case (_:ExpressionFunction | _:FoldFunction) => 
      throw new Exception(s"Unsupported function for Spark UDF: ${function}")
  }
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    try{
      argTypes.
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
    }catch {
      case t: Throwable => throw new Exception(s"FunctionUDF Error Extracting Args: \n\tModel: ${name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(evaluator:Seq[PrimitiveValue] => PrimitiveValue)(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val argList= extractArgs(args.toSeq)
    getNative(evaluator(argList))
  }
  def getUDF = {
    val inFunc = function match {
      case _:ExpressionFunction | _:FoldFunction => throw new Exception("Unsupported Function Type")
      case NativeFunction(_, evaluator, typechecker, _) => 
        sparkArgs.length match { 
          case 0 => () => {
            getNative(evaluator(Seq()))
          }
          case 1 => (arg0:Any) => {
            val argList = extractArgs(Seq(arg0))
            getNative(evaluator(argList))
          }
          case 2 => (arg0:Any, arg1:Any) => {
            val argList = extractArgs(Seq(arg0, arg1))
            getNative(evaluator(argList))
          }
          case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2))
            getNative(evaluator(argList))
          }
          case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3))
            getNative(evaluator(argList))
          }
          case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4))
            getNative(evaluator(argList))
          }
          case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
            getNative(evaluator(argList))
          }
          case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
            getNative(evaluator(argList))
          }
          case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
            getNative(evaluator(argList))
          }
          case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
            getNative(evaluator(argList))
          }
          case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
            getNative(evaluator(argList))
          }
          case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
            getNative(evaluator(argList))
          }
          case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11))
            getNative(evaluator(argList))
          }
          case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
            getNative(evaluator(argList))
          }
          case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
            getNative(evaluator(argList))
          }
          case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
            getNative(evaluator(argList))
          }
          case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
            getNative(evaluator(argList))
          }
          case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
            getNative(evaluator(argList))
          }
          case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
            getNative(evaluator(argList))
          }
          case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
            getNative(evaluator(argList))
          }
          case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
            getNative(evaluator(argList))
          }
          case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
            getNative(evaluator(argList))
          }
          case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
            getNative(evaluator(argList))
          }
          case x => varArgs(evaluator) _ 
        }
      }
      ScalaUDF(
        inFunc,
        dataType,
        if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
        org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
        if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
        Some(name), true, true)
  }
}
