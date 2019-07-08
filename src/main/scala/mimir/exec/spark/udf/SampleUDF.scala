package mimir.exec.spark.udf


case class SampleUDF(oper:Operator, model:Model, idx:Int, seed:Expression, args:Seq[org.apache.spark.sql.catalyst.expressions.Expression], hints:Seq[org.apache.spark.sql.catalyst.expressions.Expression]) extends MimirUDF {
  val sparkVarType = OperatorTranslation.getSparkType(model.varType(idx, model.argTypes(idx)))
  val sparkArgs = (args ++ hints).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg)) ++ model.hintTypes(idx).map(hint => OperatorTranslation.getSparkType(hint))).toList.toSeq
  
  def extractArgsAndHintsSeed(args:Seq[Any]) : (Long, Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
    try{
      val seedp = seed.toString().toLong
      val argList =
      model.argTypes(idx).
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
      val hintList = 
        model.hintTypes(idx).
          zipWithIndex.
          map( arg => getPrimitive(arg._1, args(argList.length+arg._2)))
     (seedp, argList,hintList)
   }catch {
      case t: Throwable => throw new Exception(s"SampleUDF Error Extracting Args and Hints: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*): Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val (seedi, argList, hintList) = extractArgsAndHintsSeed(args)
    getNative(model.sample(idx, seedi, argList, hintList))
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        getNative(model.sample(idx, 0, Seq(), Seq()))
      }
      case 1 => (arg0:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11 ))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case x => varArgs _
    }
    ScalaUDF(
      inFunc,
      sparkVarType,
      if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
      Some(model.name.id), true, true)
  }
}