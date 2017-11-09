package mimir.algebra.function

import mimir.Database
import mimir.algebra._

case class RegisteredAggregate(
  aggName: String,
  typechecker: (Seq[Type] => Type),
  defaultValue: PrimitiveValue
){
  def typecheck(args: Seq[Type]) = typechecker(args)
}

class AggregateRegistry
{
  var prototypes: scala.collection.mutable.Map[String, RegisteredAggregate] = 
    scala.collection.mutable.Map.empty;

  {
    registerStatistic("SUM", NullPrimitive())
    registerStatistic("MAX", NullPrimitive())
    registerStatistic("MIN", NullPrimitive())
    registerStatistic("STDDEV", NullPrimitive())
    register("COUNT", (t) => TInt(), IntPrimitive(0))
    register("AVG", List(TFloat()), TFloat(), NullPrimitive())
    register("GROUP_AND", List(TBool()), TBool(), BoolPrimitive(true))
    register("GROUP_OR", List(TBool()), TBool(), BoolPrimitive(false))
    register("GROUP_BITWISE_AND", List(TInt()), TInt(), IntPrimitive(Long.MaxValue))
    register("GROUP_BITWISE_OR", List(TInt()), TInt(), IntPrimitive(0))
    register("JSON_GROUP_ARRAY", (t) => TString(), StringPrimitive("[]"))
    register("FIRST", (t:Seq[Type]) => t.head, NullPrimitive())
    register("FIRST_FLOAT", (t:Seq[Type]) => t.head, NullPrimitive())
    register("FIRST_INT", (t:Seq[Type]) => t.head, NullPrimitive())
  }

  def register(
    aggName: String, 
    typechecker: Seq[Type] => Type, 
    defaultValue: PrimitiveValue
  ): Unit = {
    prototypes.put(aggName, RegisteredAggregate(aggName, typechecker, defaultValue))
  }

  def registerStatistic(
    aggName: String, 
    defaultValue: PrimitiveValue
  ): Unit = {
    register(
      aggName, 
      (t) => { 
        if(t.isEmpty){
          throw new RAException(s"Invalid Call To $aggName(); No Args")
        }
        Typechecker.assertNumeric(t.head, Function(aggName, List())); t.head },
      defaultValue
    )
  }

  def register(
    aggName: String,
    argTypes: Seq[Type],
    retType: Type, 
    defaultValue: PrimitiveValue
  ): Unit = {
    register(
      aggName, 
      t => { 
        if(t.length != argTypes.length){
          throw new RAException("Invalid arg list ["+aggName+"]: "+argTypes.mkString(", "))
        }
        if(
          t.zip(argTypes).exists { t => Typechecker.leastUpperBound(t._1, t._2) == None }
        ){ 
          throw new RAException("Invalid arg list ["+aggName+"]: "+argTypes.mkString(", "))
        }
        retType
      },
      defaultValue
    )
  }

  def typecheck(aggName: String, args: Seq[Type]): Type = 
    prototypes(aggName).typecheck(args)

  def isAggregate(aggName: String): Boolean =
    prototypes.keySet.contains(aggName)

  def defaultValue(aggName: String): PrimitiveValue =
    prototypes(aggName).defaultValue

}