package mimir.algebra.function

import mimir.Database
import mimir.algebra._

case class RegisteredAggregate(
  aggName: ID,
  typechecker: (Seq[Type] => Type),
  defaultValue: PrimitiveValue
){
  def typecheck(args: Seq[Type]) = typechecker(args)
}

class AggregateRegistry
{
  var prototypes: scala.collection.mutable.Map[ID, RegisteredAggregate] = 
    scala.collection.mutable.Map.empty;

  {
    registerStatistic(ID("sum"), NullPrimitive())
    registerStatistic(ID("max"), NullPrimitive())
    registerStatistic(ID("min"), NullPrimitive())
    registerStatistic(ID("stddev"), NullPrimitive())
    register(ID("count"), (t) => TInt(), IntPrimitive(0))
    register(ID("avg"), List(TFloat()), TFloat(), NullPrimitive())
    register(ID("group_and"), List(TBool()), TBool(), BoolPrimitive(true))
    register(ID("group_or"), List(TBool()), TBool(), BoolPrimitive(false))
    register(ID("group_bitwise_and"), List(TInt()), TInt(), IntPrimitive(Long.MaxValue))
    register(ID("group_bitwise_or"), List(TInt()), TInt(), IntPrimitive(0))
    register(ID("json_group_array"), (t) => TString(), StringPrimitive("[]"))
    register(ID("first"), (t:Seq[Type]) => t.head, NullPrimitive())
    register(ID("first_float"), (t:Seq[Type]) => t.head, NullPrimitive())
    register(ID("first_int"), (t:Seq[Type]) => t.head, NullPrimitive())
  }

  def register(
    aggName: ID, 
    typechecker: Seq[Type] => Type, 
    defaultValue: PrimitiveValue
  ): Unit = {
    prototypes.put(aggName, RegisteredAggregate(aggName, typechecker, defaultValue))
  }

  def registerStatistic(
    aggName: ID, 
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
    aggName: ID,
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

  def typecheck(aggName: ID, args: Seq[Type]): Type = 
    prototypes(aggName).typecheck(args)

  def isAggregate(aggName: ID): Boolean =
    prototypes.keySet.contains(aggName)

  def defaultValue(aggName: ID): PrimitiveValue =
    prototypes(aggName).defaultValue

}