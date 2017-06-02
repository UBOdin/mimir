package mimir.algebra;

case class RegisteredAggregate(
  aggName: String,
  typechecker: (Seq[Type] => Type)
){
  def typecheck(args: Seq[Type]) = typechecker(args)
}

object AggregateRegistry
{
  var prototypes: scala.collection.mutable.Map[String, RegisteredAggregate] = 
    scala.collection.mutable.Map.empty;

  {
    registerStatistic("SUM")
    registerStatistic("MAX")
    registerStatistic("MIN")
    registerStatistic("STDDEV")
    register("COUNT", (t) => TInt())
    register("AVG", List(TFloat()), TFloat())
    register("GROUP_AND", List(TBool()), TBool())
    register("GROUP_OR", List(TBool()), TBool())
    register("GROUP_BITWISE_AND", List(TInt()), TInt())
    register("GROUP_BITWISE_OR", List(TInt()), TInt())
    register("JSON_GROUP_ARRAY", (t) => TString())
    register("FIRST", (t:Seq[Type]) => t.head)
    register("FIRST_FLOAT", (t:Seq[Type]) => t.head)
    register("FIRST_INT", (t:Seq[Type]) => t.head)
  }

  def register(
    aggName: String, 
    typechecker: Seq[Type] => Type
  ): Unit = {
    prototypes.put(aggName, RegisteredAggregate(aggName, typechecker))
  }

  def registerStatistic(
    aggName: String
  ): Unit = {
    register(aggName, 
      (t) => { 
        if(t.isEmpty){
          throw new RAException(s"Invalid Call To $aggName(); No Args")
        }
        Typechecker.assertNumeric(t.head, Function(aggName, List())); t.head }
    )
  }

  def register(
    aggName: String,
    argTypes: Seq[Type],
    retType: Type
  ): Unit = {
    register(aggName, 
      t => { 
        if(t.length != argTypes.length){
          throw new RAException("Invalid arg list ["+aggName+"]: "+argTypes.mkString(", "))
        }
        t.zip(argTypes).foreach( t => Typechecker.escalate(t._1, t._2, aggName) ); 
        retType 
      }
    )
  }

  def typecheck(aggName: String, args: Seq[Type]): Type = 
    prototypes(aggName).typecheck(args)

  def isAggregate(aggName: String): Boolean =
    prototypes.keySet.contains(aggName)

}