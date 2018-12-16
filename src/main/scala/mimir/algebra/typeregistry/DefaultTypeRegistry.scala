package mimir.algebra.typeregistry

import mimir.algebra._

case class RegisteredType(name:String, constraints:Set[TypeConstraint], basedOn:Type = TString())

object DefaultTypeRegistry extends TypeRegistry 
{  
  val types:Seq[RegisteredType] = Seq(
    RegisteredType("credits",       Set(RegexpConstraint("^[0-9]{1,3}(\\.[0-9]{0,2})?$".r))),
    RegisteredType("email",         Set(RegexpConstraint("^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$".r))),
    RegisteredType("productid",     Set(RegexpConstraint("^P\\d+$".r))),
    RegisteredType("firecompany",   Set(RegexpConstraint("^[a-zA-Z]\\d{3}$".r))),
    RegisteredType("zipcode",       Set(RegexpConstraint("^\\d{5}(?:[-\\s]\\d{4})?$".r))),
    RegisteredType("container",     Set(RegexpConstraint("^[A-Z]{4}[0-9]{7}$".r))),
    RegisteredType("carriercode",   Set(RegexpConstraint("^[A-Z]{4}$".r))),
    RegisteredType("mmsi",          Set(RegexpConstraint("^MID\\d{6}|0MID\\d{5}|00MID\\{4}$".r))),
    RegisteredType("billoflanding", Set(RegexpConstraint("^[A-Z]{8}[0-9]{8}$".r))),
    RegisteredType("imo_code",      Set(RegexpConstraint("^\\d{7}$".r)))
  )
  val typesByName = types.map { t => t.name -> t }.toMap
  val typesByBaseType = types.groupBy { _.basedOn }
  val indexesByName = types.zipWithIndex.map { t => t._1.name -> t._2}.toMap

  def getDefinition(name:String): RegisteredType =
  {
    typesByName.get(name) match {
      case Some(definition) => definition
      case None => throw new RAException(s"Undefined user defined type: $name")
    }
  }
  def contains(name:String): Boolean =
    typesByName contains name

  def baseType(t: Type): Type = 
    t match { 
      case TUser(name) => getDefinition(name).basedOn
      case _ => t
    }
  def pickType(possibilities: Set[Type]): Type = 
  {
    if(possibilities.isEmpty){ throw new RAException("Can't pick from empty list of types") }
    val userTypes = 
      possibilities.map { case x:TUser => Some(x) case _ => None }.flatten

    // User types beat everything else.  Use alpha order arbitrarily
    if(!userTypes.isEmpty){ return userTypes.toSeq.sortBy(_.name).head }
    // TInt beats TFloat
    if(possibilities contains TInt()) { return TInt() }
    // Otherwise pick arbitrarily
    return possibilities.toSeq.sortBy(_.toString).head
  }
  def testForTypes(value: String): Set[Type] = 
  {
    // First find base types that match
    val validBaseTypes = 
      Type.tests
          .filter { _._2.findFirstMatchIn(value) != None }
          .map { _._1 }
          .toSet

    // And then find user-defined types that match
    validBaseTypes.map { 
            typesByBaseType(_) 
              .filter { _.constraints.forall { _.test(value) } }
              .map { t => TUser(t.name) }
      } .flatten
        .toSet ++ validBaseTypes
  }
  def typeCaster(t: Type, target:Expression): Expression = 
  {
    val castTarget = Function("CAST", Seq(target, TypePrimitive(rootType(t))))
    t match {
      case TUser(name) => {
        Conditional(
          ExpressionUtils.makeAnd(
            getDefinition(name)
              .constraints
              .map { _.tester(castTarget) }
          ),
          castTarget,
          NullPrimitive()
        )
      }
      case _ => castTarget
    }
  }
  def typeFromInt(i: Integer) = TUser(types(i).name)
  def typeToInt(t: TUser): Integer = indexesByName(t.name)

  def typeFromString(name: String) = TUser(getDefinition(name).name)
  def typeToString(t: TUser): String = t.name
}