package mimir.algebra.typeregistry

import mimir.algebra._

case class RegisteredType(name:String, constraints:Set[TypeConstraint], basedOn:Type = TString())

object DefaultTypeRegistry extends TypeRegistry with Serializable
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
  def supportsUserType(name:String): Boolean =
    typesByName contains name

  def parentOfUserType(t: TUser): Type = 
    getDefinition(t.name).basedOn

  def pickUserType(possibilities: Set[TUser]): Type = 
    possibilities.toSeq.sortBy(_.name).head

  def testForUserTypes(value: String, validBaseTypes:Set[BaseType]): Set[TUser] = 
  {
    // First find base types that match

    // And then find user-defined types that match
    validBaseTypes
      .map { 
        typesByBaseType.get(_).toSeq.flatten
          .filter { _.constraints.forall { _.test(value) } }
          .map { t => TUser(t.name) }
      }
      .flatten
      .toSet
  }
  def userTypeCaster(t: Type, target:Expression): Expression = 
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
  def userTypeForId(i: Integer) = TUser(types(i).name)
  def idForUserType(t: TUser): Integer = indexesByName(t.name)
  def getSerializable = this
}