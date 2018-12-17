package mimir.algebra.typeregistry

import mimir.algebra._

abstract class TypeRegistry
{
  def userTypeForId(i: Integer): TUser
  def idForUserType(t: TUser): Integer
  def supportsUserType(name:String): Boolean

  def testForUserTypes(record: String, validBaseTypes:Set[BaseType]): Set[TUser]
  def pickUserType(possibilities: Set[TUser]): Type
  def userTypeCaster(t:Type, target: Expression): Expression

  def parentOfUserType(t:TUser): Type
  def rootType(t:Type): BaseType =
    t match { 
      case u:TUser => rootType(parentOfUserType(u)); 
      case b:BaseType => b
    }
  def parentType(t:Type): Type =
    t match { 
      case u:TUser => parentOfUserType(u)
      case _:BaseType => t
    }

  def testForTypes(value:String): Set[Type] =
  {
    val validBaseTypes = 
      BaseType.tests
              .filter { _._2.findFirstMatchIn(value) != None }
              .map { _._1 }
              .toSet
    testForUserTypes(value, validBaseTypes) ++ validBaseTypes
  }
  def pickType(possibilities: Set[Type]): Type =
  {
    if(possibilities.isEmpty){ throw new RAException("Can't pick from empty list of types") }
    val userTypes = 
      possibilities.map { case x:TUser => Some(x) case _ => None }.flatten

    // User types beat everything else.  Use alpha order arbitrarily
    if(!userTypes.isEmpty){ return pickUserType(userTypes) }
    // TInt beats TFloat
    if(possibilities contains TInt()) { return TInt() }
    // Otherwise pick arbitrarily
    return possibilities.toSeq.sortBy(_.toString).head
  }

  def fromString(name:String) = 
  {
    BaseType.fromString(name)
            .getOrElse { 
              if(supportsUserType(name)){ TUser(name) }
              else { throw new RAException("Unsupported Type: "+name) }
            }
  }

  val idForBaseType = BaseType.idTypeOrder.zipWithIndex.toMap

  def typeForId(i:Integer) = 
    if(i < BaseType.idTypeOrder.size){ BaseType.idTypeOrder(i) }
    else { userTypeForId(i - BaseType.idTypeOrder.size) }
  def idForType(t:Type):Integer =
    t match { 
      case u:TUser => idForUserType(u)
      case b:BaseType => idForBaseType(b)
    }

  def getSerializable:(TypeRegistry with Serializable)

}
