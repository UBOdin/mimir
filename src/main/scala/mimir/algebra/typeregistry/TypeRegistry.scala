package mimir.algebra.typeregistry

import mimir.algebra._

abstract class TypeRegistry
{
  def userTypeForId(i: Integer): TUser
  def idForUserType(t: TUser): Integer
  def supportsUserType(name:String): Boolean

  def testForUserTypes(record: String, validBaseTypes:Set[BaseType]): Set[TUser]
  def userTypeCaster(t:TUser, target: Expression, orElse: Expression): Expression

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
  def typeCaster(t: Type, target: Expression, orElse: Expression = NullPrimitive()): Expression =
  {
    val castTarget = Function("CAST", Seq(target, TypePrimitive(rootType(t))))
    t match {
      case u:TUser => userTypeCaster(u, castTarget, orElse)
      case b:BaseType => castTarget
    }
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
