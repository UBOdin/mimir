package mimir.algebra.typeregistry

import mimir.algebra._

abstract class TypeRegistry
{
  def typeFromString(name: String): TUser
  def typeToString(t: TUser): String
  def typeFromInt(i: Integer): Type
  def typeToInt(t: TUser): Integer
  def contains(name:String): Boolean

  def testForTypes(record: String): Set[Type]
  def pickType(possibilities: Set[Type]): Type
  def typeCaster(t:Type, target: Expression): Expression

  def baseType(t:Type): Type
  def rootType(t:Type): BaseType =
    t match { 
      case u:TUser => rootType(baseType(u)); 
      case TAny() => throw new RAException("TAny doesn't have a root type")
      case b:BaseType => b
    }
}