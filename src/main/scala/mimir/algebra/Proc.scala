package mimir.algebra;

import java.sql._;

/**
 * A placeholder for use in extending Eval;  A proc is an expression that 
 * can be evaluated, but is not itself part of mimir's grammar.
 * 
 * The proc defines the method of evaluation.
 */
abstract class Proc(args: Seq[Expression]) extends Expression
{
  def getType(argTypes: Seq[Type]): Type
  def getArgs = args
  def children = args
  def get(v: Seq[PrimitiveValue]): PrimitiveValue
}
