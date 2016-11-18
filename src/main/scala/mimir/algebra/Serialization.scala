package mimir.algebra;

import java.io._;
import mimir.ctables.VGTerm;

/**
 * Java-style serialization and VGTerms don't play nicely, since
 * the model object is embedded in each VGTerm.  As a result, we
 * separate out serialization of expressions and queries.
 * 
 * This also has the benefit of keeping serialization 
 * compartmentalized so that we can swap out a different, more 
 * effective approach later on.
 */

class Serialization(db: Database) {
  
  def serialize(oper: Operator): Array[Byte] =
  {
    val bytes = new ByteArrayOutputStream();
    val objects = new ObjectOutputStream(bytes);
    objects.writeObject(sanitize(oper))
    bytes.toByteArray()
  }

  def serialize(expr: Expression): Array[Byte] =
  {
    val bytes = new ByteArrayOutputStream();
    val objects = new ObjectOutputStream(bytes);
    objects.writeObject(sanitize(expr))
    bytes.toByteArray()
  }

  def sanitize(oper: Operator): Operator =
  {
    oper.recur(sanitize(_)).recurExpressions(sanitize(_))
  }

  def sanitize(expr: Expression): Expression =
  {
    expr match {
      case VGTerm((model, _), idx, args) => 
        SerializableVGTerm(model, idx, args.map(sanitize(_)))
      case _ => 
        expr.recur(sanitize(_))
    }
  }

  def desanitize(oper: Operator): Operator =
  {
    oper.recur(desanitize(_)).recurExpressions(desanitize(_))
  }

  def desanitize(expr: Expression): Expression =
  {
    expr match {
      case SerializableVGTerm(model, idx, args) => 
        VGTerm((model, db.lenses.modelForLens(model)), idx, args.map(desanitize(_)))
      case _ => 
        expr.recur(desanitize(_))
    }
  }

  def deserializeQuery(in:Array[Byte]): Operator =
  {
    val objects = new ObjectInputStream(ByteArrayInputStream(in))
    val query = objects.readObject().asInstanceOf[Operator];
    return desanitize(query);
  }

  def deserializeExpression(in:Array[Byte]): Expression =
  {
    val objects = new ObjectInputStream(ByteArrayInputStream(in))
    val expression = objects.readObject().asInstanceOf[Expression];
    return desanitize(expression);
  }
}

case class SerializableVGTerm(model: String, idx: Index, args: List[Expression]) extends Expression {
  def toString() = "{{ "+model+";"+idx+"["+args.mkString(", ")+"] }}"
  def children: List[Expression] = args
  def rebuild(x: List[Expression]) = SerializableVGTerm(model, idx, x)
}