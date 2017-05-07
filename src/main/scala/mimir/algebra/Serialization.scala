package mimir.algebra;

import java.io._;
import mimir.Database;
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
  
  val base64in = java.util.Base64.getDecoder()
  val base64out = java.util.Base64.getEncoder()

  def serialize(oper: Operator): String =
  {
    val bytes = new ByteArrayOutputStream();
    val objects = new ObjectOutputStream(bytes);
    objects.writeObject(sanitize(oper))
    base64out.encodeToString(bytes.toByteArray())
  }

  def serialize(expr: Expression): String =
  {
    val bytes = new ByteArrayOutputStream();
    val objects = new ObjectOutputStream(bytes);
    objects.writeObject(sanitize(expr))
    base64out.encodeToString(bytes.toByteArray())
  }

  def sanitize(oper: Operator): Operator =
  {
    oper.recur(sanitize(_)).recurExpressions(sanitize(_))
  }

  def sanitize(expr: Expression): Expression =
  {
    expr match {
      case VGTerm(model, idx, args, hints) => 
        SerializableVGTerm(model.name, idx, args.map(sanitize(_)), hints.map(sanitize(_)))
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
      case SerializableVGTerm(model, idx, args, hints) => 
        VGTerm(db.models.get(model), idx, args.map(desanitize(_)), hints.map(desanitize(_)))
      case _ => 
        expr.recur(desanitize(_))
    }
  }

  def deserializeQuery(in:String): Operator =
  {
    val objects = 
      new ObjectInputStream(new ByteArrayInputStream(base64in.decode(in))) {
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
          try { Class.forName(desc.getName, false, getClass.getClassLoader) }
          catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
        }
      }

    val query = objects.readObject().asInstanceOf[Operator];
    return desanitize(query);
  }

  def deserializeExpression(in:String): Expression =
  {
    val objects = new ObjectInputStream(new ByteArrayInputStream(base64in.decode(in))) {
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
          try { Class.forName(desc.getName, false, getClass.getClassLoader) }
          catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
        }
      }
    val expression = objects.readObject().asInstanceOf[Expression];
    return desanitize(expression);
  }
}

@SerialVersionUID(100L)
case class SerializableVGTerm(model: String, idx: Integer, args: Seq[Expression], hints: Seq[Expression]) extends Expression {
  override def toString() = "{{ "+model+";"+idx+"["+args.mkString(", ")+"]"+"["+hints.mkString(", ")+"] }}"
  def children: Seq[Expression] = args ++ hints
  def rebuild(x: Seq[Expression]) = {
    val (a, h) = x.splitAt(args.length)
    SerializableVGTerm(model, idx, a, h)
  }
}