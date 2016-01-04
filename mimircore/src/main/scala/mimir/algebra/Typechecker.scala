package mimir.algebra;

import java.sql._;

import Type._;
import Arith.{Add, Sub, Mult, Div, And, Or}
import Cmp.{Gt, Lt, Lte, Gte, Eq, Neq, Like, NotLike}

class ExpressionChecker(scope: Map[String,Type.T] = Map()) {

	def assert(e: Expression, t: Type.T, msg: String = "Typechecker"): Unit = {
		val eType = typeOf(e);
		if(Typechecker.escalate(eType,t) != t){
			throw new TypeException(eType, t, msg)
		}
	}

	def typeOf(e: Expression): Type.T = {
		e match {
			case p: PrimitiveValue => p.getType;
			case Not(child) => assert(child, TBool, "NOT"); TBool
			case p: Proc => p.getType(p.children.map(typeOf(_)))
			case Arithmetic((Add | Sub | Mult | Div), lhs, rhs) =>
				Typechecker.assertNumeric(Typechecker.escalate(typeOf(lhs), typeOf(rhs)));
			case Arithmetic((And | Or), lhs, rhs) =>
				assert(lhs, TBool, "BoolOp");
				assert(rhs, TBool, "BoolOp");
				TBool
			case Comparison((Eq | Neq), lhs, rhs) => 
				Typechecker.escalate(typeOf(lhs), typeOf(rhs), "Comparison");
				TBool
			case Comparison((Gt | Gte | Lt | Lte), lhs, rhs) =>
				Typechecker.assertNumeric(Typechecker.escalate(typeOf(lhs), typeOf(rhs), "Comparison"));
				TBool
			case Comparison((Like | NotLike), lhs, rhs) =>
				assert(lhs, TString, "LIKE")
				assert(rhs, TString, "LIKE")
				TBool
			case Var(name) => scope(name)
			case Function("CAST", fargs) =>
				// Special case CAST
				Eval.inline(fargs(1)) match {
					case KeywordPrimitive(t, TType) => Type.fromString(t)
					case p:PrimitiveValue => 
						throw new SQLException("Invalid CAST to '"+p+"' of type: "+typeOf(p))
					case _ => TAny
				}
			case Function(fname, fargs) =>
				FunctionRegistry.typecheck(fname, fargs.map(typeOf(_)))
			case Conditional(condition, thenClause, elseClause) => 
				assert(condition, TBool, "WHEN")
				Typechecker.escalate(
					List(typeOf(thenClause), typeOf(elseClause))
				)
			case IsNullExpression(child) =>
				typeOf(child);
				TBool

    }
  }

}

object Typechecker {

	def typeOf(e: Expression): Type.T =
		{ (new ExpressionChecker()).typeOf(e) }
	def typeOf(e: Expression, scope: Map[String,Type.T]): Type.T =
		{ (new ExpressionChecker(scope)).typeOf(e) }

	def schemaOf(o: Operator): List[(String, Type.T)] =
	{ 
		o match {
			case Project(cols, src) =>
				val chk = new ExpressionChecker(schemaOf(src).toMap);
				cols.map( { 
						case ProjectArg(col, in) =>
							(col, chk.typeOf(in))
					})

			case Select(cond, src) =>
				val srcSchema = schemaOf(src);
				(new ExpressionChecker(srcSchema.toMap)).assert(cond, TBool, "SELECT")
				srcSchema

			case Join(lhs, rhs) =>
				val lSchema = schemaOf(lhs);
				val rSchema = schemaOf(rhs);

				val overlap = lSchema.map(_._1).toSet & rSchema.map(_._1).toSet
				if(!(overlap.isEmpty)){
					throw new SQLException("Ambiguous Keys ('"+overlap+"') in Cross Product\n"+o);
				}
				lSchema ++ rSchema

			case Union(lhs, rhs) =>
				val lSchema = schemaOf(lhs);
				val rSchema = schemaOf(rhs);

				if(!(lSchema.map(_._1).toSet.equals(rSchema.map(_._1).toSet))){
					throw new SQLException("Schema Mismatch in Union\n"+o);
				}
				lSchema

			case Table(_, sch, meta) => (sch ++ meta)

		}
	}

	def assertNumeric(t: Type.T): Type.T =
	{
		if(escalate(t, TFloat) != TFloat){
			throw new TypeException(t, TFloat, "Numeric")
		}
		t;
	}

	def escalate(a: Type.T, b: Type.T): Type.T = 
		escalate(a, b, "Escalation")
	def escalate(a: Type.T, b: Type.T, msg: String): Type.T = {
		(a,b) match {
			case (TAny,_) => b
			case (_,TAny) => a
			case (TInt, TInt) => TInt
			case ((TInt|TFloat), (TInt|TFloat)) => TFloat
			case _ => 
				if(a == b) { a } else {
					throw new TypeException(a, b, msg);
				}
		}
	}

	def escalate(l: List[Type.T]): Type.T = 
		escalate(l, "Escalation")
	def escalate(l: List[Type.T], msg: String): Type.T = 
	{
		l.fold(TAny)(escalate(_,_,msg))
	}
}