package mimir.algebra;

import java.sql._;
import java.util.NoSuchElementException;
import com.typesafe.scalalogging.slf4j.LazyLogging

import Arith.{Add, Sub, Mult, Div, And, Or}
import Cmp.{Gt, Lt, Lte, Gte, Eq, Neq, Like, NotLike}

class MissingVariable(varName: String, e: Throwable) extends 
	Exception(varName, e)

/* what's going on with scope and Map().apply? */
class ExpressionChecker(scope: (String => Type) = Map().apply _) extends LazyLogging {
	/* Assert that the expressions claimed type is its type */
	def assert(e: Expression, t: Type, msg: String = "Typechecker"): Unit = {
		val eType = typeOf(e);
		if(Typechecker.escalate(eType, t, msg, e) != t){
			throw new TypeException(eType, t, msg, Some(e))
		}
	}

	def typeOf(e: Expression): Type = {
		e match {
			case p: PrimitiveValue => p.getType;
			case Not(child) => assert(child, TBool(), "NOT"); TBool()
			case p: Proc => p.getType(p.children.map(typeOf(_)))
			case Arithmetic(op @ (Add | Sub | Mult | Div), lhs, rhs) =>
				Typechecker.assertNumeric(Typechecker.escalate(typeOf(lhs), typeOf(rhs), op.toString, e), e);
			case Arithmetic((And | Or), lhs, rhs) =>
				assert(lhs, TBool(), "BoolOp");
				assert(rhs, TBool(), "BoolOp");
				TBool()
			case Comparison((Eq | Neq), lhs, rhs) =>
				Typechecker.escalate(typeOf(lhs), typeOf(rhs), "Comparison", e);
				TBool()
			case Comparison((Gt | Gte | Lt | Lte), lhs, rhs) =>
				if(typeOf(lhs) != TDate && typeOf(rhs) != TDate) {
					Typechecker.assertNumeric(Typechecker.escalate(typeOf(lhs), typeOf(rhs), "Comparison", e), e)
				}
				TBool()
			case Comparison((Like | NotLike), lhs, rhs) =>
				assert(lhs, TString(), "LIKE")
				assert(rhs, TString(), "LIKE")
				TBool()
			case Var(name) => 
				try { 
					val t = scope(name)
					logger.debug(s"Type of $name is $t")
					t
				} catch {
					case x:NoSuchElementException => throw new MissingVariable(name, x)
				}
			case JDBCVar(t) => t
			case Function("CAST", fargs) =>
				// Special case CAST
				Eval.inline(fargs(1)) match {
					case TypePrimitive(t) => t
					case p:PrimitiveValue => 
						throw new SQLException("Invalid CAST to '"+p+"' of type: "+typeOf(p))
					case _ => TAny()
				}
			case Function(fname, fargs) =>
				FunctionRegistry.typecheck(fname, fargs.map(typeOf(_)))
			case Conditional(condition, thenClause, elseClause) => 
				assert(condition, TBool(), "WHEN")
				Typechecker.escalate(
					typeOf(elseClause),
					typeOf(thenClause),
					"IF, ELSE CLAUSE", e
				)
			case IsNullExpression(child) =>
				typeOf(child);
				TBool()
			case RowIdVar() => TRowId()

    }
  }

}

object Typechecker {

	val simpleChecker = new ExpressionChecker();
	val weakChecker = new ExpressionChecker((_) => TAny())

	def typeOf(e: Expression): Type =
		{ simpleChecker.typeOf(e) }
	def weakTypeOf(e: Expression): Type =
		{ weakChecker.typeOf(e) }
	def typeOf(e: Expression, scope: (String => Type)): Type =
		{ (new ExpressionChecker(scope)).typeOf(e) }
	def typeOf(e: Expression, o: Operator): Type =
	{ 
		typecheckerFor(o).typeOf(e)
	}
	def typecheckerFor(o: Operator): ExpressionChecker =
	{
		val scope = schemaOf(o).toMap;
		new ExpressionChecker(scope(_))
	}

	def schemaOf(o: Operator): Seq[(String, Type)] =
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
				(new ExpressionChecker(srcSchema.toMap)).assert(cond, TBool(), "SELECT")
				srcSchema

			case Aggregate(groupBy, agggregates, source) =>
				/* Get child operator schema */
				val srcSchema = schemaOf(source)
				val chk = new ExpressionChecker(srcSchema.toMap)

				/* Get Group By Args and verify type */
				val groupBySchema: Seq[(String, Type)] = groupBy.map(x => (x.toString, chk.typeOf(x)) )

				/* Get function name, check for AVG *//* Get function parameters, verify type */
				val aggSchema: Seq[(String, Type)] = agggregates.map(x => 
					(
						x.alias, 
						AggregateRegistry.typecheck(x.function, x.args.map(chk.typeOf(_)))
					)
				)

				/* Send schema to parent operator */
				val sch = groupBySchema ++ aggSchema
				//println(sch)
				sch

			case Join(lhs, rhs) =>
				val lSchema = schemaOf(lhs);
				val rSchema = schemaOf(rhs);

				val overlap = lSchema.map(_._1).toSet & rSchema.map(_._1).toSet
				if(!(overlap.isEmpty)){
					throw new SQLException("Ambiguous Keys ('"+overlap+"') in Cross Product\n"+o);
				}
				lSchema ++ rSchema

			case LeftOuterJoin(lhs, rhs, condition) =>
				schemaOf(Select(condition, Join(lhs, rhs)))

			case Union(lhs, rhs) =>
				val lSchema = schemaOf(lhs);
				val rSchema = schemaOf(rhs);

				if(!(lSchema.map(_._1).toSet.equals(rSchema.map(_._1).toSet))){
					throw new SQLException("Schema Mismatch in Union\n"+o);
				}
				lSchema

			case Table(_, sch, meta) => (sch ++ meta.map( x => (x._1, x._3) ))

			case Limit(_, _, src) => schemaOf(src)

			case Sort(_, src) => schemaOf(src)
		}
	}

	def assertNumeric(t: Type, e: Expression): Type =
 	{
		if(escalate(t, TFloat(), "Numeric") != TFloat()){
			throw new TypeException(t, TFloat(), "Numeric", Some(e))
 		}
 		t;
 	}

	def escalate(a: Type, b: Type): Type =
		escalate(a, b, "Escalation")
	def escalate(a: Type, b: Type, msg: String, e: Expression): Type = 
		escalate(a, b, msg, Some(e))
	def escalate(a: Type, b: Type, msg: String): Type = 
		escalate(a, b, msg, None)
	def escalate(a: Type, b: Type, msg: String, e: Option[Expression]): Type = 
	{
		(a,b) match {
			case _ if a.equals(b) => a
			case (TUser(name),_) => escalate(TypeRegistry.baseType(name),b,msg)
			case (_,TUser(name)) => escalate(a,TypeRegistry.baseType(name),msg)
			case (TAny(),_) => b
			case (_,TAny()) => a
			case (TInt(), TInt()) => TInt()
			case ((TInt()|TFloat()), (TInt()|TFloat())) => TFloat()
			case _ => throw new TypeException(a, b, msg, e);
		}
	}

	def escalate(l: TraversableOnce[Type]): Type =
		escalate(l, "Escalation")
	def escalate(l: TraversableOnce[Type], msg: String): Type =
	{
		l.fold(TAny())(escalate(_,_,msg))
	}
	def escalate(l: TraversableOnce[Type], msg: String, e: Expression): Type =
	{
		l.fold(TAny())(escalate(_,_,msg,e))
	}
}