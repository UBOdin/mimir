package mimir.optimizer.expression

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.algebra.function._
import mimir.algebra.Arith._
import mimir.algebra.Cmp._

class SimplifyExpressions(interpreter: Eval, functionRegistry: FunctionRegistry)
  extends BottomUpExpressionOptimizerRule
{

  def applyOne(originalExpression: Expression): Expression =
  {
    originalExpression match {
      //////////////////// Leaves ////////////////////
      case _:PrimitiveValue | _:Var | _:VGTerm | _:JDBCVar | _:RowIdVar => return originalExpression

      //////////////////// Arithmetic ////////////////////
      // Apply Arithmetic if possible
      case Arithmetic(op, lhs: PrimitiveValue, rhs: PrimitiveValue) => 
        Eval.applyArith(op, lhs, rhs)

      // Special case simplifications for AND operations
      case Arithmetic(And, BoolPrimitive(false), _) =>
        BoolPrimitive(false)
      case Arithmetic(And, _, BoolPrimitive(false)) =>
        BoolPrimitive(false)
      case Arithmetic(And, BoolPrimitive(true), rhs) =>
        rhs
      case Arithmetic(And, lhs, BoolPrimitive(true)) =>
        lhs

      // Special case simplifications for OR operations
      case Arithmetic(Or, BoolPrimitive(true), _) =>
        BoolPrimitive(true)
      case Arithmetic(Or, _, BoolPrimitive(true)) =>
        BoolPrimitive(true)
      case Arithmetic(Or, BoolPrimitive(false), rhs) =>
        rhs
      case Arithmetic(Or, lhs, BoolPrimitive(false)) =>
        lhs

      // All other forms of Arithmetic fall through
      case Arithmetic(_,_,_) => return originalExpression

      //////////////////// Comparison ////////////////////
      // Apply Comparisons if possible
      case Comparison(op, lhs: PrimitiveValue, rhs: PrimitiveValue) => 
        Eval.applyCmp(op, lhs, rhs)

      // Trivial comparison cases
      case Comparison(Cmp.Eq, a, b) if a.equals(b) => 
        BoolPrimitive(true)
      case Comparison(Cmp.Neq, a, b) if a.equals(b) => 
        BoolPrimitive(false)

      // All other forms of comparison fall through
      case Comparison(_,_,_) => return originalExpression

      //////////////////// Conditionals ////////////////////
      case Conditional(BoolPrimitive(true), thenClause, _) => thenClause
      case Conditional(BoolPrimitive(false), _, elseClause) => elseClause
      case Conditional(_, thenClause, elseClause) if thenClause.equals(elseClause) => thenClause
      case Conditional(_,_,_) => return originalExpression

      //////////////////// Unary Operators ////////////////////
      case Not(BoolPrimitive(x)) => BoolPrimitive(!x)
      case Not(_) => return originalExpression

      case IsNullExpression(NullPrimitive()) => BoolPrimitive(true)
      case IsNullExpression(_:PrimitiveValue) => BoolPrimitive(false)
      case IsNullExpression(_) => return originalExpression

      //////////////////// Function-Likes ////////////////////
      // If all args are present, and we have a registry, try to evaluate the function
      case Function(name, args) if args.forall { _.isInstanceOf[PrimitiveValue]} 
        && !functionRegistry.get(name).isInstanceOf[PassThroughNativeFunction]  =>
          interpreter.applyFunction(name, args.map { _.asInstanceOf[PrimitiveValue] })

      // A few function patterns are effectively no-ops and can be trivially removed
      case Function("MIMIR_MAKE_ROWID", Seq(x))            => x
      case Function("CAST", Seq(x, TypePrimitive(TAny()))) => x

      // Even if args aren't present, we might still be able to unfold the function
      case Function(name, args) => 
        functionRegistry.unfold(name, args) match {
          // We can!
          case Some(unfolded) => return unfolded
          // We can't!  Leave the original function
          case None => return originalExpression
        }

      // Procs behave like functions.  Evaluate them if possible...
      case p:Proc if p.args.forall { _.isInstanceOf[PrimitiveValue]} =>
        p.get( p.args.map { _.asInstanceOf[PrimitiveValue] } )

      // Otherwise, we can't do anything
      case _:Proc => return originalExpression

    }
  }
}