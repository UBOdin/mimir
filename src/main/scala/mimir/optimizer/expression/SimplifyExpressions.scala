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
      case _:PrimitiveValue | _:Var | _:RowIdVar
         | _:IsAcknowledged | _:Caveat => return originalExpression

      //////////////////// Arithmetic ////////////////////

      // Special case simplifications for AND operations
      case Arithmetic(And, BoolPrimitive(false), _) =>
        return BoolPrimitive(false)
      case Arithmetic(And, _, BoolPrimitive(false)) =>
        return BoolPrimitive(false)
      case Arithmetic(And, BoolPrimitive(true), rhs) =>
        return rhs
      case Arithmetic(And, lhs, BoolPrimitive(true)) =>
        return lhs

      // Special case simplifications for OR operations
      case Arithmetic(Or, BoolPrimitive(true), _) =>
        return BoolPrimitive(true)
      case Arithmetic(Or, _, BoolPrimitive(true)) =>
        return BoolPrimitive(true)
      case Arithmetic(Or, BoolPrimitive(false), rhs) =>
        return rhs
      case Arithmetic(Or, lhs, BoolPrimitive(false)) =>
        return lhs

      // Null Values
      case Arithmetic(_,NullPrimitive(),_) => return NullPrimitive()
      case Arithmetic(_,_,NullPrimitive()) => return NullPrimitive()

      // Apply Arithmetic if possible
      case Arithmetic(op, lhs: PrimitiveValue, rhs: PrimitiveValue) => 
        return Eval.applyArith(op, lhs, rhs)

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

      // Comparisons with NULLs
      case Comparison(_, NullPrimitive(), _) => 
        NullPrimitive()
      case Comparison(_, _, NullPrimitive()) => 
        NullPrimitive()


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
        && !(functionRegistry.get(name) match { case NativeFunction(_,_,_,pt) => pt; case _ => false })  =>
          interpreter.applyFunction(name, args.map { _.asInstanceOf[PrimitiveValue] })

      // A few function patterns are effectively no-ops and can be trivially removed
      case Function(ID("mimir_make_rowid"), Seq(x))            => x


      // Even if args aren't present, we might still be able to unfold the function
      case Function(name, args) => 
        functionRegistry.unfold(name, args) match {
          // We can!
          case Some(unfolded) => return unfolded
          // We can't!  Leave the original function
          case None => return originalExpression
        }

      case CastExpression(x, TAny())                           => x
      case CastExpression(x:PrimitiveValue, t)                 => Cast(t, x)
      case CastExpression(_, _)                                => originalExpression
      
      // Procs behave like functions.  Evaluate them if possible...
      case p:Proc if p.args.forall { _.isInstanceOf[PrimitiveValue]} =>
        p.get( p.args.map { _.asInstanceOf[PrimitiveValue] } )

      // Otherwise, we can't do anything
      case _:Proc => return originalExpression

    }
  }
}