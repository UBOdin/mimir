package mimir.algebra;

import mimir.algebra.Type._

object Eval 
{
  def evalInt(e: Expression) =
    eval(e).asLong
  def evalString(e: Expression) =
    eval(e).asString
  def evalFloat(e: Expression) =
    eval(e).asDouble
  def evalBool(e: Expression): Boolean =
    eval(e) match {
      case BoolPrimitive(v) => v
      case v => throw new TypeException(TBool, v.exprType, "Cast")
    }
  def eval(e: Expression): PrimitiveValue = 
    eval(e, Map[String, PrimitiveValue]());
  def eval(e: Expression, 
           bindings: Map[String, PrimitiveValue]
  ): PrimitiveValue = 
  {
    if(e.isInstanceOf[PrimitiveValue]){
      return e.asInstanceOf[PrimitiveValue]
    } else {
      e match {
        case Var(v) => bindings.get(v).get
        case Arithmetic(op, lhs, rhs) =>
          applyArith(op, eval(lhs, bindings), eval(rhs, bindings))
        case Comparison(op, lhs, rhs) =>
          applyCmp(op, eval(lhs, bindings), eval(rhs, bindings))
        case CaseExpression(caseWhens, caseElse) =>
          caseWhens.foldLeft(None: Option[PrimitiveValue])( (a, b) =>
            if(a != None){ a }
            else {
              if(eval(b.when, bindings).
                    asInstanceOf[BoolPrimitive].v){
                Some(eval(b.then, bindings));
              } else { None }
            }
          ).getOrElse(eval(caseElse, bindings))
        case p: Proc => p.get()
        case IsNullExpression(c, n) => {
          val isNull: Boolean = 
            eval(c, bindings).
            isInstanceOf[NullPrimitive];
          if(n){ return BoolPrimitive(!isNull); }
          else { return BoolPrimitive(isNull); }
        }
      }
      
    }
  }
  
  def inline(e: Expression): Expression = 
    inline(e, Map[String, Expression]())
  def inline(e: Expression, bindings: Map[String, Expression]):
    Expression = 
  {
    e match {
      case Var(v) => bindings.get(v).getOrElse(Var(v))
      case _ => e.rebuild( e.children.map( inline(_, bindings) ) )
    }
  }
  
  def applyArith(op: Arith.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      NullPrimitive()
    } else {
      (op, Arith.computeType(op, a.exprType, b.exprType)) match { 
        case (Arith.Add, TInt) => 
          IntPrimitive(a.asLong + b.asLong)
        case (Arith.Add, TFloat) => 
          FloatPrimitive(a.asDouble + b.asDouble)
        case (Arith.Sub, TInt) => 
          IntPrimitive(a.asLong - b.asLong)
        case (Arith.Sub, TFloat) => 
          FloatPrimitive(a.asDouble - b.asDouble)
        case (Arith.Mult, TInt) => 
          IntPrimitive(a.asLong * b.asLong)
        case (Arith.Mult, TFloat) => 
          FloatPrimitive(a.asDouble * b.asDouble)
        case (Arith.Div, TInt) => 
          IntPrimitive(a.asLong / b.asLong)
        case (Arith.Div, TFloat) => 
          FloatPrimitive(a.asDouble / b.asDouble)
        case (Arith.And, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v &&
            b.asInstanceOf[BoolPrimitive].v
          )
        case (Arith.Or, TBool) => 
          BoolPrimitive(
            a.asInstanceOf[BoolPrimitive].v ||
            b.asInstanceOf[BoolPrimitive].v
          )
      }
    }
  }

  def applyCmp(op: Cmp.Op, 
            a: PrimitiveValue, b: PrimitiveValue
  ): PrimitiveValue = {
    if(a.isInstanceOf[NullPrimitive] || 
       b.isInstanceOf[NullPrimitive]){
      NullPrimitive()
    } else {
      Cmp.computeType(op, a.exprType, b.exprType)
      op match { 
        case Cmp.Eq => 
          BoolPrimitive(a.payload.equals(b.payload))
        case Cmp.Neq => 
          BoolPrimitive(!a.payload.equals(b.payload))
        case Cmp.Gt => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong > b.asLong)
            case TFloat => BoolPrimitive(a.asDouble > b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<0
              )
          }
        case Cmp.Gte => 
          Arith.escalateNumeric(a.exprType, b.exprType) match {
            case TInt => BoolPrimitive(a.asLong >= b.asLong)
            case TFloat => BoolPrimitive(a.asDouble >= b.asDouble)
            case TDate => 
              BoolPrimitive(
                a.asInstanceOf[DatePrimitive].
                 compare(b.asInstanceOf[DatePrimitive])<=0
              )
          }
        case Cmp.Lt => 
          applyCmp(Cmp.Gt, b, a) match { 
            case BoolPrimitive(x) => 
              BoolPrimitive(!x)
          }
        case Cmp.Lte => 
          applyCmp(Cmp.Gte, b, a) match { 
            case BoolPrimitive(x) => 
              BoolPrimitive(!x)
          }
      }
    }
  }
}