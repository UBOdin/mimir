package mimir.sql

/**
  * Created by Will on 6/29/2016.
  *
  * This is a class for checking IS NULL expression, the idea is to look for cast errors where a value is cast to 0.0 instead of null
  */

  import mimir.algebra._
  import mimir.parser.OperatorParser


  import mimir.Database

  object IsNullChecker {
    var isNull = false;
    var oper:OperatorParser = null;
    var isNullExpression:net.sf.jsqlparser.expression.operators.relational.IsNullExpression = null;
    var lookingFrom = true;
    var from:String = null;
    var db:Database = null;
    var numberProblemRows = 0;

    def setIsNull(b:Boolean):Unit = {isNull = b;}
    def getIsNull():Boolean = {isNull;}

    def setOper(o:OperatorParser):Unit = {oper = o;}
    def getOper():OperatorParser = oper;

    def setIsNullExpression(e:net.sf.jsqlparser.expression.operators.relational.IsNullExpression):Unit = {isNullExpression = e}
    def getIsNullExpression():net.sf.jsqlparser.expression.operators.relational.IsNullExpression = {isNullExpression}

    def lookingForFrom():Boolean = {lookingFrom}
    def setLookingForFrom(b:Boolean):Unit = {lookingFrom = b}

    def setFrom(s:String):Unit = {from = s}
    def getFrom():String = {from}

    def setDB(d:Database):Unit = {db = d}
    def getDB():Database = {db}

    def isNullCheckStencil():String = { // is the stencil I made for the query to check, could be changed later
    val column:String = (isNullExpression.getLeftExpression).toString();
      "Select ((Select Count(secretcodenamefortable) from (select cast(" + column + " as float) as secretcodenamefortable from " + from + ") WHERE secretcodenamefortable = 0.0) - (Select Count(" + column + ") FROM " + from + " WHERE " + column +" = 0.0)) as difference;"
    }

    def i = IntPrimitive(_:Long).asInstanceOf[PrimitiveValue]

    def isNullCheck():Boolean = {
      numberProblemRows = ((db.backend.resultRows(isNullCheckStencil())(0)(0)).toString).toInt;
      numberProblemRows == 0;
    }

    def problemRowsStencil():String = {
      val column:String = (isNullExpression.getLeftExpression).toString();
      "Select * from " + from + " ORDER BY " + column + " DESC Limit " + numberProblemRows.toString+";"
    }

    def problemRows():Unit = {
      val temp = db.backend.resultRows(problemRowsStencil())
      for(i <- 0 to temp.size -1) {
        println("ROW: " + temp(i))
      }
    }

    def reset():Unit = {
      isNull = false;
      oper = null;
      isNullExpression = null;
      lookingFrom = true;
      from = null;
      numberProblemRows = 0;
    }


}
