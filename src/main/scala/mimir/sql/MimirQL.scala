package mimir.sql;

import java.sql.SQLException
import mimir.Database
import mimir.algebra._

object MimirQL {

  def applyExtend(db: Database, lastQuery: Operator, extend: Extend): Operator =
  {
    val columns = lastQuery.schema.map(_._1).toSet
    val target = extend.getTarget.toUpperCase
    val valueExpr = 
      if(extend.getValue == null){ null }
      else { db.sql.convert(extend.getValue, columns.map(x => (x,x)).toMap) }

    extend.getOp match {
      case Extend.Op.ADD_COLUMN => 
        if(columns contains target){
          throw new SQLException(s"Column '$target' already exists (Use EXTEND ALTER instead)");
        }
        if(valueExpr == null){
          throw new SQLException(s"EXTEND ADD invalid, need a value expression")
        }
        OperatorUtils.projectInColumn(target, valueExpr, lastQuery)

      case Extend.Op.ALTER_COLUMN => 
        if(!(columns contains target)){
          throw new SQLException(s"Column '$target' isn't an existing column (Use EXTEND ADD instead)");
        }
        if(valueExpr == null){
          throw new SQLException(s"EXTEND ALTER invalid, need a value expression")
        }
        OperatorUtils.replaceColumn(target, valueExpr, lastQuery)

      case Extend.Op.RENAME_COLUMN => 
        if(!(columns contains target)){
          throw new SQLException(s"Column '$target' isn't an existing column (Use EXTEND ADD instead)");
        }
        val sourceCol = valueExpr match {
          case Var(c) => 
          throw new SQLException(s"EXTEND RENAME invalid, need a column!")
        }
        OperatorUtils.renameColumn(sourceCol, target, lastQuery)
    }
  }

}