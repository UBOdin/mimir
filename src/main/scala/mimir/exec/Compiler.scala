package mimir.exec;

import java.sql._;
import mimir.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.Database;

class Compiler(db: Database) {
  def optimize(oper: Operator): Operator =
  {
    CTPercolator.percolate(oper);
  }
  
  def compile(oper: Operator): ResultIterator =
  {
    if(CTables.isProbabilistic(oper)){
      oper match { 
        case Project(cols, src) =>
          val inputIterator = compile(src);
          // println("Compiled ["+inputIterator.schema+"]: \n"+src)
          new ProjectionResultIterator(
            db,
            inputIterator,
            cols.map( (x) => (x.column, x.input) )
          );
          
        case _ =>
          throw new SQLException("Unsupported root operator\n"+oper);
      }
      
      
    } else {
      db.query(db.convert(oper))
    }
  }
}