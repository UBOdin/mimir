package mimir.exec;

import java.sql._;
import mimir.sql._;
import mimir.algebra._;
import mimir.ctables._;

object Compiler {
  def compile(ivmanager: IViewManager, backend: Backend, oper: Operator): ResultIterator =
  {
    if(CTAnalysis.isProbabilistic(oper)){
      oper match { 
        case Project(cols, src) =>
          val inputIterator = compile(ivmanager, backend, src);
          new ProjectionResultIterator(
            inputIterator,
            cols.map( (x) => (x.column, x.input) ),
            ivmanager, 
            backend
          );
          
        case _ =>
          throw new SQLException("Unsupported root operator\n"+oper);
      }
      
      
    } else {
      val rset = backend.execute(oper);
      return new ResultSetIterator(rset);
    }
  }
  def dump(result: ResultIterator): Unit =
  {
    while(result.getNext()){
      println(result.toList().mkString(", "))
    }
  }
}