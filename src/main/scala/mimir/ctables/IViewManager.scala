package mimir.ctables;

import java.sql._;
import java.io.StringReader;
import collection.JavaConversions._;

import mimir.parser._;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;
import mimir.sql._;
import mimir.{Database,Mimir};


class IViewManager(db: Database) {
  var views = scala.collection.mutable.Map[String,IView]();
  
  def init(): Unit = { }
  // {
  //   db.update("CREATE TABLE MIMIR_IVIEW(name varchar(30), query text, PRIMARY KEY(name))");
  //   db.update("CREATE TABLE MIMIR_LENS(iview varchar(30), m_id int, m_type varchar(30), parameters text, PRIMARY KEY(iview,m_id))");
  // }
  
  // def mkLens(lensType: String, iview: String, id: Int, params: List[String], source: Operator): Lens =
  // {
  //   lensType.toUpperCase() match { 
  //     case "MISSING_VALUE" => 
  //       new MissingValueLens(iview, id, params, source)
  //   }
  // }
  
  
  def create(iv: CreateIView): Unit = { }
  //   val (baseQuery, bindings) = db.convert(iv.getSelectBody)
  //   val originalSource = 
  //        Project(
  //          bindings.map( _ match { case (external, internal) =>
  //            ProjectArg(external, Var(internal))
  //           }).toList,
  //          baseQuery
  //        )
  //   var source: Operator = originalSource;
  //   val viewName = iv.getTable.getName.toUpperCase;
  //   var lensId = -1;
  //   val view = new IView(viewName, originalSource, 
  //     iv.getModules().map( 
  //       (lensMetadata) => {
  //         lensId += 1;
  //         val params = 
  //           lensMetadata.args.map( (arg:net.sf.jsqlparser.expression.Expression) => 
  //             Eval.evalString(db.convert(arg))
  //           ).toList
            
  //         val lens = mkLens(lensMetadata.name, viewName, lensId, params, source);
  //         source = lens.view
  //         lens
  //       }).toList
  //     )
  //   db.update(
  //       "INSERT INTO MIMIR_IVIEW(name, query) VALUES (?, ?)",
  //       List(viewName, iv.getSelectBody.toString())
  //   );
  //   view.lenses.map( (lens) => {
  //     db.update(
  //         "INSERT INTO MIMIR_LENS(iview, m_id, m_type, parameters) VALUES (?, "+
  //           lens.id+", ?, ?)",
  //         List(viewName, lens.lensType, lens.serializeParams)
  //     );
  //   })
  //   view.build(db);
  //   views.put(viewName, view);
  // }
  
   def load(): Unit = {}
  //   db.query(
  //     "SELECT name, query FROM MIMIR_IVIEW"
  //   ).foreach( (viewMetadata) => {
  //     val name = viewMetadata(0).asString;
  //     if(!Mimir.conf.quiet()){
  //       System.out.println("Loading IView: " + name);
  //     }
  //     val originalSource = db.convert(new CCJSqlParser(new StringReader(viewMetadata(1).asString)).Select());
  //     var source = originalSource;
  //     var lenses = List[Lens]();
  //     db.query(
  //       "SELECT m_type, m_id, parameters FROM MIMIR_LENS WHERE iview = ? ORDER BY m_id",
  //       List(name)
  //     ).foreach( (lensMetadata) => {
  //       val lens = mkLens(
  //         lensMetadata(0).asString,
  //         name, 
  //         lensMetadata(1).asLong.toInt,
  //         lensMetadata(2).asString.split(",").toList,
  //         source
  //       )
  //       lenses = lenses ++ List(lens)
  //       source = lens.view
  //     })
  //     val view = new IView(name, originalSource, lenses)
  //     view.build(db);
  //     views.put(name, view);
  //   })
  // }

  // def analyze(v: PVar): CTAnalysis =
  // {
  //   views.get(v.iview).get.analyze(db, v)
  // }
}