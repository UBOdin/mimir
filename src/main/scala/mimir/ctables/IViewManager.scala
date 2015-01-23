package mimir.ctables;

import java.sql._;
import java.io.StringReader;
import collection.JavaConversions._;

import mimir.parser._;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;
import mimir.sql._;


class IViewManager(backend: Backend, sqltora: SqlToRA) {
  var views = scala.collection.mutable.Map[String,IView]();
  
  def init(): Unit = 
  {
    backend.update("CREATE TABLE MIMIR_IVIEW(name varchar(30), query text, PRIMARY KEY(name))");
    backend.update("CREATE TABLE MIMIR_IVIEW_MODULE(iview varchar(30), m_id int, m_type varchar(30), parameters text, PRIMARY KEY(iview,m_id))");
    backend.update("CREATE TABLE MIMIR_IVIEW_VAR(iview varchar(30), m_id int, v_id int, metadata text, PRIMARY KEY(iview,m_id,v_id))");
  }
  
  def mkModule(moduleType: String, iview: String, id: Int, params: List[String]): IViewModule =
  {
    moduleType.toUpperCase() match { 
      case "MISSING_VALUE" => 
        new MissingValueModule(iview, id, params)
    }
  }
  
  
  def create(iv: CreateIView): Unit = {
    val (baseQuery, bindings) = sqltora.convert(iv.getSelectBody, null)
    var source = 
         Project(
             bindings.map( _ match { case (external, internal) =>
               ProjectArg(external, Var(internal))
              }).toList,
             baseQuery
           )
    val viewName = iv.getTable.getName.toUpperCase;
    var moduleId = -1;
    val view = new IView(viewName, source, 
      iv.getModules().map( 
        (module) => {
          moduleId += 1;
          val params = 
            module.args.map( (arg:net.sf.jsqlparser.expression.Expression) => 
              Eval.evalString(sqltora.convert(arg))
            ).toList
          mkModule(module.name, viewName, moduleId, params);
        }).toList
      )
    backend.update(
        "INSERT INTO MIMIR_IVIEW(name, query) VALUES (?, ?)",
        List(viewName, iv.getSelectBody.toString())
    );
    view.getModules().map( (mod) => {
      backend.update(
          "INSERT INTO MIMIR_IVIEW_MODULE(iview, m_id, m_type, parameters) VALUES (?, "+
            mod.moduleId+", ?, ?)",
          List(viewName, mod.moduleType, mod.moduleParams.mkString(","))
      );
    })
    view.build(backend);
    views.put(viewName, view);
  }
  
  def load(): Unit = {
    val viewMetadata = backend.execute("SELECT name, query FROM MIMIR_IVIEW");
    while(viewMetadata.isBeforeFirst()){ viewMetadata.next(); }
    while(!viewMetadata.isAfterLast()){
      val name = viewMetadata.getString(1);
      System.out.println("Loading IView: " + name);
      val query = sqltora.convert(new CCJSqlParser(new StringReader(viewMetadata.getString(2))).Select());
      val moduleMetadata = backend.execute(
        "SELECT m_type, m_id, parameters FROM MIMIR_IVIEW_MODULE WHERE iview = ? ORDER BY m_id",
        List(name)
      )
      var modules = List[IViewModule]();
      while(moduleMetadata.isBeforeFirst()){ moduleMetadata.next(); }
      while(!moduleMetadata.isAfterLast()){
        modules = modules ++ List(mkModule(
          moduleMetadata.getString(1),
          name, 
          moduleMetadata.getInt(2),
          moduleMetadata.getString(3).split(",").toList
        ))
        moduleMetadata.next();
      }
      viewMetadata.next();
      val view = new IView(name, query, modules)
      view.build(backend);
      views.put(name, view);
    }
  }

  
}