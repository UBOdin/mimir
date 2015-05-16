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


class LensManager(db: Database) {
  var lensCache = scala.collection.mutable.Map[String,Lens]();
  
  def init(): Unit = { }
  // {
  //   db.update("CREATE TABLE MIMIR_IVIEW(name varchar(30), query text, PRIMARY KEY(name))");
  //   db.update("CREATE TABLE MIMIR_LENS(iview varchar(30), m_id int, m_type varchar(30), parameters text, PRIMARY KEY(iview,m_id))");
  // }
  
  def mkLens(lensType: String, lensName: String, args: List[String], source: Operator): Lens =
  {
    lensType.toUpperCase() match { 
      case "MISSING_VALUE" => 
        new MissingValueLens(lensName, args, source)
    }
  }
  
  def create(lensDefn: CreateLens): Unit = { 
    val (baseQuery, bindings) = db.convert(lensDefn.getSelectBody)
    val originalSource = 
         Project(
           bindings.map( _ match { case (external, internal) =>
             ProjectArg(external, Var(internal))
            }).toList,
           baseQuery
         )
    val source: Operator = originalSource;
    val lensName = lensDefn.getName.toUpperCase;
    val lens = mkLens(
        lensDefn.getType(), 
        lensName, 
        lensDefn.getArgs.map( (arg:net.sf.jsqlparser.expression.Expression) => 
            Eval.evalString(db.convert(arg))
          ).toList,
        source
      );
    lens.build(db);
    lensCache.put(lensName, lens);
    save(lens);
  }

  def save(lens: Lens): Unit = {}
  
  def load(lensName: String): Option[Lens] = {
    lensCache.get(lensName);
  }

  def modelForLens(lensName: String): Model = 
    lensCache.get(lensName).get.model
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
}