package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._

import scala.collection.JavaConversions._


class LensManager(db: Database) {
  var lensCache = scala.collection.mutable.Map[String,Lens]();
  
  def init(): Unit =
  {
    db.update("""
      CREATE TABLE MIMIR_LENSES(
        name varchar(30), 
        query text, 
        lens_type varchar(30),
        parameters text,
        PRIMARY KEY(name)
      );""");
  }
  
  def mkLens(lensType: String, lensName: String, args: List[Expression], source: Operator): Lens =
  {
    lensType.toUpperCase() match { 
      case "MISSING_VALUE" => 
        new MissingValueLens(lensName, args, source)
      case "SCHEMA_MATCHING" =>
        new SchemaMatchingLens(lensName, args, source)
      case "TYPE_INFERENCE" =>
        new TypeInferenceLens(lensName, args, source)
    }
  }

  def lensTypeString(lens: Lens): String =
  {
    lens match { 
      case _:MissingValueLens => "MISSING_VALUE"
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
            db.convert(arg)
          ).toList,
        source
      );
    lens.build(db);
    lensCache.put(lensName, lens);
    save(lens);
  }

  def save(lens: Lens): Unit = {
    db.update("""
      INSERT INTO MIMIR_LENSES(name, query, lens_type, parameters) 
      VALUES (?,?,?,?)
    """, List(
      lens.name, 
      lens.source.toString,
      lensTypeString(lens),
      lens.args.map(_.toString).mkString(",")
    ))
    lens.save(db)
  }
  
  def load(lensName: String): Option[Lens] = {
    lensCache.get(lensName) match {
      case Some(s) => Some(s)
      case None => {
        val lensMetaResult =
          db.query("""
            SELECT lens_type, parameters, query
            FROM MIMIR_LENSES
            WHERE name = ?
          """, List(lensName)).allRows
        if(lensMetaResult.length == 0) { return None; }
        else if(lensMetaResult.length > 1){ 
          throw new SQLException("Multiple definitions for Lens `"+lensName+"`")
        } else {
          val lensMeta = lensMetaResult(0)
          val lens = 
            mkLens(
              lensMeta(0).asString, 
              lensName, 
              db.parseExpressionList(lensMeta(1).asString),
              db.parseOperator(lensMeta(2).asString)
            )
          lens.load(db)
          lensCache.put(lensName, lens)
          return Some(lens)
        }
      }
    }
  }

  def modelForLens(lensName: String): Model = 
    load(lensName).get.model
}