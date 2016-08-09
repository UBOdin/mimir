package mimir.lenses;

import java.io.File
import java.nio.file.Path
import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.sql._
import mimir.util.JDBCUtils

import scala.collection.JavaConversions._


class LensException(msg: String, trigger: Throwable) extends Exception(msg, trigger);

class LensManager(db: Database) {
  var lensCache = scala.collection.mutable.Map[String,Lens]()

  def serializationFolderPath: Path = java.nio.file.Paths.get("serialized", "__"+db.getName)

    def init(): Unit =
  {
    db.backend.update("""
      CREATE TABLE MIMIR_LENSES(
        name varchar(30), 
        query varchar(4000),
        lens_type varchar(30),
        parameters varchar(4000),
        PRIMARY KEY(name)
      )""")

    val serializationFolder = new File(serializationFolderPath.toString)
    if(!serializationFolder.exists()) {
      try{
        serializationFolder.mkdir()
      } catch {
        case se: SecurityException =>
          println("Could not create serialization folder, lenses will not be saved\n"+se.getMessage)
      }
    }
  }
  
  def mkLens(lensType: String, lensName: String, args: List[Expression], source: Operator): Lens =
  {
    lensType.toUpperCase match {
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
      case _: MissingValueLens => "MISSING_VALUE"
      case _: SchemaMatchingLens => "SCHEMA_MATCHING"
      case _: TypeInferenceLens => "TYPE_INFERENCE"
    }
  }

  
  def create(lensDefn: CreateLens): Unit = { 
    val (baseQuery, bindings) = db.sql.convert(lensDefn.getSelectBody, null)
    val source = 
         Project(
           bindings.map( _ match { case (external, internal) =>
             ProjectArg(external, Var(internal))
            }).toList,
           baseQuery
         )
    create(
      source, 
      lensDefn.getName, 
      lensDefn.getArgs.map( (arg:net.sf.jsqlparser.expression.Expression) =>
        //TODO refactor this
          if(lensDefn.getType.equalsIgnoreCase("SCHEMA_MATCHING"))
            Var(arg.toString)
          else
            db.sql.convert(arg)
        ).toList,
      lensDefn.getType()
      )
  }
  def create(source: Operator, lensName: String, args: List[Expression], lensType: String): Unit = {
    val lens = mkLens(
        lensType.toUpperCase, 
        lensName.toUpperCase, 
        args,
        source
      );
    lens.build(db);
    db.bestGuessCache.buildCache(lens);
    lensCache.put(lensName, lens);
    save(lens);
  }

  def save(lens: Lens): Unit = {
    db.backend.update("""
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
    // println("GETTING: "+lensName)
    lensCache.get(lensName) match {
      case Some(s) => Some(s)
      case None => {
        val lensMetaResult =
          db.backend.resultRows("""
            SELECT lens_type, parameters, query
            FROM MIMIR_LENSES
            WHERE name = ?
          """, List(lensName))
        if(lensMetaResult.length == 0) { 
          return None; 
        } else if(lensMetaResult.length > 1){ 
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
          try {
            lens.load(db)
          } catch {
            case e: Throwable => throw new LensException("Error in Lens: \n"+lens.toString, e);
          }
          lensCache.put(lensName, lens)
          return Some(lens)
        }
      }
    }
  }

  def getAllLensNames(): List[String] = {
    db.backend.resultRows(
      """
        SELECT NAME
        FROM MIMIR_LENSES
      """).
    map(_(0).asString.toUpperCase)
  }

  def modelForLens(lensName: String): Model = 
    load(lensName).get.model
}