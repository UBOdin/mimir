package mimir.lenses

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import net.sf.jsqlparser.statement.select.Select

object MissingKeyLens {
  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val schema = db.bestGuessSchema(query)
    val schemaMap = schema.toMap
    var missingOnly = false;
    var sortCols = Seq[SortColumn]()
    val keys: Seq[(String, Type)] = args.flatMap {
      case Var(col) => {
        if(schemaMap contains col){ Some((col, schemaMap(col))) }
        else {
          throw new SQLException(s"Invalid column: $col in KeyRepairLens $name")
        }
      }
      case Function("MISSING_ONLY", Seq(Var(bool))) => {
        missingOnly = bool match {
          case "TRUE" => true
          case _ => false
        }
        None
      }
      case Function("SORT", cols:Seq[Var]) => {
        sortCols = cols.map(col => {
          if(!schemaMap.contains(col.name))
            throw new SQLException(s"Invalid sort column: $col in KeyRepairLens $name")
          SortColumn(col, true) 
        })
        None
      }
      case somethingElse => throw new SQLException(s"Invalid argument ($somethingElse) for MissingKeyLens $name")
    }
    val rSch = schema.filter(p => !keys.contains(p))
    val seriesTableName = name.toUpperCase()+"_SERIES"
    val keysTableName = name.toUpperCase()+"_KEYS"
    val missingKeysTableName = name.toUpperCase()+"_MISSING_KEYS"
    val allTables = db.getAllTables()
    if(!allTables.contains(seriesTableName)){
      val createSeriesTableSql = s"CREATE TABLE $seriesTableName(${keys.map(kt => kt._1 +" "+ kt._2.toString()).mkString(",")})"
      db.update(db.stmt(createSeriesTableSql))
      val seriesTableMinMaxOper = Project(
        keys.map { col => Seq(
            ProjectArg("MIN", Var(col._1+"_MIN")), 
            ProjectArg("MAX", Var(col._1+"_MAX")))
            }.flatten,
        Aggregate(Seq(), 
        keys.map{ col => Seq(
            AggFunction("MIN", false, List(Var(col._1)), col._1+"_MIN"), 
            AggFunction("MAX", false, List(Var(col._1)), col._1+"_MAX")) 
            }.flatten,
          query
        )
       )
      val minMaxForSeries = db.query(seriesTableMinMaxOper)
      minMaxForSeries.open()
      minMaxForSeries.getNext()
      val minMax = (
      minMaxForSeries(0).asDouble.toLong,
      minMaxForSeries(1).asDouble.toLong
      )
      minMaxForSeries.close()
      db.backend.fastUpdateBatch(s"INSERT INTO $seriesTableName VALUES(?)", (minMax._1 to minMax._2).toSeq.map( i => Seq(IntPrimitive(i))))
    }
    if(!allTables.contains(keysTableName)){
      val projArgsKeys =  
        keys.map(_._1).map( col => {
            ProjectArg(col, Var(col))
        })
      var keysOper : Operator = Project(projArgsKeys, query)             
      println(keysOper)
      val results = db.compiler.compile(keysOper, List())
      val createKeysTableSql = s"CREATE TABLE $keysTableName(${keys.map(kt => kt._1 +" "+ kt._2.toString()).mkString(",")})"
      db.update(db.stmt(createKeysTableSql))
      db.backend.fastUpdateBatch(s"INSERT INTO $keysTableName VALUES(${keys.map(kt => "?").mkString(",")})", results.allRows())
    }
    if(!allTables.contains(missingKeysTableName)){
      val lTalebName = seriesTableName
      val rTalebName = keysTableName
      val projArgsLeft =  
        keys.map(_._1).map( col => {
            ProjectArg("lft_"+col, Var(col))
        })
      val projArgsRight =  
        keys.map(_._1).map( col => {
            ProjectArg("rght_"+col, Var(col))
        })
      var missingKeysOper : Operator = mimir.algebra.Select( IsNullExpression(Var("rght_"+keys.head._1)),
                  LeftOuterJoin(
                    Project(projArgsLeft, Table(lTalebName,"lft",keys,List())),
                    Project(projArgsRight, Table(rTalebName,"rght",keys,List())), 
                    Comparison(Cmp.Eq, Var("rght_"+keys.head._1), Var("lft_"+keys.head._1))
                  ))
      val sql = db.ra.convert(missingKeysOper)
      val results = new mimir.exec.ResultSetIterator(db.backend.execute(sql), 
        keys.toMap,
        keys.zipWithIndex.map(_._2), 
        Seq()
      )
      val createMissingKeysTableSql = s"CREATE TABLE $missingKeysTableName(${keys.map(kt => kt._1 +" "+ kt._2.toString()).mkString(",")})"
      db.update(db.stmt(createMissingKeysTableSql))
      db.backend.fastUpdateBatch(s"INSERT INTO $missingKeysTableName VALUES(${keys.map(kt => "?").mkString(",")})", results.allRows())
    
    }
    val colsTypes = keys.unzip
    val model = new MissingKeyModel(name+":"+ keys.unzip._1.mkString("_"),colsTypes._1, colsTypes._2.union(rSch.map(_ => TAny())))
    
    val projArgs =  
        keys.map(_._1).zipWithIndex.map( col => {
            ProjectArg(col._1, VGTerm(model, col._2, Seq(RowIdVar()), Seq(Var(col._1))))
        }).union(rSch.map(_._1).zipWithIndex.map( col => {
            ProjectArg(col._1,  VGTerm(model, keys.length+col._2, Seq(RowIdVar()), Seq(NullPrimitive())))
        }))
    
    val missingKeysOper = Project(projArgs, Table(missingKeysTableName,missingKeysTableName,keys,List()))
    val allOrMissingOper = missingOnly match {
      case true => missingKeysOper
      case _ => Union( missingKeysOper,  query )
    }
    val oper = {
      if(sortCols.isEmpty) allOrMissingOper;
      else Sort(sortCols, allOrMissingOper);
    }
    (
      oper,
      Seq(model)
    )
  }
}