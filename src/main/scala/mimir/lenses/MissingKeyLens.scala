package mimir.lenses

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import mimir.exec.result.Row
import mimir.util.JDBCUtils
import mimir.sql.RAToSql
import mimir.exec.mode.BestGuess

//TODO:  rewrite MissingKeyLens once there is spark support
object MissingKeyLens {
  def create(
    db: Database, 
    name: ID, 
    humanReadableName: String,
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val schema = db.typechecker.schemaOf(query)
    val schemaMap = schema.toMap
    var missingOnly = false;
    var sortCols = Seq[(ID, Boolean)]()
    val keys: Seq[(ID, Type)] = args.flatMap {
      case Var(col) => {
        if(schemaMap contains col){ Some((col, schemaMap(col))) }
        else {
          throw new RAException(s"Invalid column: $col in KeyRepairLens $name")
        }
      }
      case Function(ID("missing_only"), Seq(Var(bool))) => {
        missingOnly = bool.id match {
          case "TRUE" => true
          case _ => false
        }
        None
      }
      case Function(ID("sort"), cols) => {
        sortCols = cols.map { 
          case col:Var => 
            if(!schemaMap.contains(col.name))
              throw new RAException(s"Invalid sort column: $col in KeyRepairLens $name (not a column in the input)")
            (col.name, true) 
          case col => 
            throw new RAException(s"Invalid sort column: $col in KeyRepairLens $name (not a column reference)")
        }
        None
      }
      case somethingElse => throw new RAException(s"Invalid argument ($somethingElse) for MissingKeyLens $name")
    }
    val rSch = schema.filter(p => !keys.contains(p))
    val allKeysHT = db.query(Project(
        keys.map { col => Seq(
            ProjectArg(ID("MIN"), Var(ID(col._1,"_MIN"))), 
            ProjectArg(ID("MAX"), Var(ID(col._1,"_MAX"))))
            }.flatten,
        Aggregate(Seq(), 
        keys.map{ col => Seq(
            AggFunction(ID("min"), false, List(Var(col._1)), ID(col._1,"_MIN")), 
            AggFunction(ID("max"), false, List(Var(col._1)), ID(col._1,"_MAX"))) 
            }.flatten,
          query
        )
       ))(minMaxForSeries => {
        val row = minMaxForSeries.next()
        val minMax = (
          row(0).asDouble.toLong,
          row(1).asDouble.toLong
        )
        HardTable(
          keys,
          (minMax._1 to minMax._2).toSeq.map( i => Seq(IntPrimitive(i)))
        )
      })
    
    val rght = ID("rght_", _:ID)
    
    val projKeys = 
      Project(
        keys.map { case (col, _) => ProjectArg( rght(col), Var(col) ) },
        BestGuess.rewriteRaw(db, query)._1       
      )
      

    val missingKeysLookup = 
      mimir.algebra.Select( 
        rght(keys.head._1).isNull,
        LeftOuterJoin(
          allKeysHT,
          projKeys,
          Var(rght(keys.head._1)).eq(Var(keys.head._1))
        )
      )
    val htData = db.query(missingKeysLookup)(_.toList.map( row =>
      keys.zipWithIndex.map(col => row(col._2+1))) 
    )
    
    val missingKeys = HardTable(keys, htData)
    
    val colsTypes = keys.unzip
    val model = new MissingKeyModel(
      ID(name,":",ID(keys.unzip._1, "_")),
      colsTypes._1, 
      colsTypes._2.union(rSch.map(sche => sche._2))
    )
    
    val projArgs =  
        keys.map(_._1).zipWithIndex.map( col => {
            ProjectArg(col._1, VGTerm(model.name, col._2, Seq(RowIdVar()), Seq(Var(col._1))))
        }).union(rSch.map(_._1).zipWithIndex.map( col => {             
            ProjectArg(col._1,  VGTerm(model.name, keys.length+col._2, Seq(RowIdVar()), Seq(NullPrimitive())))
        }))
    
    val missingKeysOper = Project(projArgs, missingKeys)
    val allOrMissingOper = missingOnly match {
      case true => missingKeysOper
      case _ => Union( missingKeysOper,  query )
    }
    val oper = {
      if(sortCols.isEmpty) allOrMissingOper;
      else allOrMissingOper.sortByID(sortCols:_*);
    }
    (
      oper,
      Seq(model)
    )
  }
}