package mimir.lenses

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import net.sf.jsqlparser.statement.select.Select
import mimir.exec.result.Row
import mimir.util.JDBCUtils
import mimir.sql.RAToSql
import mimir.exec.mode.BestGuess

//TODO:  rewrite MissingKeyLens once there is spark support
object MissingKeyLens {
  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val schema = db.typechecker.schemaOf(query)
    val schemaMap = schema.toMap
    var missingOnly = false;
    var sortCols = Seq[(String, Boolean)]()
    val keys: Seq[(String, Type)] = args.flatMap {
      case Var(col) => {
        if(schemaMap contains col){ Some((col, schemaMap(col))) }
        else {
          throw new RAException(s"Invalid column: $col in KeyRepairLens $name")
        }
      }
      case Function("MISSING_ONLY", Seq(Var(bool))) => {
        missingOnly = bool match {
          case "TRUE" => true
          case _ => false
        }
        None
      }
      case Function("SORT", cols) => {
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
       ))(minMaxForSeries => {
        val row = minMaxForSeries.next()
        val minMax = (
          row(0).asDouble.toLong,
          row(1).asDouble.toLong
        )
        HardTable(keys.map(key => (s"${key._1}",key._2)), (minMax._1 to minMax._2).toSeq.map( i => Seq(IntPrimitive(i))))
      })
    
    
    val projKeys = Project(keys.map(key => (s"rght_${key._1}",key._1)).map( col => {
            ProjectArg(col._1, Var(col._2))
        }), BestGuess.rewriteRaw(db, query)._1)             
      

    val missingKeysLookup = mimir.algebra.Select( IsNullExpression(Var("rght_"+keys.head._1)),
                  LeftOuterJoin(
                    allKeysHT,
                    projKeys,
                    Comparison(Cmp.Eq, Var("rght_"+keys.head._1), Var(s"${keys.head._1}"))
                  ))
    val df = db.backend.execute(missingKeysLookup).rdd.toLocalIterator
    var htData = Seq[Seq[PrimitiveValue]]()
    while(df.hasNext){
      htData = htData :+ keys.zipWithIndex.map(col => IntPrimitive(df.next.getInt(col._2+1))) 
    }
    
    val missingKeys = HardTable(keys, htData)
    
    val colsTypes = keys.unzip
    val model = new MissingKeyModel(name+":"+ keys.unzip._1.mkString("_"),colsTypes._1, colsTypes._2.union(rSch.map(sche => sche._2)))
    
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
      else allOrMissingOper.sort(sortCols:_*);
    }
    (
      oper,
      Seq(model)
    )
  }
}