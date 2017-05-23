package mimir.lenses

import java.sql.SQLException

import scala.util._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.models._
import mimir.util.TimeUtils
import mimir.util.SqlUtils
import oracle.sql.SQLUtil


// Create Lens test1 as select * from product with Type_Inference(.7);
object TypeInferenceLens extends LazyLogging
{
  var totalVotes: scala.collection.mutable.ArraySeq[Double] = null

  var votes: IndexedSeq[scala.collection.mutable.Map[Type, Double]] = null

  def create(
    db:Database, 
    name:String, 
    query:Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val modelColumns = 
      query.schema.map({
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }).flatten.toIndexedSeq

    totalVotes =
    { val v = new scala.collection.mutable.ArraySeq[Double](modelColumns.length)
      for(col <- (0 until modelColumns.size)){ v.update(col, 0.0) }
      v
    }
    votes =
      modelColumns.map(_ => {
       val temp = scala.collection.mutable.Map[Type, Double]()
       (Type.tests ++ TypeRegistry.registeredTypes).map((tup) => {
          temp.put(Type.fromString(tup._1.toString),0.0)
       })
        temp
      })

    if(modelColumns.isEmpty){ 
      logger.warn("Type inference lens created on table with no string attributes")
      return (query, List())
    }

    if(args.isEmpty){
      throw new ModelException("Type inference lens requires a single parameter")
    }

    val model = 
      new TypeInferenceModel(
        name,
        modelColumns,
        Eval.evalFloat(args(0))
      )


    val columnIndexes =
      modelColumns.zipWithIndex.toMap


    TimeUtils.monitor(s"Train $name", TypeInferenceModel.logger.info(_)){
      db.query(
        Project(
          modelColumns.map( c => ProjectArg(c, Var(c)) ),
          query
        )
      ) { _.foreach { row => learn(row.tuple)  } }
    }


    // Creating the table for the repair_key function
    val countTableName = name + "_TIBacked"
    if(!db.tableExists(countTableName)) {
      var createTableStatement = s"CREATE TABLE $countTableName(columnName TEXT, typeGuess TEXT, projscore REAL, typeExplicit TEXT, score REAL);"
      try{
        db.backend.update(createTableStatement)
      }
      catch{
        case sQLException: SQLException => println("TI backed table not created");
      }

    }

    // create the table for the key repair to operate over, I will use this model for the TI lens

    var loc = 0
    (votes zip modelColumns).map((x) => { // update the map of each column that tracks the type counts
      val v = x._1
      val col = x._2
      val totalV = totalVotes(loc)
      loc += 1
      v.map((tup) => { // loop over types
        val typ:String = tup._1.toString()
        var score = 0.0
        if(totalV > 0.0){
          score = tup._2 / totalV
          if(score > 1.0){
            throw new Exception("Score is greater than 1.0 in Type Inference, something is wrong")
          }
        }
//        println(s"INSERT INTO $countTableName values('$col', '$typ', null, $score);");
        db.backend.update(s"INSERT INTO $countTableName values('$col', '$typ', $score, null, $score);") // update the table for the RK lens
      })
    })

    // create key repair model from this table and use it for the lens

    val RKname = countTableName + "_RK"
    val RKq = s"SELECT * FROM $countTableName;" // query for the RK lens
    val RKop = SqlUtils.plainSelectStringtoOperator(db,RKq)
    val RKargs:Seq[Expression] = Seq(Var("COLUMNNAME"),Function("SCORE_BY",Seq(Var("SCORE")))) // args for the RK lens
    val tup = KeyRepairLens.create(db,RKname,RKop,RKargs)
    val RKmodel:Seq[Model] = tup._2

//    println(RKmodel)

    val repairs = 
      query.schema.map(_._1).map( col => {
        if(columnIndexes contains col){

/*
          println(VGTerm(RKmodel(0), columnIndexes(col), Seq(), Seq()))
          println(VGTerm(RKmodel(1), columnIndexes(col), Seq(), Seq()))
          ProjectArg(col,
            Function("CAST", Seq(
              Var(col),
              VGTerm(model, columnIndexes(col), Seq(), Seq())
            ))
          )
*/
          val VGGuess = VGTerm(RKmodel(0), columnIndexes(col), Seq(), Seq())
          val VGExpected = VGTerm(RKmodel(2), columnIndexes(col), Seq(), Seq())
          val VGScore = VGTerm(RKmodel(1), columnIndexes(col), Seq(), Seq())

          val threshold = args(0) // threshold passed in as the argument, from the lens creation

          ProjectArg(col,
            Conditional(Not(IsNullExpression(VGExpected)),
              Function("CAST",Seq(Var(col),VGExpected)),
              Conditional(Comparison(mimir.algebra.Cmp.Gte,VGScore,threshold),Function("CAST",Seq(Var(col),VGGuess)),Var(col))
            )
          )


        } else {
          ProjectArg(col, Var(col))
        }
      })

    // println(repairs)

    (
      Project(repairs, query),
      RKmodel
    )
  }

  final def learn(row: Seq[PrimitiveValue]):Unit =
  {
    row.zipWithIndex.foreach({ case (v, idx) => learn(idx, v) })
  }

  final def learn(idx: Int, p: PrimitiveValue):Unit =
  {
    p match {
      case null            => ()
      case NullPrimitive() => ()
      case _               => learn(idx, p.asString)
    }
  }

  final def learn(idx: Int, v: String):Unit =
  {
    totalVotes(idx) += 1.0
/*    val typeList: scala.collection.mutable.Map[Type, Double] = scala.collection.mutable.Map[Type, Double]()
    (Type.tests ++ TypeRegistry.registeredTypes).map((tup) => {
      typeList.put(Type.fromString(tup._1.toString),0.0)
    })
*/
    val candidates = detectType(v)
    TypeInferenceModel.logger.debug(s"Guesses for '$v': $candidates")
    val votesForCurrentIdx = votes(idx)
    for(t <- candidates){
      votesForCurrentIdx(t) = votesForCurrentIdx.getOrElse(t, 0.0) + 1.0
    }

  }

  def detectType(v: String): Iterable[Type] = {
    Type.tests.flatMap({ case (t, regexp) =>
      regexp.findFirstMatchIn(v).map(_ => t)
    })++
      TypeRegistry.matchers.flatMap({ case (regexp, name) =>
        regexp.findFirstMatchIn(v).map(_ => TUser(name))
      })
  }

}
