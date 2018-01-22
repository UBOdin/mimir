package mimir.statistics

import mimir.Database
import mimir.algebra._


object StatsPlugins{

    def columnRSDScore(colName: String, db:Database, query: Operator): Double = {

      var testQuery = query.filter(Var(colName).isNull.not).sort((colName, true)).project(colName)
      
      var diffAdj: Seq[Double] = Seq()
      var sum: Double = 0
      var count = 0

      // Run the query
      db.query(testQuery) { result =>

        val rowWindow = 
          result
            .map { _(0) } // Only one attribute in each row.  Pick it out
            .sliding(2)   // Get a 2-element sliding window over the result
        for( rowPair <- rowWindow ){
          val a = rowPair(0)
          val b = rowPair(1)
          val currDiff = Math.abs(a.asDouble - b.asDouble)
          sum += currDiff
          diffAdj = diffAdj :+ (currDiff)
          count += 1
        }
      }
      val mean = Math.floor((sum/count)*10000)/10000
      val stdDev = Math.floor(Math.sqrt((diffAdj.map(x => (x-mean)*(x-mean)).sum)/count)*10000)/10000
      val relativeStdDev = Math.abs(stdDev/mean)
      
      if(relativeStdDev > 1.0) 1.0 else relativeStdDev
    }
    
    
    def columnRSquareScore(colName: String,  db:Database, query: Operator): Double = {
  
      //!!!! Change below by using Aggregartion. Create a query with Select colname(y) and an incremental value(x)
      val testQuery = query.filter(Var(colName).isNull.not).project(colName).sort((colName, true))
      
      val rValue: Double = db.query(testQuery) { result => 
        
        val attributeList = 
          result.zipWithIndex.map{ tup =>
            (tup._2.toDouble,
            tup._2 * tup._2.toDouble,
            tup._1(0).asDouble,
            tup._1(0).asDouble * tup._1(0).asDouble,
            tup._1(0).asDouble * tup._2,
            1.0) 
          }
        
        
        val (xSum, xSqSum, ySum, ySqSum, xySum, sum) = 
          attributeList.foldLeft(0.0, 0.0, 0.0, 0.0, 0.0, 0.0){
          case ((acc1, acc2, acc3, acc4, acc5, acc6), (a, b, c, d, e, f)) =>
            (acc1+a, acc2+b, acc3+c, acc4+d, acc5+e, acc6+f)
        }
        
        
        ((sum*xySum)-(xSum*ySum))/Math.sqrt(((sum*xSqSum)-(xSum*xSum))*((sum*ySqSum)-(ySum*ySum)))
      }
      rValue*rValue
    }
    

    def score(statType:String, db:Database, colName:String, oper: Operator): Double = {
      
      val resultScore = (statType) match {
        case "RELSTDDEV" => columnRSDScore(colName, db, oper)
        case "RSQUARED"   => columnRSquareScore(colName, db, oper)
        case _           => throw new Exception (s"Invalid Score Evaluation Type : $statType")
      }
      
      resultScore
      
    }
    
    def combinedScore(db:Database, colName:String, oper: Operator, RSDFactor: Double = 0.5, RSQFactor: Double = 0.5): Double = {
      
      val RSDValue = score("RELSTDDEV", db, colName, oper)
      val RSQValue = score("RSQUARED" , db, colName, oper)
      
      ((RSDFactor*(1-RSDValue))+(RSQFactor*RSQValue))
    }
}
