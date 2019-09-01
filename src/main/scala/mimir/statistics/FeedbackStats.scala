package mimir.statistics

import mimir.Database
import mimir.algebra._
import mimir.util._
import mimir.models.FeedbackSource
import mimir.models.FeedbackSourceIdentifier
import mimir.models.SourcedFeedback

class FeedbackStats(db: Database){
  
  val fbConfidence = scala.collection.mutable.Map[FeedbackSourceIdentifier, Double]()
  
  /** Computes the confidence of each feedback source.
   *
   *  loops through every feedback element and compares each to 
   *  a trusted value. Sources of feedback are scored based on 
   *  the number of feedback elements that are equal to the trusted
   *  value.
   */
  // def calcConfidence() : Unit = {
  //   val models = db.models.getAllModels
  //   var sourceStats = scala.collection.mutable.Map[FeedbackSourceIdentifier, (Int,Int)]() // (correct,total)
  //   db.models.getAllModels.flatMap {
  //     case modelfb:SourcedFeedback => Some(modelfb)
  //     case _ => None
  //   }.map( modelfb => {
  //       for((fbKey,sourceMap) <- modelfb.feedback) {
  //         for((sourceId,value) <- sourceMap) {
  //           sourceMap.get(FeedbackSource.groundSource) match {
  //             case Some(groundTruth) if(value.equals(groundTruth)) => {
  //               val stats = sourceStats.getOrElse(sourceId,(0,0))
  //               sourceStats(sourceId) = (stats._1+1,stats._2+1)
  //             }  
  //             case Some(groundTruth) => {
  //               val stats = sourceStats.getOrElse(sourceId,(0,0))
  //               sourceStats(sourceId) = (stats._1,stats._2+1)
  //             }
  //             case _ => {}
  //           }
  //         }
  //       }
  //       for((sourceId,stats) <- sourceStats) {
  //         fbConfidence(sourceId)=stats._1/stats._2
  //       }
  //     })
  // }
  
  //map and fold implementation - runs slower
  /*var fbConfidence = Map[FeedbackSourceIdentifier, Double]()
  def calcConfidence() : Unit = {
    fbConfidence = db.models.getAllModels.flatMap {
      case modelfb:SourcedFeedback => Some(modelfb)
      case _ => None
    }.map( modelfb => {
      modelfb.feedback.foldLeft(Map[FeedbackSourceIdentifier, (Int,Int)]())((sourceStats, elem) => elem match { 
        case (fbKey,sourceMap) => {
          val truth = sourceMap.get(FeedbackSource.groundSource) 
          sourceMap.map {
            case (sourceId,value) => {
              truth match {
                case Some(groundTruth) if(value.equals(groundTruth)) => (sourceId -> (1,1))
                case _ => (sourceId -> (0,1))
              }
            }
          }.foldLeft(sourceStats)((newSourceStats, elem) => 
            newSourceStats.get(elem._1) match {
              case Some(stats) => newSourceStats + (elem._1 -> (stats._1+elem._2._1,stats._2+elem._2._2))
              case None => newSourceStats + (elem._1 -> (elem._2._1,elem._2._2))
            }) 
        }
      })
    }).foldLeft(Map[FeedbackSourceIdentifier, (Int,Int)]())((aggregatedSourceStats, modelStats) => 
        modelStats.foldLeft(aggregatedSourceStats)((newSourceStats, elem) => 
            newSourceStats.get(elem._1) match {
              case Some(stats) => newSourceStats + (elem._1 -> (stats._1+elem._2._1,stats._2+elem._2._2))
              case None => newSourceStats + (elem._1 -> (elem._2._1,elem._2._2))
            }) 
      ).map {
        case (sourceId,(correct,total)) => (sourceId -> (correct.toDouble/total.toDouble))
      }
  }*/
  
}