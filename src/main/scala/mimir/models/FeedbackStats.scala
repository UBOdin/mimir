package mimir.models

import mimir.Database
import mimir.algebra._
import mimir.util._

class FeedbackStats(db: Database){
  
  val fbConfidence = scala.collection.mutable.Map[FeedbackSourceIdentifier, Double]()
  
  def calcConfidence(idx: Int, args: Seq[PrimitiveValue]) : Unit = {
    
    val models = db.models.getAllModels
    var sourceStats = scala.collection.mutable.Map[FeedbackSourceIdentifier, (Int,Int)]() // (correct,total)
    for(model <- models) {
      if(model.isInstanceOf[SourcedFeedback]) {
        val modelfb = model.asInstanceOf[SourcedFeedback]
        var total : Double = 0.0
        var correct : Double = 0.0
        for((fbKey,sourceMap) <- modelfb.feedback) {
          for((sourceId,value) <- sourceMap) {
            val groundTruth = sourceMap.getOrElse(FeedbackSource.groundSource,null)
            if (groundTruth!=null) {
              val stats = sourceStats.getOrElse(sourceId,(0,0))
              sourceStats(sourceId) = if(value.equals(groundTruth)) {
                (stats._1+1,stats._2+1)
              }
              else {
                (stats._1,stats._2+1)
              }
            }
          }
        }
        for((sourceId,stats) <- sourceStats) {
          fbConfidence(sourceId)=stats._1/stats._2
        }
      }
    }
  }
  
}