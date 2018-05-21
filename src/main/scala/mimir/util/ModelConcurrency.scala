package mimir.util

import mimir.models.ModelManager
import mimir.models.ProgressiveUpdate
import mimir.Database

class ModelConcurrency (models:ModelManager, db: Database)
extends Runnable{
  val modelManager = models
  val datab = db
  def init():Unit = {
      //On startup if anything needs to be established
  }
  def run(){
    while(true){
      modelManager.cache.foreach({
        case (name, model) => {
          if(model.isInstanceOf[ProgressiveUpdate]){
            val modelRun = model.asInstanceOf[ProgressiveUpdate]
            if(!modelRun.isCompleted()) {
              modelRun.progressiveTrain(datab, modelRun.getQuery())
            }
          }
        }
      })
      Thread.sleep(2000)
    }
  }
}