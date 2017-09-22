package mimir.models

import mimir.algebra._

object FeedbackSource {
  var feedbackSource:String = ""
}

trait DataIndependentFeedback extends SourcedFeedbackT[Int] {
  val name: String
  def validateChoice(idx: Int, v: PrimitiveValue): Boolean
  def choices(idx:Int) : Option[PrimitiveValue] = getFeedback(idx, null)
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]) : Int = idx 
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
    if(validateChoice(idx, v)){ setFeedback(idx, args, v) }
    else { throw ModelException(s"Invalid choice for $name: $v") }
  def isAcknowledged(idx: Int, args: Seq[PrimitiveValue]): Boolean =
    hasFeedback(idx, args)
}

trait SourcedFeedback extends SourcedFeedbackT[String] 

trait SourcedFeedbackT[T] {
  val feedbackSources = scala.collection.mutable.Set[String]()
  val feedback = scala.collection.mutable.Map[T,scala.collection.mutable.Map[String, PrimitiveValue]]()
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]) : T
  def getFeedback(idx: Int, args: Seq[PrimitiveValue]) : Option[PrimitiveValue] = {
    feedback.get(getFeedbackKey(idx,args)) match {
      case None => None
      case Some(sourceMap) => sourceMap.get(FeedbackSource.feedbackSource) 
    }
  }
  def setFeedback(idx: Int, args: Seq[PrimitiveValue], value:PrimitiveValue) : Unit = {
    val fbKey = getFeedbackKey(idx,args)
    feedback.get(fbKey) match {
      case None => feedback(fbKey) = scala.collection.mutable.Map(FeedbackSource.feedbackSource -> value)
      case Some(sourceMap) => sourceMap(FeedbackSource.feedbackSource) = value
    }
    feedbackSources.add(FeedbackSource.feedbackSource)    
  }
  def hasFeedback(idx: Int, args: Seq[PrimitiveValue]) : Boolean = {
    feedback.get(getFeedbackKey(idx,args)) match {
      case None => false
      case Some(sourceMap) => sourceMap.get(FeedbackSource.feedbackSource) match {
        case None => false
        case Some(value) => true
      }
    }
  }
}