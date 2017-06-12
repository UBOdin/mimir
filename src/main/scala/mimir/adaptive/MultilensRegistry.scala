package mimir.adaptive

object MultilensRegistry
{

  val multilenses = Map[String,Multilens](
    "DISCALA_ABADI" -> DiscalaAbadiNormalizer,
    "TYPE_INFERENCE" -> TypeInference
  )

}