package mimir.adaptive

object MultilensRegistry
{

  val multilenses = Map[String,Multilens](
    "DISCALA_ABADI" -> DiscalaAbadiNormalizer,
    "DETECT_HEADER" -> CheckHeader,
    "TYPE_INFERENCE" -> TypeInference,
    "SCHEMA_MATCHING" -> SchemaMatching,
    "SHAPE_WATCHER" -> ShapeWatcher
  )

}
