package mimir.data

import org.apache.spark.sql.DataFrame
import mimir.exec.result.ResultIterator


sealed trait TableImplementation;

case class DataFrameTableImplementation(df: Dataframe)
case class QueryTableImplementation(query: Operator)
case class IteratorTableImplementation(iterator: ResultIterator)