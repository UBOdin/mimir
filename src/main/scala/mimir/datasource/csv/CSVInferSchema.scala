/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.ubodin.csv

import java.math.BigDecimal

import scala.util.control.Exception._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

private[csv] object CSVInferSchema {

  /**
   * Similar to the JSON schema inference
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def infer(
      tokenRDD: RDD[Array[String]],
      header: Array[String],
      options: CSVOptions): StructType = {
    val fields = if (options.inferSchemaFlag) {
      val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
      val rootTypes: Array[DataType] =
        tokenRDD.aggregate(startType)(inferRowType(options), mergeRowTypes)

      header.zip(rootTypes).map { case (thisHeader, rootType) =>
        val dType = rootType match {
          case _: NullType => StringType
          case other => other
        }
        StructField(thisHeader, dType, nullable = true)
      }
    } else {
      // By default fields are assumed to be StringType
      header.map(fieldName => StructField(fieldName, StringType, nullable = true))
    }

    val outputSchema = fields ++ Array(
        StructField(mimir.adaptive.DataSourceErrors.mimirDataSourceErrorColumn, BooleanType, nullable = true), 
        StructField(mimir.adaptive.DataSourceErrors.mimirDataSourceErrorRowColumn, StringType, nullable = true)) 
    StructType(outputSchema)
  }

  private def inferRowType(options: CSVOptions)
      (rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      rowSoFar(i) = inferField(rowSoFar(i), next(i), options)
      i+=1
    }
    rowSoFar
  }

  def mergeRowTypes(first: Array[DataType], second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case (a, b) =>
      findTightestCommonType(a, b).getOrElse(NullType)
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  def inferField(typeSoFar: DataType, field: String, options: CSVOptions): DataType = {
    if (field == null || field.isEmpty || field == options.nullValue) {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseInteger(field, options)
        case IntegerType => tryParseInteger(field, options)
        case LongType => tryParseLong(field, options)
        case _: DecimalType =>
          // DecimalTypes have different precisions and scales, so we try to find the common type.
          findTightestCommonType(typeSoFar, tryParseDecimal(field, options)).getOrElse(StringType)
        case DoubleType => tryParseDouble(field, options)
        case TimestampType => tryParseTimestamp(field, options)
        case BooleanType => tryParseBoolean(field, options)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }

  private def isInfOrNan(field: String, options: CSVOptions): Boolean = {
    field == options.nanValue || field == options.negativeInf || field == options.positiveInf
  }

  private def tryParseInteger(field: String, options: CSVOptions): DataType = {
    if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field, options)
    }
  }

  private def tryParseLong(field: String, options: CSVOptions): DataType = {
    if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDecimal(field, options)
    }
  }

  private def tryParseDecimal(field: String, options: CSVOptions): DataType = {
    val decimalTry = allCatch opt {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new BigDecimal(field)
      // Because many other formats do not support decimal, it reduces the cases for
      // decimals by disallowing values having scale (eg. `1.1`).
      if (bigDecimal.scale <= 0) {
        // `DecimalType` conversion can fail when
        //   1. The precision is bigger than 38.
        //   2. scale is bigger than precision.
        DecimalType(bigDecimal.precision, bigDecimal.scale)
      } else {
        tryParseDouble(field, options)
      }
    }
    decimalTry.getOrElse(tryParseDouble(field, options))
  }

  private def tryParseDouble(field: String, options: CSVOptions): DataType = {
    if ((allCatch opt field.toDouble).isDefined || isInfOrNan(field, options)) {
      DoubleType
    } else {
      tryParseTimestamp(field, options)
    }
  }

  private def tryParseTimestamp(field: String, options: CSVOptions): DataType = {
    // This case infers a custom `dataFormat` is set.
    if ((allCatch opt options.timestampFormat.parse(field)).isDefined) {
      TimestampType
    } else if ((allCatch opt DateTimeUtils.stringToTime(field)).isDefined) {
      // We keep this for backwards compatibility.
      TimestampType
    } else {
      tryParseBoolean(field, options)
    }
  }

  private def tryParseBoolean(field: String, options: CSVOptions): DataType = {
    if ((allCatch opt field.toBoolean).isDefined) {
      BooleanType
    } else {
      stringType()
    }
  }

  // Defining a function to return the StringType constant is necessary in order to work around
  // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
  // see issue #128 for more details.
  private def stringType(): DataType = {
    StringType
  }

  private val numericPrecedence: IndexedSeq[DataType] = TypeCoercion.numericPrecedence

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion]]
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    // These two cases below deal with when `DecimalType` is larger than `IntegralType`.
    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    // These two cases below deal with when `IntegralType` is larger than `DecimalType`.
    case (t1: IntegralType, t2: DecimalType) =>
      findTightestCommonType(DecimalType.forType(t1), t2)
    case (t1: DecimalType, t2: IntegralType) =>
      findTightestCommonType(t1, DecimalType.forType(t2))

    // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
    // in most case, also have better precision.
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
      Some(DoubleType)

    case (t1: DecimalType, t2: DecimalType) =>
      val scale = math.max(t1.scale, t2.scale)
      val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
      if (range + scale > 38) {
        // DecimalType can't support precision > 38
        Some(DoubleType)
      } else {
        Some(DecimalType(range + scale, scale))
      }

    case _ => None
  }
}
