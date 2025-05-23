/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.util

/*
 * The following code is originally from https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
 * and is licensed under the Apache license:
 *
 * License: Apache License 2.0, Copyright 2014 and onwards The Apache Software Foundation.
 * https://github.com/apache/spark/blob/master/LICENSE
 *
 * It has been modified by the Lance developers to fit the needs of the Lance project.
 */

import com.lancedb.lance.spark.LanceConstant

import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.types._

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

object LanceArrowUtils {
  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case int: ArrowType.Int if !int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
      case _ => ArrowUtils.fromArrowField(field)
    }
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    }.toArray)
  }

  def toArrowSchema(
      schema: StructType,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean,
      largeVarTypes: Boolean = false): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        largeVarTypes)
    }.asJava)
  }

  def toArrowField(
      name: String,
      dt: DataType,
      nullable: Boolean,
      timeZoneId: String,
      largeVarTypes: Boolean = false): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(
          name,
          fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId, largeVarTypes)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(
          name,
          fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId, largeVarTypes)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          mapType,
          Seq(toArrowField(
            MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId,
            largeVarTypes)).asJava)
      case udt: UserDefinedType[_] =>
        toArrowField(name, udt.sqlType, nullable, timeZoneId, largeVarTypes)
      case dataType =>
        val fieldType =
          new FieldType(nullable, toArrowType(dataType, timeZoneId, largeVarTypes, name), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  /**
   * Contains copy of org.apache.spark.sql.util.ArrowUtils#toArrowType for Spark version compatibility
   * Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes
   */
  private def toArrowType(
      dt: DataType,
      timeZoneId: String,
      largeVarTypes: Boolean = false,
      name: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType if name.equals(LanceConstant.ROW_ID) => new ArrowType.Int(8 * 8, false)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case _: StringType if !largeVarTypes => ArrowType.Utf8.INSTANCE
    case BinaryType if !largeVarTypes => ArrowType.Binary.INSTANCE
    case _: StringType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
    case BinaryType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale, 8 * 16)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType if timeZoneId == null =>
      throw SparkException.internalError("Missing timezoneId where it is mandatory.")
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TimestampNTZType =>
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case NullType => ArrowType.Null.INSTANCE
    case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case CalendarIntervalType => new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
    case _ =>
      throw unsupportedDataTypeError(dt)
  }

  private def deduplicateFieldNames(
      dt: DataType,
      errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    case udt: UserDefinedType[_] => deduplicateFieldNames(udt.sqlType, errorOnDuplicatedFieldNames)
    case st @ StructType(fields) =>
      val newNames = if (st.names.toSet.size == st.names.length) {
        st.names
      } else {
        if (errorOnDuplicatedFieldNames) {
          throw duplicatedFieldNameInArrowStructError(st.names)
        }
        val genNawName = st.names.groupBy(identity).map {
          case (name, names) if names.length > 1 =>
            val i = new AtomicInteger()
            name -> { () => s"${name}_${i.getAndIncrement()}" }
          case (name, _) => name -> { () => name }
        }
        st.names.map(genNawName(_)())
      }
      val newFields =
        fields.zip(newNames).map { case (StructField(_, dataType, nullable, metadata), name) =>
          StructField(
            name,
            deduplicateFieldNames(dataType, errorOnDuplicatedFieldNames),
            nullable,
            metadata)
        }
      StructType(newFields)
    case ArrayType(elementType, containsNull) =>
      ArrayType(deduplicateFieldNames(elementType, errorOnDuplicatedFieldNames), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        deduplicateFieldNames(keyType, errorOnDuplicatedFieldNames),
        deduplicateFieldNames(valueType, errorOnDuplicatedFieldNames),
        valueContainsNull)
    case _ => dt
  }

  /* Copy from copy of org.apache.spark.sql.errors.ExecutionErrors for Spark version compatibility */
  private def unsupportedDataTypeError(typeName: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> ("\"" + typeName.sql.toUpperCase(Locale.ROOT) + "\"")))
  }

  /* Copy from copy of org.apache.spark.sql.errors.ExecutionErrors for Spark version compatibility */
  private def duplicatedFieldNameInArrowStructError(fieldNames: Seq[String])
      : SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
      messageParameters = Map("fieldNames" -> fieldNames.mkString("[", ", ", "]")))
  }
}
