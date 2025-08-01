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
package com.lancedb.lance.spark.utils;

import com.lancedb.lance.namespace.model.JsonArrowDataType;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.JsonArrowSchema;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.types.YearMonthIntervalType;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting Spark schema types to JsonArrow schema types used by the Lance
 * Namespace API.
 */
public class SchemaConverter {

  private SchemaConverter() {
    // Utility class
  }

  /**
   * Converts a Spark StructType to JsonArrowSchema.
   *
   * @param sparkSchema the Spark StructType to convert
   * @return JsonArrowSchema representation
   */
  public static JsonArrowSchema toJsonArrowSchema(StructType sparkSchema) {
    JsonArrowSchema jsonSchema = new JsonArrowSchema();
    List<JsonArrowField> fields = new ArrayList<>();

    for (StructField sparkField : sparkSchema.fields()) {
      fields.add(toJsonArrowField(sparkField));
    }

    jsonSchema.setFields(fields);
    return jsonSchema;
  }

  /**
   * Converts a Spark StructField to JsonArrowField.
   *
   * @param sparkField the Spark StructField to convert
   * @return JsonArrowField representation
   */
  private static JsonArrowField toJsonArrowField(StructField sparkField) {
    JsonArrowField field = new JsonArrowField();
    field.setName(sparkField.name());
    field.setNullable(sparkField.nullable());
    field.setType(toJsonArrowDataType(sparkField.dataType(), sparkField.name()));
    return field;
  }

  /**
   * Converts a Spark DataType to JsonArrowDataType.
   *
   * @param sparkType the Spark DataType to convert
   * @param fieldName the name of the field (used for special cases like ROW_ID)
   * @return JsonArrowDataType representation
   */
  private static JsonArrowDataType toJsonArrowDataType(DataType sparkType, String fieldName) {
    JsonArrowDataType dataType = new JsonArrowDataType();

    if (sparkType instanceof BooleanType) {
      dataType.setType("bool");
    } else if (sparkType instanceof ByteType) {
      dataType.setType("int8");
    } else if (sparkType instanceof ShortType) {
      dataType.setType("int16");
    } else if (sparkType instanceof IntegerType) {
      dataType.setType("int32");
    } else if (sparkType instanceof LongType) {
      dataType.setType("int64");
      // Special handling for ROW_ID field (unsigned 64-bit)
      // Note: JsonArrowDataType doesn't have signed/unsigned distinction in this simple mapping
    } else if (sparkType instanceof FloatType) {
      dataType.setType("float32");
    } else if (sparkType instanceof DoubleType) {
      dataType.setType("float64");
    } else if (sparkType instanceof StringType) {
      dataType.setType("string");
    } else if (sparkType instanceof BinaryType) {
      dataType.setType("binary");
    } else if (sparkType instanceof DateType) {
      dataType.setType("date");
    } else if (sparkType instanceof TimestampType) {
      dataType.setType("timestamp");
    } else if (sparkType instanceof TimestampNTZType) {
      dataType.setType("timestamp");
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      dataType.setType("decimal");
      // Note: precision and scale would need additional fields if supported
    } else if (sparkType instanceof NullType) {
      dataType.setType("null");
    } else if (sparkType instanceof YearMonthIntervalType) {
      dataType.setType("interval");
    } else if (sparkType instanceof DayTimeIntervalType) {
      dataType.setType("duration");
    } else if (sparkType instanceof CalendarIntervalType) {
      dataType.setType("interval");
    } else if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      dataType.setType("list");
      // Create element field
      JsonArrowField elementField = new JsonArrowField();
      elementField.setName("element");
      elementField.setNullable(arrayType.containsNull());
      elementField.setType(toJsonArrowDataType(arrayType.elementType(), "element"));
      List<JsonArrowField> fields = new ArrayList<>();
      fields.add(elementField);
      dataType.setFields(fields);
    } else if (sparkType instanceof StructType) {
      StructType structType = (StructType) sparkType;
      dataType.setType("struct");
      List<JsonArrowField> fields = new ArrayList<>();
      for (StructField field : structType.fields()) {
        fields.add(toJsonArrowField(field));
      }
      dataType.setFields(fields);
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      dataType.setType("map");
      List<JsonArrowField> fields = new ArrayList<>();
      // Create struct field containing key and value
      JsonArrowField mapStructField = new JsonArrowField();
      mapStructField.setName("entries");
      mapStructField.setNullable(false);
      JsonArrowDataType mapStructType = new JsonArrowDataType();
      mapStructType.setType("struct");
      List<JsonArrowField> structFields = new ArrayList<>();
      // Key field
      JsonArrowField keyField = new JsonArrowField();
      keyField.setName("key");
      keyField.setNullable(false);
      keyField.setType(toJsonArrowDataType(mapType.keyType(), "key"));
      structFields.add(keyField);
      // Value field
      JsonArrowField valueField = new JsonArrowField();
      valueField.setName("value");
      valueField.setNullable(mapType.valueContainsNull());
      valueField.setType(toJsonArrowDataType(mapType.valueType(), "value"));
      structFields.add(valueField);
      mapStructType.setFields(structFields);
      mapStructField.setType(mapStructType);
      fields.add(mapStructField);
      dataType.setFields(fields);
    } else if (sparkType instanceof UserDefinedType) {
      UserDefinedType<?> udt = (UserDefinedType<?>) sparkType;
      return toJsonArrowDataType(udt.sqlType(), fieldName);
    } else {
      throw new IllegalArgumentException("Unsupported Spark data type: " + sparkType);
    }

    return dataType;
  }
}
