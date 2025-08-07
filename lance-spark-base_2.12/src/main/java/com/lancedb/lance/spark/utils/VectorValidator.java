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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates vector columns in Spark DataFrames for Lance FixedSizeList conversion. This validator
 * ensures that arrays marked as vectors have consistent dimensions.
 */
public class VectorValidator {
  private static class VectorFieldInfo {
    final int fieldIndex;
    final String fieldName;
    final int expectedDimension;
    final DataType elementType;

    VectorFieldInfo(int fieldIndex, String fieldName, int expectedDimension, DataType elementType) {
      this.fieldIndex = fieldIndex;
      this.fieldName = fieldName;
      this.expectedDimension = expectedDimension;
      this.elementType = elementType;
    }
  }

  private final List<VectorFieldInfo> vectorFields;

  public VectorValidator(StructType schema) {
    this.vectorFields = new ArrayList<>();

    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (field.dataType() instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) field.dataType();
        DataType elementType = arrayType.elementType();

        if ((elementType instanceof FloatType || elementType instanceof DoubleType)
            && field.metadata() != null
            && field.metadata().contains(LanceArrowUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY())) {

          long dimension =
              field.metadata().getLong(LanceArrowUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY());
          vectorFields.add(new VectorFieldInfo(i, field.name(), (int) dimension, elementType));
        }
      }
    }
  }

  public boolean hasVectorFields() {
    return !vectorFields.isEmpty();
  }

  public void validateRow(InternalRow row) {
    for (VectorFieldInfo fieldInfo : vectorFields) {
      if (row.isNullAt(fieldInfo.fieldIndex)) {
        throw new IllegalArgumentException(
            String.format("Vector field '%s' cannot be null", fieldInfo.fieldName));
      }

      UnsafeArrayData array = (UnsafeArrayData) row.getArray(fieldInfo.fieldIndex);

      if (array == null) {
        throw new IllegalArgumentException(
            String.format("Vector field '%s' cannot be null", fieldInfo.fieldName));
      }

      int actualDimension = array.numElements();
      if (actualDimension != fieldInfo.expectedDimension) {
        throw new IllegalArgumentException(
            String.format(
                "Vector field '%s' has dimension %d but expected %d",
                fieldInfo.fieldName, actualDimension, fieldInfo.expectedDimension));
      }
    }
  }
}
