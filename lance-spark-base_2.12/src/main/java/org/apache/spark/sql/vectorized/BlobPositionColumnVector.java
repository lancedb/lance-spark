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
package org.apache.spark.sql.vectorized;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/** A column vector that provides the position values from a blob struct. */
public class BlobPositionColumnVector extends ColumnVector {
  private final BlobStructAccessor blobAccessor;

  public BlobPositionColumnVector(BlobStructAccessor blobAccessor) {
    super(DataTypes.LongType);
    this.blobAccessor = blobAccessor;
  }

  @Override
  public void close() {
    // Cleanup handled by parent
  }

  @Override
  public boolean hasNull() {
    return blobAccessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return blobAccessor.getNullCount();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return blobAccessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("Cannot get boolean from blob position");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Cannot get byte from blob position");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("Cannot get short from blob position");
  }

  @Override
  public int getInt(int rowId) {
    Long position = blobAccessor.getPosition(rowId);
    return position != null ? position.intValue() : 0;
  }

  @Override
  public long getLong(int rowId) {
    Long position = blobAccessor.getPosition(rowId);
    return position != null ? position : 0L;
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("Cannot get float from blob position");
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("Cannot get double from blob position");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Cannot get array from blob position");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException("Cannot get map from blob position");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("Cannot get decimal from blob position");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("Cannot get string from blob position");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Cannot get binary from blob position");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Blob position has no children");
  }
}
