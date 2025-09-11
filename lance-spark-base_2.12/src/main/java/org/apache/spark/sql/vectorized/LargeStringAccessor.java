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

import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Accessor for LargeVarCharVector to handle large string data.
 * LargeVarCharVector uses 64-bit offsets instead of 32-bit offsets,
 * allowing for larger string data storage.
 */
public class LargeStringAccessor {
  private final LargeVarCharVector vector;

  public LargeStringAccessor(LargeVarCharVector vector) {
    this.vector = vector;
  }

  public void close() {
    // Vector is managed externally
  }

  public int getNullCount() {
    return vector.getNullCount();
  }

  public boolean isNullAt(int rowId) {
    return vector.isNull(rowId);
  }

  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    byte[] bytes = vector.get(rowId);
    return UTF8String.fromBytes(bytes);
  }
}