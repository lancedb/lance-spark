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
package com.lancedb.lance.spark.internal;

import com.lancedb.lance.spark.read.LanceInputPartition;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class LanceFragmentColumnarBatchScanner implements AutoCloseable {
  private final LanceFragmentScanner fragmentScanner;
  private final ArrowReader arrowReader;
  private ColumnarBatch currentColumnarBatch;

  public LanceFragmentColumnarBatchScanner(
      LanceFragmentScanner fragmentScanner, ArrowReader arrowReader) {
    this.fragmentScanner = fragmentScanner;
    this.arrowReader = arrowReader;
  }

  public static LanceFragmentColumnarBatchScanner create(
      int fragmentId, LanceInputPartition inputPartition) {
    LanceFragmentScanner fragmentScanner =
        LanceDatasetAdapter.getFragmentScanner(fragmentId, inputPartition);
    return new LanceFragmentColumnarBatchScanner(fragmentScanner, fragmentScanner.getArrowReader());
  }

  public boolean loadNextBatch() throws IOException {
    if (arrowReader.loadNextBatch()) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

      List<ColumnVector> fieldVectors =
          root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .collect(Collectors.toList());
      if (fragmentScanner.withFragemtId()) {
        ConstantColumnVector fragmentVector =
            new ConstantColumnVector(root.getRowCount(), DataTypes.IntegerType);
        fragmentVector.setInt(fragmentScanner.fragmentId());
        fieldVectors.add(fragmentVector);
      }

      currentColumnarBatch =
          new ColumnarBatch(fieldVectors.toArray(new ColumnVector[] {}), root.getRowCount());
      return true;
    }
    return false;
  }

  /** @return the current batch, the caller responsible for closing the batch */
  public ColumnarBatch getCurrentBatch() {
    return currentColumnarBatch;
  }

  @Override
  public void close() throws IOException {
    if (currentColumnarBatch != null) {
      currentColumnarBatch.close();
    }
    if (currentColumnarBatch != null) {
      currentColumnarBatch.close();
    }
    arrowReader.close();
    fragmentScanner.close();
  }
}
