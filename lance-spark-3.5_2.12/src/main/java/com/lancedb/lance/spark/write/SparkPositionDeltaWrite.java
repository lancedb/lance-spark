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
package com.lancedb.lance.spark.write;

import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.RowAddress;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.spark.LanceConfig;
import com.lancedb.lance.spark.LanceConstant;
import com.lancedb.lance.spark.SparkOptions;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private final StructType sparkSchema;
  private final LanceConfig config;

  public SparkPositionDeltaWrite(StructType sparkSchema, LanceConfig config) {
    this.sparkSchema = sparkSchema;
    this.config = config;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return Distributions.clustered(new NamedReference[] {segmentId});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue =
        new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[] {sortValue};
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new PositionDeltaBatchWrite();
  }

  private class PositionDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new PositionDeltaWriteFactory(sparkSchema, config);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();
      List<FragmentMetadata> newFragments = new ArrayList<>();

      Arrays.stream(messages)
          .map(m -> (DeltaWriteTaskCommit) m)
          .forEach(
              m -> {
                removedFragmentIds.addAll(m.removedFragmentIds());
                updatedFragments.addAll(m.updatedFragments());
                newFragments.addAll(m.newFragments());
              });

      LanceDatasetAdapter.updateFragments(
          config, removedFragmentIds, updatedFragments, newFragments);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {}
  }

  private static class PositionDeltaWriteFactory implements DeltaWriterFactory {
    private final StructType sparkSchema;
    private final LanceConfig config;

    PositionDeltaWriteFactory(StructType sparkSchema, LanceConfig config) {
      this.sparkSchema = sparkSchema;
      this.config = config;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      int batch_size = SparkOptions.getBatchSize(config);
      LanceArrowWriter arrowWriter = LanceDatasetAdapter.getArrowWriter(sparkSchema, batch_size);
      WriteParams params = SparkOptions.genWriteParamsFromConfig(config);
      Callable<List<FragmentMetadata>> fragmentCreator =
          () -> LanceDatasetAdapter.createFragment(config.getDatasetUri(), arrowWriter, params);
      FutureTask<List<FragmentMetadata>> fragmentCreationTask = new FutureTask<>(fragmentCreator);
      Thread fragmentCreationThread = new Thread(fragmentCreationTask);
      fragmentCreationThread.start();

      return new LanceDeltaWriter(
          config, new LanceDataWriter(arrowWriter, fragmentCreationTask, fragmentCreationThread));
    }
  }

  private static class LanceDeltaWriter implements DeltaWriter<InternalRow> {
    private final LanceConfig config;
    private final LanceDataWriter writer;

    // Key is fragmentId, Value is fragment's deleted row indexes
    private final Map<Integer, RoaringBitmap> deletedRows;

    private LanceDeltaWriter(LanceConfig config, LanceDataWriter writer) {
      this.config = config;
      this.writer = writer;
      this.deletedRows = new HashMap<>();
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      int fragmentId = metadata.getInt(0);
      deletedRows.compute(
          fragmentId,
          (k, v) -> {
            if (v == null) {
              v = new RoaringBitmap();
            }
            // Get the row index which is low 32 bits of row address.
            // See
            // https://github.com/lancedb/lance/blob/main/rust/lance-core/src/utils/address.rs#L36
            v.add(RowAddress.rowIndex(id.getLong(0)));
            return v;
          });
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {
      throw new UnsupportedOperationException("Update is not supported");
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      writer.write(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      // Write new fragments to store new updated rows.
      LanceBatchWrite.TaskCommit append = (LanceBatchWrite.TaskCommit) writer.commit();
      List<FragmentMetadata> newFragments = append.getFragments();

      List<Long> removedFragmentIds = new ArrayList<>();
      List<FragmentMetadata> updatedFragments = new ArrayList<>();

      // Deleting updated rows from old fragments.
      this.deletedRows.forEach(
          (fragmentId, rowIndexes) -> {
            FragmentMetadata updatedFragment =
                LanceDatasetAdapter.deleteRows(
                    config, fragmentId, ImmutableList.copyOf(rowIndexes));
            if (updatedFragment != null) {
              updatedFragments.add(updatedFragment);
            } else {
              removedFragmentIds.add(Long.valueOf(fragmentId));
            }
          });

      return new DeltaWriteTaskCommit(removedFragmentIds, updatedFragments, newFragments);
    }

    @Override
    public void abort() throws IOException {
      writer.abort();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  private static class DeltaWriteTaskCommit implements WriterCommitMessage {
    private List<Long> removedFragmentIds;
    private List<FragmentMetadata> updatedFragments;
    private List<FragmentMetadata> newFragments;

    DeltaWriteTaskCommit(
        List<Long> removedFragmentIds,
        List<FragmentMetadata> updatedFragments,
        List<FragmentMetadata> newFragments) {
      this.removedFragmentIds = removedFragmentIds;
      this.updatedFragments = updatedFragments;
      this.newFragments = newFragments;
    }

    public List<Long> removedFragmentIds() {
      return removedFragmentIds == null ? Collections.emptyList() : removedFragmentIds;
    }

    public List<FragmentMetadata> updatedFragments() {
      return updatedFragments == null ? Collections.emptyList() : updatedFragments;
    }

    public List<FragmentMetadata> newFragments() {
      return newFragments == null ? Collections.emptyList() : newFragments;
    }
  }
}
