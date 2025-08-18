package com.lancedb.lance.spark.write;

import com.lancedb.lance.spark.LanceConfig;
import com.lancedb.lance.spark.LanceConstant;
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

import java.io.IOException;

public class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private final StructType sparkSchema;
  private final LanceConfig config;

  public SparkPositionDeltaWrite(StructType sparkSchema, LanceConfig config) {
    this.sparkSchema = sparkSchema;
    this.config = config;
  }

  @Override
  public Distribution requiredDistribution() {
    NamedReference segmentId = Expressions.column(LanceConstant.SEGMENT_ID);
    return Distributions.clustered(new NamedReference[]{segmentId});
  }

  @Override
  public SortOrder[] requiredOrdering() {
    NamedReference segmentId = Expressions.column(LanceConstant.ROW_ADDRESS);
    SortValue sortValue = new SortValue(segmentId, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    return new SortValue[]{sortValue};
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new PositionDeltaBatchWrite();
  }

  private class PositionDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new PositionDeltaWriteFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
  }

  private static class PositionDeltaWriteFactory implements DeltaWriterFactory {

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return null;
    }
  }

  private static class LanceDeltaWriter implements DeltaWriter<InternalRow> {

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {

    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) throws IOException {

    }

    @Override
    public void insert(InternalRow row) throws IOException {

    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      return null;
    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
  }
}
