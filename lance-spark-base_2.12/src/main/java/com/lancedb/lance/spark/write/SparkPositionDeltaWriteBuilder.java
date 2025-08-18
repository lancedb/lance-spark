package com.lancedb.lance.spark.write;

import com.lancedb.lance.spark.LanceConfig;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {
  private final StructType sparkSchema;
  private final LanceConfig config;

  public SparkPositionDeltaWriteBuilder(StructType sparkSchema, LanceConfig config) {
    this.sparkSchema = sparkSchema;
    this.config = config;
  }

  public DeltaWrite build() {
    return new SparkPositionDeltaWrite(sparkSchema, config);
  }
}
