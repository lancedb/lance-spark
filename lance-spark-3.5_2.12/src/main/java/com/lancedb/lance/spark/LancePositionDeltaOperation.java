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
package com.lancedb.lance.spark;

import com.lancedb.lance.spark.read.LanceScanBuilder;
import com.lancedb.lance.spark.write.SparkPositionDeltaWriteBuilder;

import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.SupportsDelta;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class LancePositionDeltaOperation implements RowLevelOperation, SupportsDelta {
  private final Command command;
  private final StructType sparkSchema;
  private final LanceConfig config;

  public LancePositionDeltaOperation(Command command, StructType sparkSchema, LanceConfig config) {
    this.command = command;
    this.sparkSchema = sparkSchema;
    this.config = config;
  }

  @Override
  public Command command() {
    return command;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    return new LanceScanBuilder(sparkSchema, config);
  }

  @Override
  public DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    return new SparkPositionDeltaWriteBuilder(sparkSchema, config);
  }

  @Override
  public NamedReference[] rowId() {
    NamedReference rowAddr = Expressions.column(LanceConstant.ROW_ADDRESS);
    return new NamedReference[] {rowAddr};
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    NamedReference segmentId = Expressions.column(LanceConstant.FRAGMENT_ID);
    return new NamedReference[] {segmentId};
  }

  @Override
  public boolean representUpdateAsDeleteAndInsert() {
    return true;
  }
}
