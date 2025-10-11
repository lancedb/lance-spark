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
package org.apache.spark.sql.execution.datasources.v2

import com.lancedb.lance.spark.{LanceConstant, LanceDataset}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog._

case class AddColumnsBackfillExec(
    catalog: TableCatalog,
    ident: Identifier,
    columnNames: Seq[String],
    query: LogicalPlan)
  extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val table = catalog.loadTable(ident) match {
      case lanceTable: LanceDataset => new LanceDataset(lanceTable.config(), query.schema)
      case _ =>
        throw new UnsupportedOperationException("AddColumnsBackfill only support LanceDataset")
    }

    val relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    val append =
      AppendData.byPosition(
        relation,
        query,
        Map(LanceConstant.BACKFILL_COLUMNS_KEY -> columnNames.mkString(",")))
    val qe = session.sessionState.executePlan(append)
    qe.assertCommandExecuted()

    Nil
  }
}
