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
package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{AddColumnsBackfill, LogicalPlan}

import scala.jdk.CollectionConverters._

class LanceSqlExtensionsAstBuilder(delegate: ParserInterface)
  extends LanceSqlExtensionsBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: LanceSqlExtensionsParser.SingleStatementContext)
      : LogicalPlan = {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitAddColumnsBackfill(ctx: LanceSqlExtensionsParser.AddColumnsBackfillContext)
      : AddColumnsBackfill = {
    val table = UnresolvedIdentifier(visitMultipartIdentifier(ctx.multipartIdentifier()))
    val columnNames = visitColumnList(ctx.columnList())
    val source = UnresolvedRelation(Seq(ctx.identifier().getText))
    AddColumnsBackfill(table, columnNames, source)
  }

  override def visitMultipartIdentifier(ctx: LanceSqlExtensionsParser.MultipartIdentifierContext)
      : Seq[String] = {
    ctx.parts.asScala.map(_.getText).toSeq
  }

  /**
   * Visit identifier list.
   */
  override def visitColumnList(ctx: LanceSqlExtensionsParser.ColumnListContext): Seq[String] = {
    ctx.columns.asScala.map(_.getText).toSeq
  }
}
