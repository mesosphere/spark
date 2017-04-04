/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.execution.datasources.DataSourceAnalysis
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class DataSourceAnalysisSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var targetAttributes: Seq[Attribute] = _
  private var targetPartitionSchema: StructType = _

  override def beforeAll(): Unit = {
    targetAttributes = Seq('a.int, 'd.int, 'b.int, 'c.int)
    targetPartitionSchema = new StructType()
      .add("b", IntegerType)
      .add("c", IntegerType)
  }

  private def checkProjectList(actual: Seq[Expression], expected: Seq[Expression]): Unit = {
    // Remove aliases since we have no control on their exprId.
    val withoutAliases = actual.map {
      case alias: Alias => alias.child
      case other => other
    }
    assert(withoutAliases === expected)
  }

  Seq(true, false).foreach { caseSensitive =>
    val rule = DataSourceAnalysis(new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive))
    test(
      s"convertStaticPartitions only handle INSERT having at least static partitions " +
        s"(caseSensitive: $caseSensitive)") {
      intercept[AssertionError] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int),
          providedPartitions = Map("b" -> None, "c" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    test(s"Missing columns (caseSensitive: $caseSensitive)") {
      // Missing columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int),
          providedPartitions = Map("b" -> Some("1"), "c" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    test(s"Missing partitioning columns (caseSensitive: $caseSensitive)") {
      // Missing partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int),
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Missing partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int, 'g.int),
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int),
          providedPartitions = Map("b" -> Some("1"), "d" -> None),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    test(s"Wrong partitioning columns (caseSensitive: $caseSensitive)") {
      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int),
          providedPartitions = Map("b" -> Some("1"), "d" -> Some("2")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      // Wrong partitioning columns.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int),
          providedPartitions = Map("b" -> Some("1"), "c" -> Some("3"), "d" -> Some("2")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }

      if (caseSensitive) {
        // Wrong partitioning columns.
        intercept[AnalysisException] {
          rule.convertStaticPartitions(
            sourceAttributes = Seq('e.int, 'f.int),
            providedPartitions = Map("b" -> Some("1"), "C" -> Some("3")),
            targetAttributes = targetAttributes,
            targetPartitionSchema = targetPartitionSchema)
        }
      }
    }

    test(
      s"Static partitions need to appear before dynamic partitions" +
      s" (caseSensitive: $caseSensitive)") {
      // Static partitions need to appear before dynamic partitions.
      intercept[AnalysisException] {
        rule.convertStaticPartitions(
          sourceAttributes = Seq('e.int, 'f.int),
          providedPartitions = Map("b" -> None, "c" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
      }
    }

    test(s"All static partitions (caseSensitive: $caseSensitive)") {
      if (!caseSensitive) {
        val nonPartitionedAttributes = Seq('e.int, 'f.int)
        val expected = nonPartitionedAttributes ++
          Seq(Cast(Literal("1"), IntegerType), Cast(Literal("3"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1"), "C" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
        checkProjectList(actual, expected)
      }

      {
        val nonPartitionedAttributes = Seq('e.int, 'f.int)
        val expected = nonPartitionedAttributes ++
          Seq(Cast(Literal("1"), IntegerType), Cast(Literal("3"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1"), "c" -> Some("3")),
          targetAttributes = targetAttributes,
          targetPartitionSchema = targetPartitionSchema)
        checkProjectList(actual, expected)
      }

      // Test the case having a single static partition column.
      {
        val nonPartitionedAttributes = Seq('e.int, 'f.int)
        val expected = nonPartitionedAttributes ++ Seq(Cast(Literal("1"), IntegerType))
        val actual = rule.convertStaticPartitions(
          sourceAttributes = nonPartitionedAttributes,
          providedPartitions = Map("b" -> Some("1")),
          targetAttributes = Seq('a.int, 'd.int, 'b.int),
          targetPartitionSchema = new StructType().add("b", IntegerType))
        checkProjectList(actual, expected)
      }
    }

    test(s"Static partition and dynamic partition (caseSensitive: $caseSensitive)") {
      val nonPartitionedAttributes = Seq('e.int, 'f.int)
      val dynamicPartitionAttributes = Seq('g.int)
      val expected =
        nonPartitionedAttributes ++
          Seq(Cast(Literal("1"), IntegerType)) ++
          dynamicPartitionAttributes
      val actual = rule.convertStaticPartitions(
        sourceAttributes = nonPartitionedAttributes ++ dynamicPartitionAttributes,
        providedPartitions = Map("b" -> Some("1"), "c" -> None),
        targetAttributes = targetAttributes,
        targetPartitionSchema = targetPartitionSchema)
      checkProjectList(actual, expected)
    }
  }
}
