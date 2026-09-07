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

package org.apache.spark.sql.execution.exchange

import java.nio.charset.StandardCharsets

import org.apache.spark.shuffle.{
  ShuffleRecoveryCanonicalValue,
  ShuffleRecoveryComputationIdentity,
  ShuffleRecoveryIntValue,
  ShuffleRecoveryMapperDecomposition,
  ShuffleRecoveryMapperSplit,
  ShuffleRecoverySourceToken}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Literal, Rand, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ShuffleRecoveryComputationIdentityBuilderSuite extends SharedSparkSession {
  import ShuffleRecoveryMissReason._

  test("equivalent independently planned shuffles produce byte-identical identities") {
    val firstPlan = filteredRangePlan()
    val secondPlan = filteredRangePlan()
    val first = build(hashExchange(firstPlan), firstPlan)
    val second = build(hashExchange(secondPlan), secondPlan)

    assert(first.canonicalPayload === second.canonicalPayload)
    assert(first.digest === second.digest)
  }

  test("attempt-local plan and object identity do not enter the semantic identity") {
    val firstPlan = projectedLiteralRange(7)
    val secondPlan = projectedLiteralRange(7)
    val firstExchange = hashExchange(firstPlan)
    val secondExchange = hashExchange(secondPlan)

    assert(firstPlan ne secondPlan)
    assert(firstExchange ne secondExchange)
    assert(build(firstExchange, firstPlan).canonicalPayload ===
      build(secondExchange, secondPlan).canonicalPayload)
  }

  test("literal, output nullability, partitioning, source and decomposition mutations differ") {
    val literalSeven = projectedLiteralRange(7)
    val literalEight = projectedLiteralRange(8)
    assert(build(hashExchange(literalSeven), literalSeven).digest !==
      build(hashExchange(literalEight), literalEight).digest)

    val nullable = projectedNullRange()
    assert(build(hashExchange(nullable), nullable).digest !==
      build(hashExchange(literalSeven), literalSeven).digest)

    assert(build(hashExchange(literalSeven, 2), literalSeven).digest !==
      build(hashExchange(literalSeven, 3), literalSeven).digest)

    val sourceA = build(hashExchange(literalSeven), literalSeven, source = "source-A")
    val sourceB = build(hashExchange(literalSeven), literalSeven, source = "source-B")
    assert(sourceA.digest !== sourceB.digest)

    val splitA = build(hashExchange(literalSeven), literalSeven, firstSplit = "split-0")
    val splitB = build(hashExchange(literalSeven), literalSeven, firstSplit = "split-mutated")
    assert(splitA.digest !== splitB.digest)
  }

  test("ANSI and session timezone are mandatory semantic discriminators") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val base = build(exchange, plan, ansi = false, timeZone = "UTC")

    assert(base.digest !== build(exchange, plan, ansi = true, timeZone = "UTC").digest)
    assert(base.digest !==
      build(exchange, plan, ansi = false, timeZone = "America/Los_Angeles").digest)
  }

  test("resolved runtime values are ordered canonically and remain semantic") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val first = build(
      exchange,
      plan,
      resolvedValues = Map(
        "z" -> ShuffleRecoveryIntValue(2),
        "a" -> ShuffleRecoveryIntValue(1)))
    val second = build(
      exchange,
      plan,
      resolvedValues = List(
        "a" -> ShuffleRecoveryIntValue(1),
        "z" -> ShuffleRecoveryIntValue(2)).toMap)
    val mutated = build(
      exchange,
      plan,
      resolvedValues = Map(
        "a" -> ShuffleRecoveryIntValue(1),
        "z" -> ShuffleRecoveryIntValue(3)))

    assert(first.canonicalPayload === second.canonicalPayload)
    assert(first.digest !== mutated.digest)
  }

  test("missing source identity fails closed") {
    val plan = projectedLiteralRange(7)
    val exchange = hashExchange(plan)
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Nil,
      decomposition(),
      Map.empty,
      ShuffleRecoveryIdentitySemanticConfig(ansiEnabled = false, "UTC"))

    assert(ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs) ===
      ShuffleRecoveryIdentityRejected(SourceTokenUnavailable))
  }

  test("nondeterministic expressions fail closed before identity creation") {
    val child = rangePlan()
    val project = ProjectExec(Seq(Alias(Rand(7L), "random")()), child)
    val exchange = ShuffleExchangeExec(HashPartitioning(project.output.take(1), 2), project)

    assert(buildResult(exchange, project) === ShuffleRecoveryIdentityRejected(NonDeterministic))
  }

  test("unmodeled operators fail closed even when the opportunity allowlist knows them") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      val aggregatePlan = spark.range(0, 32, 1, 4).groupBy().count().queryExecution.executedPlan
      val exchange =
        ShuffleExchangeExec(HashPartitioning(aggregatePlan.output.take(1), 2), aggregatePlan)

      assert(buildResult(exchange, aggregatePlan) ===
        ShuffleRecoveryIdentityRejected(UnsupportedOperator))
    }
  }

  test("range partitioning remains outside the supported identity slice") {
    val plan = rangePlan()
    val ordering = Seq(SortOrder(plan.output.head, Ascending))
    val exchange = ShuffleExchangeExec(RangePartitioning(ordering, 2), plan)

    assert(buildResult(exchange, plan) ===
      ShuffleRecoveryIdentityRejected(RangePartitioningPresent))
  }

  test("string hash partitioning fails closed until collation hashing switches are modeled") {
    val plan = spark.range(0, 8, 1, 2)
      .selectExpr("cast(id as string) as key")
      .queryExecution.executedPlan
    val exchange = ShuffleExchangeExec(HashPartitioning(plan.output.take(1), 2), plan)

    assert(buildResult(exchange, plan) ===
      ShuffleRecoveryIdentityRejected(UnsupportedExpression))
  }

  private def rangePlan(): SparkPlan =
    spark.range(0, 32, 1, 4).queryExecution.executedPlan

  private def filteredRangePlan(): SparkPlan =
    spark.range(0, 32, 1, 4).where("id > 7").queryExecution.executedPlan

  private def projectedLiteralRange(value: Int): SparkPlan = {
    val child = rangePlan()
    ProjectExec(Seq(Alias(Literal(value), "value")()), child)
  }

  private def projectedNullRange(): SparkPlan = {
    val child = rangePlan()
    ProjectExec(Seq(Alias(Literal.create(null, IntegerType), "value")()), child)
  }

  private def hashExchange(child: SparkPlan, partitions: Int = 2): ShuffleExchangeExec =
    ShuffleExchangeExec(HashPartitioning(child.output.take(1), partitions), child)

  private def build(
      exchange: ShuffleExchangeExec,
      child: SparkPlan,
      source: String = "source-A",
      firstSplit: String = "split-0",
      ansi: Boolean = false,
      timeZone: String = "UTC",
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue] = Map.empty)
      : ShuffleRecoveryComputationIdentity = {
    buildResult(
      exchange,
      child,
      source,
      firstSplit,
      ansi,
      timeZone,
      resolvedValues) match {
      case ShuffleRecoveryIdentityBuilt(identity) => identity
      case ShuffleRecoveryIdentityRejected(reason) => fail(s"identity rejected: ${reason.code}")
    }
  }

  private def buildResult(
      exchange: ShuffleExchangeExec,
      child: SparkPlan,
      source: String = "source-A",
      firstSplit: String = "split-0",
      ansi: Boolean = false,
      timeZone: String = "UTC",
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue] = Map.empty)
      : ShuffleRecoveryIdentityBuildResult = {
    val range = rangeLeaf(child)
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Seq(range -> ShuffleRecoverySourceToken.copyOf(
        1, source.getBytes(StandardCharsets.UTF_8))),
      decomposition(firstSplit),
      resolvedValues,
      ShuffleRecoveryIdentitySemanticConfig(ansi, timeZone))
    ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs)
  }

  private def rangeLeaf(plan: SparkPlan): RangeExec = {
    plan.collectFirst { case range: RangeExec => range }
      .getOrElse(fail(s"expected RangeExec below ${plan.nodeName}"))
  }

  private def decomposition(firstSplit: String = "split-0"): ShuffleRecoveryMapperDecomposition = {
    ShuffleRecoveryMapperDecomposition(
      4,
      (0 until 4).map { index =>
        val descriptor = if (index == 0) firstSplit else s"split-$index"
        ShuffleRecoveryMapperSplit.copyOf(
          0,
          index,
          1,
          descriptor.getBytes(StandardCharsets.UTF_8))
      }.toVector)
  }
}
