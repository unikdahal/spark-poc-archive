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

import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.shuffle._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] sealed trait ShuffleRecoveryIdentityBuildResult
private[sql] final case class ShuffleRecoveryIdentityBuilt(
    identity: ShuffleRecoveryComputationIdentity) extends ShuffleRecoveryIdentityBuildResult
private[sql] final case class ShuffleRecoveryIdentityRejected(
    reason: ShuffleRecoveryMissReason) extends ShuffleRecoveryIdentityBuildResult

/**
 * Semantic configuration resolved before computation-identity construction.
 *
 * These fields are mandatory rather than user-selectable identity options. Over-including a
 * semantic setting can cause a safe cache miss; allowing callers to omit a required setting could
 * cause a false match.
 */
private[sql] final case class ShuffleRecoveryIdentitySemanticConfig(
    ansiEnabled: Boolean,
    sessionTimeZone: String)

/**
 * Already resolved, immutable facts consumed at the shuffle materialization boundary.
 *
 * Source-token interpretation belongs to the source-read identity boundary. This type only owns
 * exact ordered inclusion of those opaque tokens and the mapper-decomposition descriptors. Plan
 * object identity is used solely as an in-process lookup key and is never encoded.
 */
private[sql] final class ShuffleRecoveryResolvedIdentityInputs private (
    private val sourceTokens: IdentityHashMap[SparkPlan, ShuffleRecoverySourceToken],
    val mapperDecomposition: ShuffleRecoveryMapperDecomposition,
    val resolvedValues: Map[String, ShuffleRecoveryCanonicalValue],
    val semanticConfig: ShuffleRecoveryIdentitySemanticConfig,
    private val parentIdentities:
      IdentityHashMap[ShuffleExchangeExec, ShuffleRecoveryComputationIdentity]) {

  private[exchange] def sourceTokenFor(plan: SparkPlan): Option[ShuffleRecoverySourceToken] =
    Option(sourceTokens.get(plan))

  private[exchange] def parentIdentityFor(
      exchange: ShuffleExchangeExec): Option[ShuffleRecoveryComputationIdentity] =
    Option(parentIdentities.get(exchange))
}

private[sql] object ShuffleRecoveryResolvedIdentityInputs {
  private[sql] def create(
      sourceTokens: Seq[(SparkPlan, ShuffleRecoverySourceToken)],
      mapperDecomposition: ShuffleRecoveryMapperDecomposition,
      resolvedValues: Map[String, ShuffleRecoveryCanonicalValue],
      semanticConfig: ShuffleRecoveryIdentitySemanticConfig,
      parentIdentities: Seq[(ShuffleExchangeExec, ShuffleRecoveryComputationIdentity)] = Nil)
      : ShuffleRecoveryResolvedIdentityInputs = {
    require(mapperDecomposition != null, "mapper decomposition must not be null")
    require(resolvedValues != null, "resolved values must not be null")
    require(semanticConfig != null, "semantic configuration must not be null")
    require(semanticConfig.sessionTimeZone != null && semanticConfig.sessionTimeZone.nonEmpty,
      "session time zone must not be empty")

    val sources = new IdentityHashMap[SparkPlan, ShuffleRecoverySourceToken]()
    sourceTokens.foreach { case (plan, token) =>
      require(plan != null && token != null, "source token binding must not contain null")
      require(sources.put(plan, token) == null, "duplicate source plan token binding")
    }
    val parents =
      new IdentityHashMap[ShuffleExchangeExec, ShuffleRecoveryComputationIdentity]()
    parentIdentities.foreach { case (exchange, identity) =>
      require(exchange != null && identity != null,
        "parent identity binding must not contain null")
      require(parents.put(exchange, identity) == null,
        "duplicate parent exchange identity binding")
    }
    new ShuffleRecoveryResolvedIdentityInputs(
      sources,
      mapperDecomposition,
      resolvedValues,
      semanticConfig,
      parents)
  }
}

/**
 * Closed Phase 1 policy shared by identity construction and eligibility classification.
 *
 * The existing opportunity rule set remains the authoritative class allowlist. This policy adds
 * the stricter semantic requirement needed for a positive computation identity: every admitted
 * class must also have an explicit canonical encoder below. A class that is observable by the
 * opportunity study but lacks such an encoder is still a recovery miss.
 */
private[sql] object ShuffleRecoveryComputationIdentityPolicy {
  import ShuffleRecoveryMissReason._

  private val supportedPlanClasses = Set(
    classOf[ProjectExec].getName,
    classOf[FilterExec].getName,
    classOf[RangeExec].getName)

  private val supportedExpressionClasses = Set(
    classOf[Alias].getName,
    classOf[AttributeReference].getName,
    classOf[BoundReference].getName,
    classOf[Literal].getName,
    classOf[EqualTo].getName,
    classOf[EqualNullSafe].getName,
    classOf[GreaterThan].getName,
    classOf[GreaterThanOrEqual].getName,
    classOf[LessThan].getName,
    classOf[LessThanOrEqual].getName,
    classOf[And].getName,
    classOf[Or].getName,
    classOf[Not].getName,
    classOf[IsNull].getName,
    classOf[IsNotNull].getName,
    classOf[Pmod].getName,
    classOf[Murmur3Hash].getName)

  private[exchange] def planMissReason(
      plan: SparkPlan,
      rules: ShuffleRecoveryEligibilityRules): Option[ShuffleRecoveryMissReason] = {
    val className = plan.getClass.getName
    if (!rules.allowedOperatorClassNames.contains(className)) {
      Some(CustomOperator)
    } else if (!supportedPlanClasses.contains(className)) {
      Some(UnsupportedOperator)
    } else {
      None
    }
  }

  private[exchange] def expressionMissReason(
      expression: Expression,
      rules: ShuffleRecoveryEligibilityRules): Option[ShuffleRecoveryMissReason] = {
    val className = expression.getClass.getName
    if (!expression.deterministic || expression.isInstanceOf[Nondeterministic]) {
      Some(NonDeterministic)
    } else if (ShuffleRecoveryOpportunityAnalyzer
        .isPythonOrArrowExpressionClassName(className)) {
      Some(PythonOrArrowPresent)
    } else if (!rules.allowedExpressionClassNames.contains(className)) {
      Some(UnsupportedExpression)
    } else if (!supportedExpressionClasses.contains(className)) {
      Some(UnsupportedExpression)
    } else {
      None
    }
  }
}

/**
 * Builds a versioned computation identity from resolved Spark SQL shuffle semantics.
 *
 * It deliberately supports a small closed operator/expression slice. No canonicalized-plan text,
 * reflection, `toString`, scheduler ids, object ids, executor ids, or query execution ids enter the
 * payload. Range partitioning, aggregates, Python/Arrow, custom nodes, and unknown semantics fail
 * closed.
 */
private[sql] object ShuffleRecoveryComputationIdentityBuilder {
  import ShuffleRecoveryExpressionKind._
  import ShuffleRecoveryMissReason._
  import ShuffleRecoveryOperatorKind._

  private val MaxPlanDepth = 64
  private val MaxPlanNodes = 4096
  private val HashSeed = 42
  private val HashCompatibilityId = "spark-murmur3-32-seed-42-v1"
  private val RowEncodingVersion = "unsafe-row-v1"
  private val SerializerCompatibilityId = "unsafe-row-serializer-v1"
  private val CodecCompatibilityId = "spark-internal-row-v1"
  private val ShuffleWriteFormatId = "spark-sort-shuffle-unsafe-row-v1"
  private val ProviderReadFormatId = "reference-shuffle-provider-v1"

  private final class BuildContext(
      val inputs: ShuffleRecoveryResolvedIdentityInputs,
      val rules: ShuffleRecoveryEligibilityRules) {
    val activePlans = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val sourceTokens = mutable.ArrayBuffer.empty[ShuffleRecoverySourceToken]
    val parentIdentities = mutable.ArrayBuffer.empty[ShuffleRecoveryComputationIdentity]
    val parentOrdinals =
      new IdentityHashMap[ShuffleExchangeExec, java.lang.Integer]()
    var planNodes: Int = 0
  }

  private final case class BuildFailure(reason: ShuffleRecoveryMissReason)
      extends RuntimeException(null, null, false, false)

  def build(
      exchange: ShuffleExchangeExec,
      inputs: ShuffleRecoveryResolvedIdentityInputs,
      rules: ShuffleRecoveryEligibilityRules = ShuffleRecoveryEligibilityRules.conservative)
      : ShuffleRecoveryIdentityBuildResult = {
    if (exchange == null || inputs == null || rules == null) {
      return ShuffleRecoveryIdentityRejected(DeterminismUnproven)
    }
    if (exchange.pipelined) {
      return ShuffleRecoveryIdentityRejected(UnsupportedShuffleMode)
    }

    try {
      val context = new BuildContext(inputs, rules)
      val outputContract = buildOutputContract(exchange.child.output)
      val producer = buildOperator(exchange.child, context, 0)
      val partitioning = buildPartitioning(
        exchange.outputPartitioning,
        exchange.child.output,
        context)
      val semanticConfig = Map(
        "spark.sql.ansi.enabled" -> inputs.semanticConfig.ansiEnabled.toString,
        "spark.sql.session.timeZone" -> inputs.semanticConfig.sessionTimeZone)
      val compatibility = ShuffleRecoveryCompatibility(
        ShuffleRecoveryComputationIdentity.SparkCompatibilityId,
        ShuffleWriteFormatId,
        ProviderReadFormatId)
      val identity = ShuffleRecoveryComputationIdentity.create(
        outputContract,
        producer,
        partitioning,
        inputs.mapperDecomposition,
        context.sourceTokens.toVector,
        inputs.resolvedValues,
        semanticConfig,
        compatibility,
        context.parentIdentities.toVector)
      ShuffleRecoveryIdentityBuilt(identity)
    } catch {
      case BuildFailure(reason) => ShuffleRecoveryIdentityRejected(reason)
      case _: IllegalArgumentException =>
        ShuffleRecoveryIdentityRejected(DeterminismUnproven)
    }
  }

  private def buildOutputContract(output: Seq[Attribute]): ShuffleRecoveryOutputContract = {
    val fields = output.map { attribute =>
      val dataType = canonicalDataType(attribute.dataType)
      // Metadata is deliberately over-included when present. This is conservative: metadata that
      // does not affect bytes may cause a miss, but no metadata mutation can silently reuse bytes.
      val metadata = if (attribute.metadata == Metadata.empty) {
        Map.empty[String, String]
      } else {
        Map("spark-metadata-json" -> attribute.metadata.json)
      }
      ShuffleRecoveryOutputField.create(dataType, attribute.nullable, metadata)
    }.toVector
    ShuffleRecoveryOutputContract(
      fields,
      RowEncodingVersion,
      SerializerCompatibilityId,
      CodecCompatibilityId)
  }

  private def buildOperator(
      plan: SparkPlan,
      context: BuildContext,
      depth: Int): ShuffleRecoveryOperatorNode = {
    if (depth > MaxPlanDepth) {
      fail(DeterminismUnproven)
    }
    context.planNodes = Math.addExact(context.planNodes, 1)
    if (context.planNodes > MaxPlanNodes) {
      fail(DeterminismUnproven)
    }
    if (context.activePlans.put(plan, java.lang.Boolean.TRUE) != null) {
      fail(DeterminismUnproven)
    }
    try {
      ShuffleRecoveryComputationIdentityPolicy.planMissReason(plan, context.rules).foreach(fail)
      plan match {
        case project: ProjectExec =>
          val ordinals = inputOrdinals(project.child.output)
          val expressions = project.projectList.map { expression =>
            buildExpression(expression, ordinals, context, depth + 1)
          }.toVector
          ShuffleRecoveryOperatorNode(
            Project,
            Vector.empty,
            expressions,
            Vector(ShuffleRecoveryInlineOperator(
              buildOperator(project.child, context, depth + 1))))

        case filter: FilterExec =>
          val ordinals = inputOrdinals(filter.child.output)
          val condition = buildExpression(filter.condition, ordinals, context, depth + 1)
          ShuffleRecoveryOperatorNode(
            Filter,
            Vector.empty,
            Vector(condition),
            Vector(ShuffleRecoveryInlineOperator(
              buildOperator(filter.child, context, depth + 1))))

        case range: RangeExec =>
          val token = context.inputs.sourceTokenFor(range).getOrElse {
            fail(SourceTokenUnavailable)
          }
          context.sourceTokens += token
          ShuffleRecoveryOperatorNode(
            RangeSource,
            Vector(
              ShuffleRecoveryLongValue(range.start),
              ShuffleRecoveryLongValue(range.end),
              ShuffleRecoveryLongValue(range.step),
              ShuffleRecoveryIntValue(range.numSlices)),
            Vector.empty,
            Vector.empty)

        case _ => fail(UnsupportedOperator)
      }
    } finally {
      context.activePlans.remove(plan)
    }
  }

  private def buildPartitioning(
      partitioning: org.apache.spark.sql.catalyst.plans.physical.Partitioning,
      input: Seq[Attribute],
      context: BuildContext): ShuffleRecoveryPartitioning = partitioning match {
    case HashPartitioning(expressions, count) =>
      if (count <= 0) {
        fail(InvalidPartitionCount)
      }
      val ordinals = inputOrdinals(input)
      val canonicalExpressions = expressions.map { expression =>
        if (!isHashStableType(expression.dataType)) {
          fail(UnsupportedExpression)
        }
        buildExpression(expression, ordinals, context, 0)
      }.toVector
      ShuffleRecoveryHashPartitioning(
        count,
        canonicalExpressions,
        HashSeed,
        HashCompatibilityId)
    case SinglePartition => ShuffleRecoverySinglePartition
    case _: RangePartitioning => fail(RangePartitioningPresent)
    case _ => fail(UnsupportedPartitioning)
  }

  private def buildExpression(
      expression: Expression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    if (depth > MaxPlanDepth) {
      fail(DeterminismUnproven)
    }
    ShuffleRecoveryComputationIdentityPolicy
      .expressionMissReason(expression, context.rules)
      .foreach(fail)
    val dataType = canonicalDataType(expression.dataType)

    expression match {
      case literal: Literal =>
        ShuffleRecoveryExpressionNode(
          Literal,
          dataType,
          literal.nullable,
          Vector(canonicalLiteral(literal)),
          Vector.empty)

      case attribute: AttributeReference =>
        val ordinal = inputOrdinals.getOrElse(attribute.exprId, fail(DeterminismUnproven))
        ShuffleRecoveryExpressionNode(
          Input,
          dataType,
          attribute.nullable,
          Vector(ShuffleRecoveryIntValue(ordinal)),
          Vector.empty)

      case bound: BoundReference =>
        if (bound.ordinal < 0) {
          fail(DeterminismUnproven)
        }
        ShuffleRecoveryExpressionNode(
          Input,
          dataType,
          bound.nullable,
          Vector(ShuffleRecoveryIntValue(bound.ordinal)),
          Vector.empty)

      case alias: Alias =>
        // Alias names and ExprIds are routing metadata; the value-producing child is semantic.
        ShuffleRecoveryExpressionNode(
          Alias,
          dataType,
          alias.nullable,
          Vector.empty,
          Vector(buildExpression(alias.child, inputOrdinals, context, depth + 1)))

      case value: EqualTo => binary(EqualTo, value, inputOrdinals, context, depth)
      case value: EqualNullSafe =>
        binary(EqualNullSafe, value, inputOrdinals, context, depth)
      case value: GreaterThan => binary(GreaterThan, value, inputOrdinals, context, depth)
      case value: GreaterThanOrEqual =>
        binary(GreaterThanOrEqual, value, inputOrdinals, context, depth)
      case value: LessThan => binary(LessThan, value, inputOrdinals, context, depth)
      case value: LessThanOrEqual =>
        binary(LessThanOrEqual, value, inputOrdinals, context, depth)
      case value: And => binary(And, value, inputOrdinals, context, depth)
      case value: Or => binary(Or, value, inputOrdinals, context, depth)
      case value: Pmod => binary(Pmod, value, inputOrdinals, context, depth)

      case value: Not => unary(Not, value.child, value, inputOrdinals, context, depth)
      case value: IsNull =>
        unary(IsNull, value.child, value, inputOrdinals, context, depth)
      case value: IsNotNull =>
        unary(IsNotNull, value.child, value, inputOrdinals, context, depth)

      case hash: Murmur3Hash =>
        ShuffleRecoveryExpressionNode(
          Murmur3Hash,
          dataType,
          hash.nullable,
          Vector(ShuffleRecoveryIntValue(hash.seed)),
          hash.children.map(buildExpression(_, inputOrdinals, context, depth + 1)).toVector)

      case _ => fail(UnsupportedExpression)
    }
  }

  private def binary(
      kind: ShuffleRecoveryExpressionKind,
      expression: BinaryExpression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    ShuffleRecoveryExpressionNode(
      kind,
      canonicalDataType(expression.dataType),
      expression.nullable,
      Vector.empty,
      Vector(
        buildExpression(expression.left, inputOrdinals, context, depth + 1),
        buildExpression(expression.right, inputOrdinals, context, depth + 1)))
  }

  private def unary(
      kind: ShuffleRecoveryExpressionKind,
      child: Expression,
      expression: Expression,
      inputOrdinals: Map[ExprId, Int],
      context: BuildContext,
      depth: Int): ShuffleRecoveryExpressionNode = {
    ShuffleRecoveryExpressionNode(
      kind,
      canonicalDataType(expression.dataType),
      expression.nullable,
      Vector.empty,
      Vector(buildExpression(child, inputOrdinals, context, depth + 1)))
  }

  private def canonicalLiteral(literal: Literal): ShuffleRecoveryCanonicalValue = {
    if (literal.value == null) {
      return ShuffleRecoveryNullValue
    }
    literal.dataType match {
      case BooleanType => ShuffleRecoveryBooleanValue(literal.value.asInstanceOf[Boolean])
      case ByteType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Byte].toInt)
      case ShortType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Short].toInt)
      case IntegerType | DateType => ShuffleRecoveryIntValue(literal.value.asInstanceOf[Int])
      case LongType | TimestampType | TimestampNTZType =>
        ShuffleRecoveryLongValue(literal.value.asInstanceOf[Long])
      case FloatType =>
        ShuffleRecoveryCanonicalValue.float(literal.value.asInstanceOf[Float])
      case DoubleType =>
        ShuffleRecoveryCanonicalValue.double(literal.value.asInstanceOf[Double])
      case BinaryType =>
        ShuffleRecoveryCanonicalValue.binary(literal.value.asInstanceOf[Array[Byte]])
      case _: StringType =>
        ShuffleRecoveryStringValue(literal.value.asInstanceOf[UTF8String].toString)
      case _: DecimalType =>
        val decimal = literal.value.asInstanceOf[Decimal].toJavaBigDecimal
        ShuffleRecoveryCanonicalValue.decimal(
          decimal.scale(),
          decimal.unscaledValue().toByteArray)
      case _ => fail(UnsupportedExpression)
    }
  }

  private def canonicalDataType(dataType: DataType): String = dataType match {
    case BooleanType => "boolean-v1"
    case ByteType => "byte-v1"
    case ShortType => "short-v1"
    case IntegerType => "int-v1"
    case LongType => "long-v1"
    case FloatType => "float-v1"
    case DoubleType => "double-v1"
    case DateType => "date-days-v1"
    case TimestampType => "timestamp-micros-with-session-time-zone-v1"
    case TimestampNTZType => "timestamp-ntz-micros-v1"
    case BinaryType => "binary-v1"
    case decimal: DecimalType => s"decimal-v1-${decimal.precision}-${decimal.scale}"
    case string: StringType =>
      val constraint = string.constraint match {
        case NoConstraint => "none"
        case FixedLength(length) => s"fixed-$length"
        case MaxLength(length) => s"max-$length"
      }
      s"string-v1-collation-${string.collationId}-$constraint"
    case _ => fail(UnsupportedExpression)
  }

  private def isHashStableType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType | DateType | TimestampType | TimestampNTZType => true
    case BinaryType | _: DecimalType => true
    // String hashing has additional collation/runtime compatibility switches. It stays excluded
    // until those switches are modeled by the supported slice rather than guessed here.
    case _ => false
  }

  private def inputOrdinals(output: Seq[Attribute]): Map[ExprId, Int] = {
    val pairs = output.zipWithIndex.map { case (attribute, ordinal) =>
      attribute.exprId -> ordinal
    }
    if (pairs.map(_._1).distinct.size != pairs.size) {
      fail(DeterminismUnproven)
    }
    pairs.toMap
  }

  private def fail(reason: ShuffleRecoveryMissReason): Nothing = throw BuildFailure(reason)
}
