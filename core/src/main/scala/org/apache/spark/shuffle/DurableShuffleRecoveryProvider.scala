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

package org.apache.spark.shuffle

import org.apache.spark.network.buffer.ManagedBuffer

/**
 * Private capability description for durable shuffle recovery.
 *
 * This contract is intentionally narrower than a stable provider API. Spark owns semantic
 * identity, scheduler policy, and validation; implementations only certify and expose immutable
 * shuffle artifacts. The descriptor is copied and validated before it can influence recovery.
 */
private[spark] final case class DurableShuffleRecoveryCapabilityDescriptor(
    providerCapabilityId: String,
    contractVersion: Int,
    artifactFormatVersion: Int,
    readVersion: Int,
    exactFetchRepresentations: Array[String],
    exactReducerAggregateStatistics: Boolean,
    mapperLocalBlockMetadataQueryable: Boolean,
    integrityCapability: String,
    immutableIncarnations: Boolean,
    conditionalRetirement: Boolean,
    retirementRequiresRevision: Boolean,
    lifecycleSemantics: String,
    authorizationRequirement: String,
    maxMappers: Int,
    maxReducers: Int,
    maxMetadataBytes: Long)

/** Spark-owned immutable snapshot of a validated provider capability descriptor. */
private[spark] final case class DurableShuffleRecoveryCapabilities(
    providerCapabilityId: String,
    contractVersion: Int,
    artifactFormatVersion: Int,
    readVersion: Int,
    exactFetchRepresentations: Vector[String],
    exactReducerAggregateStatistics: Boolean,
    mapperLocalBlockMetadataQueryable: Boolean,
    integrityCapability: String,
    immutableIncarnations: Boolean,
    conditionalRetirement: Boolean,
    retirementRequiresRevision: Boolean,
    lifecycleSemantics: String,
    authorizationRequirement: String,
    maxMappers: Int,
    maxReducers: Int,
    maxMetadataBytes: Long)

private[spark] object DurableShuffleRecoveryContract {
  val ContractVersion = 1
  val ArtifactFormatVersion = 1
  val ReadVersion = 1

  val ExactReducerRangeFetch = "exact-reducer-range-v1"
  val ExactIndexSha256AndReducerChecksum = "sha256-index+optional-reducer-checksum-v1"
  val AttemptBindingIndependentArtifacts =
    "attempt-binding-independent-artifacts+provider-retention-v1"
  val CurrentAttemptAuthorization = "current-attempt-authorization-v1"

  val MaxCapabilities = 8
  val MaxCapabilityStringBytes = 128
  val MaxMappers = 65536
  val MaxReducers = 1000000
  val MaxMetadataBytes = 32L * 1024L * 1024L

  private val KnownFetchRepresentations = Set(ExactReducerRangeFetch)
  private val KnownIntegrityCapabilities = Set(ExactIndexSha256AndReducerChecksum)
  private val KnownLifecycleSemantics = Set(AttemptBindingIndependentArtifacts)
  private val KnownAuthorizationRequirements = Set(CurrentAttemptAuthorization)

  /**
   * Negotiates one provider descriptor for a specific candidate.
   *
   * Any unknown, malformed, or incompatible stable capability is a recovery miss. Callers must
   * never infer support from a provider id or silently downgrade an exact-fetch requirement.
   */
  def negotiate(
      descriptor: DurableShuffleRecoveryCapabilityDescriptor,
      expectedProviderCapabilityId: String,
      mapperCount: Int,
      reducerCount: Int): Either[String, DurableShuffleRecoveryCapabilities] = {
    try {
      if (descriptor == null || descriptor.exactFetchRepresentations == null) {
        return Left("provider capability descriptor contains a null field")
      }
      validateText(expectedProviderCapabilityId, "expected provider capability id")
      val providerId = validateText(
        descriptor.providerCapabilityId, "provider capability id")
      if (providerId != expectedProviderCapabilityId) {
        return Left("provider capability id does not match the selected candidate")
      }
      if (descriptor.contractVersion != ContractVersion) {
        return Left("provider recovery contract version is unsupported")
      }
      if (descriptor.artifactFormatVersion != ArtifactFormatVersion) {
        return Left("provider durable artifact format is unsupported")
      }
      if (descriptor.readVersion != ReadVersion) {
        return Left("provider durable read version is unsupported")
      }

      val rawFetch = descriptor.exactFetchRepresentations.clone()
      if (rawFetch.isEmpty || rawFetch.length > MaxCapabilities) {
        return Left("provider exact-fetch capability count is invalid")
      }
      val fetch = rawFetch.iterator.map { value =>
        validateText(value, "exact-fetch capability")
      }.toVector
      if (fetch.distinct.size != fetch.size ||
          fetch.exists(value => !KnownFetchRepresentations.contains(value)) ||
          !fetch.contains(ExactReducerRangeFetch)) {
        return Left("provider exact-fetch representation is unsupported")
      }

      val integrity = validateText(
        descriptor.integrityCapability, "integrity capability")
      if (!KnownIntegrityCapabilities.contains(integrity)) {
        return Left("provider integrity capability is unsupported")
      }
      val lifecycle = validateText(
        descriptor.lifecycleSemantics, "lifecycle semantics")
      if (!KnownLifecycleSemantics.contains(lifecycle)) {
        return Left("provider lifecycle semantics are unsupported")
      }
      val authorization = validateText(
        descriptor.authorizationRequirement, "authorization requirement")
      if (!KnownAuthorizationRequirements.contains(authorization)) {
        return Left("provider authorization requirement is unsupported")
      }

      if (!descriptor.immutableIncarnations) {
        return Left("provider cannot guarantee immutable artifact incarnations")
      }
      if (descriptor.retirementRequiresRevision && !descriptor.conditionalRetirement) {
        return Left("provider retirement revision requirement is inconsistent")
      }
      if (descriptor.maxMappers < 0 || descriptor.maxMappers > MaxMappers ||
          descriptor.maxReducers <= 0 || descriptor.maxReducers > MaxReducers ||
          descriptor.maxMetadataBytes <= 0L ||
          descriptor.maxMetadataBytes > MaxMetadataBytes) {
        return Left("provider capability bounds are invalid")
      }
      if (mapperCount < 0 || mapperCount > descriptor.maxMappers ||
          reducerCount <= 0 || reducerCount > descriptor.maxReducers) {
        return Left("candidate shape exceeds provider capability bounds")
      }

      Right(DurableShuffleRecoveryCapabilities(
        providerId,
        descriptor.contractVersion,
        descriptor.artifactFormatVersion,
        descriptor.readVersion,
        fetch,
        descriptor.exactReducerAggregateStatistics,
        descriptor.mapperLocalBlockMetadataQueryable,
        integrity,
        descriptor.immutableIncarnations,
        descriptor.conditionalRetirement,
        descriptor.retirementRequiresRevision,
        lifecycle,
        authorization,
        descriptor.maxMappers,
        descriptor.maxReducers,
        descriptor.maxMetadataBytes))
    } catch {
      case _: IllegalArgumentException =>
        Left("provider capability descriptor failed bounded validation")
    }
  }

  private def validateText(value: String, field: String): String = {
    if (value == null || value.isEmpty) {
      throw new IllegalArgumentException(s"$field must not be empty")
    }
    if (value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length >
        MaxCapabilityStringBytes) {
      throw new IllegalArgumentException(s"$field is too large")
    }
    value
  }
}

/** Exact Spark-authoritative winner selection presented to a durable provider for certification. */
private[spark] final case class DurableShuffleRecoveryCertificationRequest(
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    winningMapTaskIds: Vector[Long],
    reducerCount: Int)

/** Immutable provider certification of exactly the winner selection supplied by Spark. */
private[spark] final case class DurableShuffleRecoveryCertifiedSelection(
    providerCapabilityId: String,
    contractVersion: Int,
    artifactFormatVersion: Int,
    readVersion: Int,
    winningMapTaskIds: Vector[Long],
    reducerCount: Int,
    mapArtifacts: Vector[ShuffleRecoveryMapArtifact])

/**
 * Authority required before group-scoped durable artifacts can become eligible for destruction.
 *
 * The contract deliberately does not define how authority is obtained. A later lifecycle
 * integration must establish it from trustworthy external state; SparkContext.stop() is not such
 * authority.
 */
private[spark] trait DurableShuffleRecoveryGroupLifecycleAuthority {
  def recoveryGroup: String
}

private[spark] sealed trait DurableShuffleRecoveryGroupFinishResult
private[spark] case object DurableShuffleRecoveryGroupFinished
  extends DurableShuffleRecoveryGroupFinishResult
private[spark] case object DurableShuffleRecoveryGroupAlreadyAbsent
  extends DurableShuffleRecoveryGroupFinishResult
private[spark] case object DurableShuffleRecoveryGroupFinishRefused
  extends DurableShuffleRecoveryGroupFinishResult
private[spark] case object DurableShuffleRecoveryGroupFinishUnavailable
  extends DurableShuffleRecoveryGroupFinishResult

/** Exact immutable provider incarnation named by a destructive retirement request. */
private[spark] final case class DurableShuffleRecoveryRetirementRequest(
    recoveryGroup: String,
    publishingGeneration: Long,
    incarnationId: String,
    examinedRevision: Option[String])

private[spark] sealed trait DurableShuffleRecoveryRetirementResult
private[spark] case object DurableShuffleRecoveryArtifactRetired
  extends DurableShuffleRecoveryRetirementResult
private[spark] case object DurableShuffleRecoveryArtifactAlreadyAbsent
  extends DurableShuffleRecoveryRetirementResult
private[spark] case object DurableShuffleRecoveryArtifactRetirementRefused
  extends DurableShuffleRecoveryRetirementResult
private[spark] case object DurableShuffleRecoveryArtifactRetirementUnavailable
  extends DurableShuffleRecoveryRetirementResult

/** Exact per-reducer physical metadata required by the fetch path. */
private[spark] trait DurableShuffleRecoveryBlockMetadata {
  def offset: Long
  def length: Long

  final def isEmpty: Boolean = length == 0L
}

/**
 * Provider read view for one immutable mapper output.
 *
 * Implementations may answer reducer metadata lazily. Spark does not require an M x R block
 * matrix to be materialized in driver memory.
 */
private[spark] trait DurableShuffleRecoveryResolvedMap {
  def numReducers: Int
  def dataLength: Long
  def indexBytes: Long
  def blockMetadata(reduceId: Int): DurableShuffleRecoveryBlockMetadata
  def getBlockData(reduceId: Int): Option[ManagedBuffer]
}

/**
 * Private durable-shuffle recovery capability.
 *
 * Provider methods never decide semantic equivalence or mutate scheduler state. Methods that may
 * perform external work must enforce [[ShuffleRecoveryExternalCallGuard]] in their implementation.
 */
private[spark] trait DurableShuffleRecoveryProvider {
  def capabilityDescriptor: DurableShuffleRecoveryCapabilityDescriptor

  def certifyWinningSelection(
      request: DurableShuffleRecoveryCertificationRequest):
      DurableShuffleRecoveryCertifiedSelection

  def claim(request: ShuffleRecoveryClaimRequest): ShuffleRecoveryClaimResult

  /** Releases one attempt-local routing alias without deleting durable artifacts. */
  def release(binding: ShuffleRecoveryBinding): Unit

  /** Ends resources owned by this driver attempt; safe to call repeatedly. */
  def finishAttempt(): Unit

  /** Group destruction requires independently established lifecycle authority. */
  def finishGroup(
      authority: DurableShuffleRecoveryGroupLifecycleAuthority):
      DurableShuffleRecoveryGroupFinishResult

  /** Conditionally retires only the exact examined immutable incarnation. */
  def retireExact(
      request: DurableShuffleRecoveryRetirementRequest):
      DurableShuffleRecoveryRetirementResult

  private[shuffle] def openBoundMap(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): DurableShuffleRecoveryResolvedMap

  private[shuffle] def openBoundMapForFetch(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int,
      expected: ShuffleRecoveryPreparedMap): ShuffleRecoveryBoundMapReadResult
}
