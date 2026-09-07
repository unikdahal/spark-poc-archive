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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Authenticated identity references for one recovery attempt.
 *
 * These values identify an already-authenticated channel or principal. They are not credentials
 * and must not contain bearer tokens, passwords, private keys, or other secret material.
 */
private[spark] final case class ShuffleRecoveryAuthorizationContext(
    principalRef: String,
    workloadRef: String,
    policyRef: String,
    dataViewRef: Option[String]) {

  override def toString: String = {
    val principal = ShuffleRecoveryAttemptContext.redactedIdentifier(principalRef)
    val workload = ShuffleRecoveryAttemptContext.redactedIdentifier(workloadRef)
    val policy = ShuffleRecoveryAttemptContext.redactedIdentifier(policyRef)
    val dataView = dataViewRef.map(ShuffleRecoveryAttemptContext.redactedIdentifier)
    s"ShuffleRecoveryAuthorizationContext(principal=$principal, workload=$workload, " +
      s"policy=$policy, dataView=$dataView)"
  }
}

/** Bounded retention hints. Expiry is cache loss and never a query-correctness dependency. */
private[spark] final case class ShuffleRecoveryRetentionPolicy(
    ttlMillis: Long,
    cleanupDeadlineMillis: Long)

/** Capabilities granted by the external workload-lineage authority. */
private[spark] final case class ShuffleRecoveryLifecycleCapabilities(
    mayFinishGroup: Boolean)

/**
 * Spark-owned immutable context for one driver attempt in a recovery lineage.
 *
 * Generation establishes ordering only. Authorization is evaluated independently at current use
 * time and knowing either the recovery group or generation never grants access to durable data.
 */
private[spark] final case class ShuffleRecoveryAttemptContext private (
    recoveryGroup: String,
    generation: Long,
    attemptInstanceId: String,
    authorization: ShuffleRecoveryAuthorizationContext,
    lifecycle: ShuffleRecoveryLifecycleCapabilities,
    retention: ShuffleRecoveryRetentionPolicy) {

  override def toString: String = {
    val group = ShuffleRecoveryAttemptContext.redactedIdentifier(recoveryGroup)
    val attempt = ShuffleRecoveryAttemptContext.redactedIdentifier(attemptInstanceId)
    s"ShuffleRecoveryAttemptContext(group=$group, generation=$generation, attempt=$attempt, " +
      s"authorization=$authorization, mayFinishGroup=${lifecycle.mayFinishGroup}, " +
      s"ttlMillis=${retention.ttlMillis}, " +
      s"cleanupDeadlineMillis=${retention.cleanupDeadlineMillis})"
  }
}

private[spark] object ShuffleRecoveryAttemptContext {
  private val MaxRetentionMillis = 365L * 24L * 60L * 60L * 1000L
  private val MaxCleanupDeadlineMillis = 24L * 60L * 60L * 1000L

  def create(
      recoveryGroup: String,
      generation: Long,
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycle: ShuffleRecoveryLifecycleCapabilities,
      retention: ShuffleRecoveryRetentionPolicy):
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryAttemptContext] = {
    try {
      ShuffleRecoveryManifestCodec.validateIdentifier(recoveryGroup, "recovery group")
      ShuffleRecoveryManifestCodec.validateIdentifier(attemptInstanceId, "attempt instance id")
      validateAuthorization(authorization)
      if (lifecycle == null || retention == null) {
        Left(ShuffleRecoveryContextUnavailable)
      } else if (generation <= 0L || generation == Long.MaxValue) {
        Left(ShuffleRecoveryGenerationInvalid)
      } else if (retention.ttlMillis <= 0L ||
          retention.ttlMillis > MaxRetentionMillis ||
          retention.cleanupDeadlineMillis <= 0L ||
          retention.cleanupDeadlineMillis > MaxCleanupDeadlineMillis) {
        Left(ShuffleRecoveryLifecycleExpired)
      } else {
        Right(ShuffleRecoveryAttemptContext(
          recoveryGroup,
          generation,
          attemptInstanceId,
          authorization,
          lifecycle,
          retention))
      }
    } catch {
      case NonFatal(_) => Left(ShuffleRecoveryContextUnavailable)
    }
  }

  private def validateAuthorization(context: ShuffleRecoveryAuthorizationContext): Unit = {
    if (context == null || context.dataViewRef == null) {
      throw new IllegalArgumentException("authorization context must not be null")
    }
    ShuffleRecoveryManifestCodec.validateIdentifier(context.principalRef, "principal reference")
    ShuffleRecoveryManifestCodec.validateIdentifier(context.workloadRef, "workload reference")
    ShuffleRecoveryManifestCodec.validateIdentifier(context.policyRef, "policy reference")
    context.dataViewRef.foreach { value =>
      ShuffleRecoveryManifestCodec.validateIdentifier(value, "data-view reference")
    }
  }

  private[shuffle] def redactedIdentifier(value: String): String = {
    if (value == null) {
      "<absent>"
    } else {
      val digest = MessageDigest.getInstance("SHA-256")
        .digest(value.getBytes(StandardCharsets.UTF_8))
      digest.take(6).iterator.map(byte => f"${byte & 0xff}%02x").mkString
    }
  }
}

private[spark] sealed trait ShuffleRecoveryAttemptDiagnostic {
  def code: String
}
private[spark] case object ShuffleRecoveryContextUnavailable
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "context-unavailable"
}
private[spark] case object ShuffleRecoveryGenerationInvalid
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "generation-invalid"
}
private[spark] case object ShuffleRecoveryAuthorizationRejected
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "authorization-rejected"
}
private[spark] case object ShuffleRecoveryLifecycleExpired
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "lifecycle-expired"
}
private[spark] case object ShuffleRecoveryProviderUnavailable
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "provider-unavailable"
}
private[spark] case object ShuffleRecoverySemanticIdentityMiss
  extends ShuffleRecoveryAttemptDiagnostic {
  override val code: String = "semantic-identity-miss"
}

private[spark] sealed trait ShuffleRecoveryGenerationAllocation
private[spark] final case class ShuffleRecoveryGenerationAllocated(generation: Long)
  extends ShuffleRecoveryGenerationAllocation
private[spark] case object ShuffleRecoveryGenerationAllocationUnavailable
  extends ShuffleRecoveryGenerationAllocation
private[spark] case object ShuffleRecoveryGenerationAllocationRejected
  extends ShuffleRecoveryGenerationAllocation

/**
 * External/orchestrator generation allocation contract.
 *
 * Implementations must allocate one unique, strictly increasing positive value per recovery group.
 * Spark validates returned values but does not provide a distributed lease or consensus service.
 */
private[shuffle] trait ShuffleRecoveryGenerationAllocator {
  def allocate(
      recoveryGroup: String,
      authorization: ShuffleRecoveryAuthorizationContext): ShuffleRecoveryGenerationAllocation
}

/**
 * Per-lineage factory that rejects duplicate or non-monotonic allocations observed in this driver.
 *
 * Cross-driver uniqueness remains the allocator's responsibility. This local fence makes a broken
 * allocator fail closed when concurrent requests in the same process return the same generation.
 */
private[spark] final class ShuffleRecoveryAttemptContextFactory(
    recoveryGroup: String,
    allocator: ShuffleRecoveryGenerationAllocator) {

  ShuffleRecoveryManifestCodec.validateIdentifier(recoveryGroup, "recovery group")
  if (allocator == null) {
    throw new IllegalArgumentException("generation allocator must not be null")
  }

  private var lastAcceptedGeneration = 0L

  def allocate(
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycle: ShuffleRecoveryLifecycleCapabilities,
      retention: ShuffleRecoveryRetentionPolicy):
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryAttemptContext] = {
    val allocation = try {
      ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery generation allocation")
      allocator.allocate(recoveryGroup, authorization)
    } catch {
      case NonFatal(_) => ShuffleRecoveryGenerationAllocationUnavailable
    }

    allocation match {
      case ShuffleRecoveryGenerationAllocated(value) => synchronized {
        if (value <= lastAcceptedGeneration || value <= 0L || value == Long.MaxValue) {
          Left(ShuffleRecoveryGenerationInvalid)
        } else {
          ShuffleRecoveryAttemptContext.create(
            recoveryGroup,
            value,
            attemptInstanceId,
            authorization,
            lifecycle,
            retention) match {
            case right @ Right(_) =>
              lastAcceptedGeneration = value
              right
            case left => left
          }
        }
      }
      case ShuffleRecoveryGenerationAllocationRejected =>
        Left(ShuffleRecoveryGenerationInvalid)
      case ShuffleRecoveryGenerationAllocationUnavailable =>
        Left(ShuffleRecoveryContextUnavailable)
    }
  }
}

private[spark] sealed trait ShuffleRecoveryAuthorizationAction
private[spark] case object ShuffleRecoveryDiscover extends ShuffleRecoveryAuthorizationAction
private[spark] case object ShuffleRecoveryClaim extends ShuffleRecoveryAuthorizationAction
private[spark] case object ShuffleRecoveryInstall extends ShuffleRecoveryAuthorizationAction
private[spark] case object ShuffleRecoveryFinishGroup extends ShuffleRecoveryAuthorizationAction

private[spark] sealed trait ShuffleRecoveryAuthorizationResult
private[spark] final case class ShuffleRecoveryAuthorized(policyRevision: Long)
  extends ShuffleRecoveryAuthorizationResult
private[spark] case object ShuffleRecoveryAuthorizationDenied
  extends ShuffleRecoveryAuthorizationResult
private[spark] case object ShuffleRecoveryAuthorizationUnavailable
  extends ShuffleRecoveryAuthorizationResult

/** Current-use policy authority. Implementations must not infer authorization from a group id. */
private[shuffle] trait ShuffleRecoveryAuthorizationAuthority {
  def authorize(
      context: ShuffleRecoveryAttemptContext,
      action: ShuffleRecoveryAuthorizationAction): ShuffleRecoveryAuthorizationResult
}

private[spark] final case class ShuffleRecoveryAuthenticatedClaimRequest(
    attemptContext: ShuffleRecoveryAttemptContext,
    policyRevision: Long,
    request: ShuffleRecoveryClaimRequest)

/** Provider claim surface that makes current authorization context unavoidable. */
private[shuffle] trait ShuffleRecoveryAuthenticatedClaimProvider {
  def compatibilityId: String
  def claim(request: ShuffleRecoveryAuthenticatedClaimRequest): ShuffleRecoveryClaimResult
  def release(binding: ShuffleRecoveryBinding): Unit
}

/** Performs provider-side current-use authorization immediately before a legacy provider claim. */
private[spark] final class ShuffleRecoveryAuthorizingClaimProvider(
    delegate: ShuffleRecoveryClaimProvider,
    authority: ShuffleRecoveryAuthorizationAuthority)
  extends ShuffleRecoveryAuthenticatedClaimProvider {

  if (delegate == null || authority == null) {
    throw new IllegalArgumentException("claim delegate and authority must not be null")
  }

  override def compatibilityId: String = delegate.compatibilityId

  override def claim(
      request: ShuffleRecoveryAuthenticatedClaimRequest): ShuffleRecoveryClaimResult = {
    if (request == null || request.attemptContext == null || request.request == null ||
        request.policyRevision <= 0L) {
      return ShuffleRecoveryClaimRejected("authenticated claim context is unavailable")
    }
    authorize(request.attemptContext) match {
      case ShuffleRecoveryAuthorized(revision) if revision > 0L =>
        delegate.claim(request.request)
      case _: ShuffleRecoveryAuthorized =>
        ShuffleRecoveryClaimRejected("provider authorization revision is invalid")
      case ShuffleRecoveryAuthorizationDenied =>
        ShuffleRecoveryClaimRejected("provider authorization rejected")
      case ShuffleRecoveryAuthorizationUnavailable =>
        ShuffleRecoveryClaimUnavailable
    }
  }

  override def release(binding: ShuffleRecoveryBinding): Unit = delegate.release(binding)

  private def authorize(
      context: ShuffleRecoveryAttemptContext): ShuffleRecoveryAuthorizationResult = {
    try {
      ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider authorization")
      authority.authorize(context, ShuffleRecoveryClaim)
    } catch {
      case NonFatal(_) => ShuffleRecoveryAuthorizationUnavailable
    }
  }
}

private[spark] final case class ShuffleRecoveryGroupCompletion(
    recoveryGroup: String,
    lineageIncarnationId: String,
    terminalGeneration: Long,
    completionId: String,
    authorization: ShuffleRecoveryAuthorizationContext)

private[spark] sealed trait ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupFinished extends ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupAlreadyFinished extends ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupFinishRejected extends ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupFinishUnavailable extends ShuffleRecoveryGroupFinishResult

/** External durable cleanup surface. It must be idempotent and incarnation-safe. */
private[shuffle] trait ShuffleRecoveryGroupFinisher {
  def finishGroup(completion: ShuffleRecoveryGroupCompletion): ShuffleRecoveryGroupFinishResult
}

/**
 * Attempt-local lifecycle fencing for discovery, claims, reservations, and provider bindings.
 *
 * stop() never invokes group cleanup. Binding release is performed after the lifecycle lock is
 * dropped, so blocking provider cleanup cannot deadlock scheduler-local state.
 */
private[spark] final class ShuffleRecoveryAttemptLifecycle(
    val context: ShuffleRecoveryAttemptContext,
    reservations: ShuffleRecoveryReservationManager,
    authority: ShuffleRecoveryAuthorizationAuthority) {

  if (context == null || reservations == null || authority == null) {
    throw new IllegalArgumentException("attempt lifecycle inputs must not be null")
  }

  private final case class ActiveBinding(
      binding: ShuffleRecoveryBinding,
      provider: ShuffleRecoveryAuthenticatedClaimProvider)

  private val bindings = mutable.LinkedHashMap.empty[String, ActiveBinding]
  private var stopped = false

  def findCompatible(
      identity: ShuffleRecoveryFeasibilityIdentity,
      lookup: ShuffleRecoveryCandidateLookup,
      nowMillis: Long): Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryManifest] = {
    if (identity == null || lookup == null || nowMillis < 0L) {
      return Left(ShuffleRecoveryContextUnavailable)
    }
    authorizeCurrent(ShuffleRecoveryDiscover) match {
      case Left(reason) => Left(reason)
      case Right(_) =>
        val candidate = try {
          ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery candidate discovery")
          lookup.findCompatible(context.recoveryGroup, identity, context.generation)
        } catch {
          case NonFatal(_) => return Left(ShuffleRecoveryProviderUnavailable)
        }
        if (isStopped) {
          Left(ShuffleRecoveryContextUnavailable)
        } else {
          candidate match {
            case None => Left(ShuffleRecoverySemanticIdentityMiss)
            case Some(value) => candidateAllowed(value, nowMillis).map(_ => value)
          }
        }
    }
  }

  def claim(
      candidate: ShuffleRecoveryManifest,
      target: ShuffleRecoveryAdoptionTarget,
      provider: ShuffleRecoveryAuthenticatedClaimProvider):
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryClaimed] = {
    if (!validClaimTarget(candidate, target, provider)) {
      return Left(ShuffleRecoverySemanticIdentityMiss)
    }
    authorizeCurrent(ShuffleRecoveryClaim) match {
      case Left(reason) => Left(reason)
      case Right(policyRevision) =>
        val request = ShuffleRecoveryClaimRequest(
          candidate.recoveryGroup,
          candidate.generation,
          candidate.incarnationId,
          candidate.identity.providerCompatibilityId,
          target.targetShuffleId,
          candidate.mapperCount,
          candidate.reducerCount,
          candidate.mapArtifacts)
        val result = try {
          ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider claim")
          provider.claim(ShuffleRecoveryAuthenticatedClaimRequest(
            context,
            policyRevision,
            request))
        } catch {
          case NonFatal(_) => ShuffleRecoveryClaimUnavailable
        }
        result match {
          case claimed: ShuffleRecoveryClaimed =>
            if (registerBinding(claimed.binding, provider)) {
              Right(claimed)
            } else {
              Left(ShuffleRecoveryContextUnavailable)
            }
          case ShuffleRecoveryClaimUnavailable => Left(ShuffleRecoveryProviderUnavailable)
          case ShuffleRecoveryClaimMissing => Left(ShuffleRecoverySemanticIdentityMiss)
          case ShuffleRecoveryClaimCorrupt => Left(ShuffleRecoverySemanticIdentityMiss)
          case ShuffleRecoveryClaimRejected(reason) if reason != null &&
              reason.toLowerCase(java.util.Locale.ROOT).contains("authorization") =>
            Left(ShuffleRecoveryAuthorizationRejected)
          case _: ShuffleRecoveryClaimRejected => Left(ShuffleRecoverySemanticIdentityMiss)
        }
    }
  }

  /** Revalidates current policy before the reservation can install scheduler-local state. */
  def consumeAuthorized(
      reservation: ShuffleRecoveryAdoptionReservation)(commit: => Boolean): Boolean = {
    authorizeCurrent(ShuffleRecoveryInstall) match {
      case Right(_) => reservations.consumeIfCurrent(reservation)(commit)
      case Left(_) =>
        reservations.abandon(reservation)
        false
    }
  }

  def releaseBinding(binding: ShuffleRecoveryBinding): Unit = {
    val active = synchronized {
      if (binding == null) None else bindings.remove(binding.bindingId)
    }
    active.foreach(value => releaseQuietly(value.provider, value.binding))
  }

  def stop(): Unit = {
    val toRelease = synchronized {
      if (stopped) {
        Vector.empty
      } else {
        stopped = true
        reservations.shutdown()
        val result = bindings.values.toVector
        bindings.clear()
        result
      }
    }
    toRelease.foreach(value => releaseQuietly(value.provider, value.binding))
  }

  /**
   * Explicit group completion remains separate from attempt stop. The external authority may
   * complete the lineage after this attempt has ended, so authorization here does not depend on
   * attempt-local stopped state. stop() never reaches this path.
   */
  def finishGroup(
      completion: ShuffleRecoveryGroupCompletion,
      finisher: ShuffleRecoveryGroupFinisher): ShuffleRecoveryGroupFinishResult = {
    if (!validCompletion(completion) || finisher == null || !context.lifecycle.mayFinishGroup) {
      return ShuffleRecoveryGroupFinishRejected
    }
    authorizeGroupFinish() match {
      case ShuffleRecoveryAuthorizationDenied => ShuffleRecoveryGroupFinishRejected
      case ShuffleRecoveryAuthorizationUnavailable => ShuffleRecoveryGroupFinishUnavailable
      case ShuffleRecoveryAuthorized(revision) if revision <= 0L =>
        ShuffleRecoveryGroupFinishRejected
      case _: ShuffleRecoveryAuthorized =>
        try {
          ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery group completion")
          finisher.finishGroup(completion)
        } catch {
          case NonFatal(_) => ShuffleRecoveryGroupFinishUnavailable
        }
    }
  }

  private def authorizeCurrent(
      action: ShuffleRecoveryAuthorizationAction): Either[ShuffleRecoveryAttemptDiagnostic, Long] = {
    if (isStopped) {
      Left(ShuffleRecoveryContextUnavailable)
    } else {
      val result = try {
        ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery authorization")
        authority.authorize(context, action)
      } catch {
        case NonFatal(_) => ShuffleRecoveryAuthorizationUnavailable
      }
      result match {
        case ShuffleRecoveryAuthorized(revision) if revision > 0L => Right(revision)
        case _: ShuffleRecoveryAuthorized => Left(ShuffleRecoveryAuthorizationRejected)
        case ShuffleRecoveryAuthorizationDenied => Left(ShuffleRecoveryAuthorizationRejected)
        case ShuffleRecoveryAuthorizationUnavailable => Left(ShuffleRecoveryProviderUnavailable)
      }
    }
  }

  private def candidateAllowed(
      candidate: ShuffleRecoveryManifest,
      nowMillis: Long): Either[ShuffleRecoveryAttemptDiagnostic, Unit] = {
    if (candidate == null || candidate.recoveryGroup != context.recoveryGroup) {
      Left(ShuffleRecoverySemanticIdentityMiss)
    } else if (candidate.generation <= 0L || candidate.generation >= context.generation) {
      Left(ShuffleRecoveryGenerationInvalid)
    } else if (candidate.publicationTimestampMillis < 0L ||
        candidate.publicationTimestampMillis > nowMillis) {
      Left(ShuffleRecoverySemanticIdentityMiss)
    } else if (nowMillis - candidate.publicationTimestampMillis >= context.retention.ttlMillis) {
      Left(ShuffleRecoveryLifecycleExpired)
    } else {
      Right(())
    }
  }

  private def validClaimTarget(
      candidate: ShuffleRecoveryManifest,
      target: ShuffleRecoveryAdoptionTarget,
      provider: ShuffleRecoveryAuthenticatedClaimProvider): Boolean = {
    candidate != null && target != null && provider != null &&
      candidate.recoveryGroup == context.recoveryGroup &&
      candidate.generation > 0L && candidate.generation < context.generation &&
      candidate.mapperCount == target.mapperCount &&
      candidate.reducerCount == target.reducerCount &&
      provider.compatibilityId == candidate.identity.providerCompatibilityId
  }

  private def registerBinding(
      binding: ShuffleRecoveryBinding,
      provider: ShuffleRecoveryAuthenticatedClaimProvider): Boolean = {
    val bindingIsValid = binding != null && provider != null &&
      binding.recoveryGroup == context.recoveryGroup &&
      binding.publishingGeneration > 0L && binding.publishingGeneration < context.generation
    val accepted = synchronized {
      if (!bindingIsValid || stopped ||
          bindings.size >= ShuffleRecoveryAttemptLifecycle.MaxBindings ||
          bindings.contains(binding.bindingId)) {
        false
      } else {
        bindings.put(binding.bindingId, ActiveBinding(binding, provider))
        true
      }
    }
    if (!accepted && binding != null && provider != null) {
      releaseQuietly(provider, binding)
    }
    accepted
  }

  private def validCompletion(completion: ShuffleRecoveryGroupCompletion): Boolean = {
    if (completion == null || completion.authorization != context.authorization ||
        completion.recoveryGroup != context.recoveryGroup ||
        completion.terminalGeneration != context.generation) {
      false
    } else {
      try {
        ShuffleRecoveryManifestCodec.validateIdentifier(
          completion.lineageIncarnationId, "lineage incarnation id")
        ShuffleRecoveryManifestCodec.validateIdentifier(completion.completionId, "completion id")
        true
      } catch {
        case NonFatal(_) => false
      }
    }
  }

  private def authorizeGroupFinish(): ShuffleRecoveryAuthorizationResult = {
    try {
      ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery group authorization")
      authority.authorize(context, ShuffleRecoveryFinishGroup)
    } catch {
      case NonFatal(_) => ShuffleRecoveryAuthorizationUnavailable
    }
  }

  private def isStopped: Boolean = synchronized { stopped }

  private def releaseQuietly(
      provider: ShuffleRecoveryAuthenticatedClaimProvider,
      binding: ShuffleRecoveryBinding): Unit = {
    try {
      ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery binding release")
      provider.release(binding)
    } catch {
      case NonFatal(_) =>
    }
  }
}

private[shuffle] object ShuffleRecoveryAttemptLifecycle {
  private val MaxBindings = ShuffleRecoveryManifestCodec.MaxMaps
}
