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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

/** Stable namespace for one externally managed recovery lineage incarnation. */
private[spark] final case class ShuffleRecoveryGroupKey(
    recoveryGroup: String,
    lineageIncarnationId: String)

/**
 * Redacted authenticated identity carried by recovery operations.
 *
 * This is an identity reference, not a credential. Raw credentials stay in the deployment's
 * authenticated channel and are never persisted in recovery manifests or diagnostic records.
 */
private[spark] final case class ShuffleRecoveryAuthorizationContext(
    principalRef: String,
    policyRef: Option[String],
    dataViewRef: Option[String])

private[spark] final case class ShuffleRecoveryLifecycleCapabilities(
    mayFinishGroup: Boolean)

private[spark] final case class ShuffleRecoveryRetentionPolicy(
    expiresAtEpochMillis: Option[Long]) {

  def isExpired(nowEpochMillis: Long): Boolean =
    expiresAtEpochMillis.exists(nowEpochMillis >= _)
}

private[spark] sealed trait ShuffleRecoveryDiagnosticCode {
  def name: String
}

private[spark] case object ShuffleRecoveryContextUnavailable
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "CONTEXT_UNAVAILABLE"
}
private[spark] case object ShuffleRecoveryGenerationInvalid
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "GENERATION_INVALID"
}
private[spark] case object ShuffleRecoveryGenerationDuplicate
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "GENERATION_DUPLICATE"
}
private[spark] case object ShuffleRecoveryAuthorizationRejected
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "AUTHORIZATION_REJECTED"
}
private[spark] case object ShuffleRecoveryRetentionExpired
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "RETENTION_EXPIRED"
}
private[spark] case object ShuffleRecoveryProviderUnavailable
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "PROVIDER_UNAVAILABLE"
}
private[spark] case object ShuffleRecoverySemanticIdentityMiss
  extends ShuffleRecoveryDiagnosticCode {
  override val name: String = "SEMANTIC_IDENTITY_MISS"
}

/** Bounded, redacted diagnostic suitable for logs and structured prototype evidence. */
private[spark] final case class ShuffleRecoveryDiagnostic(
    code: ShuffleRecoveryDiagnosticCode,
    detail: String) {

  override def toString: String = s"${code.name}:$detail"
}

private[shuffle] final class ShuffleRecoveryAttemptState {
  private val active = new AtomicBoolean(true)

  def isActive: Boolean = active.get()

  def stop(): Boolean = active.compareAndSet(true, false)
}

private[shuffle] final class ShuffleRecoveryAuthorizationFence(initialRevision: Long) {
  require(initialRevision > 0L, "authorization revision must be positive")

  private val revision = new AtomicLong(initialRevision)

  def currentRevision: Long = revision.get()

  /**
   * Advances the local revocation fence. Revision exhaustion permanently disables the fence.
   *
   * Returning zero on exhaustion is deliberate: zero can never back a valid grant, and replacing
   * `Long.MaxValue` with zero ensures a grant issued at the maximum revision becomes stale rather
   * than accidentally surviving a policy change.
   */
  def advance(): Long = {
    while (true) {
      val current = revision.get()
      if (current <= 0L) {
        return 0L
      } else if (current == Long.MaxValue) {
        if (revision.compareAndSet(current, 0L)) {
          return 0L
        }
      } else {
        val updated = current + 1L
        if (revision.compareAndSet(current, updated)) {
          return updated
        }
      }
    }
    0L
  }
}

/**
 * Local authorization proof returned after a current-use authorization check.
 *
 * Scheduler installation reads only the local attempt and policy fences. External policy lookup is
 * completed before this value exists, so revalidation never puts provider or policy I/O on the
 * DAGScheduler event loop. A deployment that supports live revocation updates the policy fence via
 * its authenticated control channel.
 */
private[spark] final class ShuffleRecoveryAuthorizationGrant private[shuffle] (
    val operation: ShuffleRecoveryAuthorizationOperation,
    val policyRevision: Long,
    private val attemptState: ShuffleRecoveryAttemptState,
    private val policyFence: ShuffleRecoveryAuthorizationFence) {

  def isCurrent: Boolean =
    policyRevision > 0L && attemptState.isActive && policyFence.currentRevision == policyRevision

  override def toString: String =
    s"ShuffleRecoveryAuthorizationGrant(${operation.name},revision=$policyRevision,current=$isCurrent)"
}

private[spark] sealed trait ShuffleRecoveryAuthorizationOperation {
  def name: String
}
private[spark] case object ShuffleRecoveryDiscover
  extends ShuffleRecoveryAuthorizationOperation {
  override val name: String = "discover"
}
private[spark] case object ShuffleRecoveryClaim
  extends ShuffleRecoveryAuthorizationOperation {
  override val name: String = "claim"
}
private[spark] case object ShuffleRecoveryPublish
  extends ShuffleRecoveryAuthorizationOperation {
  override val name: String = "publish"
}
private[spark] case object ShuffleRecoveryFinishGroup
  extends ShuffleRecoveryAuthorizationOperation {
  override val name: String = "finish-group"
}

private[spark] final case class ShuffleRecoveryAuthorizationRequest(
    groupKey: ShuffleRecoveryGroupKey,
    attemptInstanceId: String,
    authorization: ShuffleRecoveryAuthorizationContext,
    operation: ShuffleRecoveryAuthorizationOperation,
    artifactGeneration: Option[Long],
    artifactIncarnationId: Option[String])

private[shuffle] sealed trait ShuffleRecoveryAuthorizationDecision
private[shuffle] final case class ShuffleRecoveryAuthorizationAllowed(
    policyRevision: Long,
    fence: ShuffleRecoveryAuthorizationFence)
  extends ShuffleRecoveryAuthorizationDecision
private[shuffle] final case class ShuffleRecoveryAuthorizationDenied(reason: String)
  extends ShuffleRecoveryAuthorizationDecision
private[shuffle] case object ShuffleRecoveryAuthorizationUnavailable
  extends ShuffleRecoveryAuthorizationDecision

/** External/current-policy authorization boundary. Calls may block and must stay off the scheduler. */
private[spark] trait ShuffleRecoveryAuthorizationAuthority {
  def authorize(
      request: ShuffleRecoveryAuthorizationRequest): ShuffleRecoveryAuthorizationDecision
}

/**
 * Immutable Spark-owned recovery context for one driver attempt.
 *
 * Generation establishes ordering only. Authorization is checked independently for every external
 * discovery/claim/group-finish use and produces a local revocation fence for scheduler install.
 */
private[spark] final class ShuffleRecoveryAttemptContext private[shuffle] (
    val groupKey: ShuffleRecoveryGroupKey,
    val generation: Long,
    val attemptInstanceId: String,
    val authorization: ShuffleRecoveryAuthorizationContext,
    val lifecycleCapabilities: ShuffleRecoveryLifecycleCapabilities,
    val retentionPolicy: ShuffleRecoveryRetentionPolicy,
    private val state: ShuffleRecoveryAttemptState) {

  require(generation > 0L, "generation must be positive")

  def recoveryGroup: String = groupKey.recoveryGroup

  def isActive: Boolean = state.isActive

  def stop(): Boolean = state.stop()

  def retentionExpired(nowEpochMillis: Long): Boolean =
    retentionPolicy.isExpired(nowEpochMillis)

  def authorize(
      authority: ShuffleRecoveryAuthorizationAuthority,
      operation: ShuffleRecoveryAuthorizationOperation,
      artifactGeneration: Option[Long] = None,
      artifactIncarnationId: Option[String] = None):
      Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAuthorizationGrant] = {
    if (!isActive) {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "attempt-stopped"))
    }
    if (authority == null || operation == null) {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "authorization-context-unavailable"))
    }
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery authorization")
    val request = ShuffleRecoveryAuthorizationRequest(
      groupKey,
      attemptInstanceId,
      authorization,
      operation,
      artifactGeneration,
      artifactIncarnationId)
    val decision = try {
      authority.authorize(request)
    } catch {
      case NonFatal(_) => ShuffleRecoveryAuthorizationUnavailable
    }
    decision match {
      case ShuffleRecoveryAuthorizationAllowed(revision, fence)
          if revision > 0L && fence != null && fence.currentRevision == revision && isActive =>
        Right(new ShuffleRecoveryAuthorizationGrant(operation, revision, state, fence))
      case _: ShuffleRecoveryAuthorizationAllowed =>
        Left(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryAuthorizationRejected,
          "stale-policy-grant"))
      case ShuffleRecoveryAuthorizationDenied(_) =>
        Left(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryAuthorizationRejected,
          "policy-denied"))
      case ShuffleRecoveryAuthorizationUnavailable =>
        Left(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryProviderUnavailable,
          "authorization-unavailable"))
    }
  }

  /** Never renders lineage, attempt, principal, policy, or data-view identifiers. */
  override def toString: String = {
    val policy = if (authorization.policyRef.isDefined) "present" else "none"
    val dataView = if (authorization.dataViewRef.isDefined) "present" else "none"
    s"ShuffleRecoveryAttemptContext(group=redacted,lineage=redacted,generation=$generation," +
      s"attempt=redacted,principal=redacted,policy=$policy,dataView=$dataView,active=$isActive)"
  }
}

private[spark] final case class ShuffleRecoveryGenerationRequest(
    groupKey: ShuffleRecoveryGroupKey,
    attemptInstanceId: String,
    principalRef: String)

private[spark] sealed trait ShuffleRecoveryGenerationResult
private[spark] final case class ShuffleRecoveryGenerationAllocated(generation: Long)
  extends ShuffleRecoveryGenerationResult
private[spark] case object ShuffleRecoveryGenerationAllocationDuplicate
  extends ShuffleRecoveryGenerationResult
private[spark] case object ShuffleRecoveryGenerationAllocationInvalid
  extends ShuffleRecoveryGenerationResult
private[spark] case object ShuffleRecoveryGenerationAllocationUnavailable
  extends ShuffleRecoveryGenerationResult

/**
 * Narrow orchestrator contract for assigning unique monotonic generations.
 *
 * This is not a lease service. Spark consumes one allocation result and does not renew it.
 */
private[spark] trait ShuffleRecoveryGenerationAllocator {
  def allocate(request: ShuffleRecoveryGenerationRequest): ShuffleRecoveryGenerationResult

  def reserveAssigned(
      request: ShuffleRecoveryGenerationRequest,
      generation: Long): ShuffleRecoveryGenerationResult
}

/**
 * Deterministic bounded in-process allocator used by focused contract tests.
 *
 * Real deployments obtain generations from an external authority. This reference implementation
 * deliberately refuses new recovery allocations when its bounded bookkeeping is full rather than
 * evicting idempotency state and risking generation reuse.
 */
private[spark] final class ReferenceShuffleRecoveryGenerationAllocator(
    maxGroups: Int = 1024,
    maxAttemptsPerGroup: Int = 1024) extends ShuffleRecoveryGenerationAllocator {

  require(maxGroups > 0, "maximum recovery groups must be positive")
  require(maxAttemptsPerGroup > 0, "maximum attempts per recovery group must be positive")

  private final case class GroupState(
      var highestGeneration: Long,
      attempts: mutable.HashMap[String, Long])

  private val groups = mutable.HashMap.empty[ShuffleRecoveryGroupKey, GroupState]

  override def allocate(
      request: ShuffleRecoveryGenerationRequest): ShuffleRecoveryGenerationResult = synchronized {
    validateRequest(request) match {
      case Some(_) => ShuffleRecoveryGenerationAllocationInvalid
      case None =>
        groupState(request.groupKey) match {
          case Left(result) => result
          case Right(group) =>
            group.attempts.get(request.attemptInstanceId) match {
              case Some(existing) => ShuffleRecoveryGenerationAllocated(existing)
              case None if group.attempts.size >= maxAttemptsPerGroup =>
                ShuffleRecoveryGenerationAllocationUnavailable
              case None if group.highestGeneration == Long.MaxValue =>
                ShuffleRecoveryGenerationAllocationInvalid
              case None =>
                val next = group.highestGeneration + 1L
                group.highestGeneration = next
                group.attempts.put(request.attemptInstanceId, next)
                ShuffleRecoveryGenerationAllocated(next)
            }
        }
    }
  }

  override def reserveAssigned(
      request: ShuffleRecoveryGenerationRequest,
      generation: Long): ShuffleRecoveryGenerationResult = synchronized {
    validateRequest(request) match {
      case Some(_) => ShuffleRecoveryGenerationAllocationInvalid
      case None if generation <= 0L => ShuffleRecoveryGenerationAllocationInvalid
      case None =>
        groupState(request.groupKey) match {
          case Left(result) => result
          case Right(group) =>
            group.attempts.get(request.attemptInstanceId) match {
              case Some(existing) if existing == generation =>
                ShuffleRecoveryGenerationAllocated(existing)
              case Some(_) => ShuffleRecoveryGenerationAllocationDuplicate
              case None if generation <= group.highestGeneration =>
                ShuffleRecoveryGenerationAllocationDuplicate
              case None if group.attempts.size >= maxAttemptsPerGroup =>
                ShuffleRecoveryGenerationAllocationUnavailable
              case None =>
                group.highestGeneration = generation
                group.attempts.put(request.attemptInstanceId, generation)
                ShuffleRecoveryGenerationAllocated(generation)
            }
        }
    }
  }

  private[shuffle] def trackedGroupCount: Int = synchronized(groups.size)

  private[shuffle] def trackedAttemptCount(groupKey: ShuffleRecoveryGroupKey): Int = synchronized {
    groups.get(groupKey).map(_.attempts.size).getOrElse(0)
  }

  private def groupState(
      groupKey: ShuffleRecoveryGroupKey):
      Either[ShuffleRecoveryGenerationResult, GroupState] = {
    groups.get(groupKey) match {
      case Some(group) => Right(group)
      case None if groups.size >= maxGroups => Left(ShuffleRecoveryGenerationAllocationUnavailable)
      case None =>
        val group = GroupState(0L, mutable.HashMap.empty[String, Long])
        groups.put(groupKey, group)
        Right(group)
    }
  }

  private def validateRequest(request: ShuffleRecoveryGenerationRequest): Option[String] = {
    if (request == null || request.groupKey == null) {
      Some("null generation request")
    } else {
      try {
        ShuffleRecoveryAttemptContext.validateIdentifier(
          request.groupKey.recoveryGroup,
          "recovery group")
        ShuffleRecoveryAttemptContext.validateIdentifier(
          request.groupKey.lineageIncarnationId,
          "lineage incarnation")
        ShuffleRecoveryAttemptContext.validateIdentifier(
          request.attemptInstanceId,
          "attempt instance")
        ShuffleRecoveryAttemptContext.validateIdentifier(
          request.principalRef,
          "principal reference")
        None
      } catch {
        case _: IllegalArgumentException => Some("invalid generation request")
      }
    }
  }
}

private[spark] object ShuffleRecoveryAttemptContext {
  private val MaxRetentionMillis = 365L * 24L * 60L * 60L * 1000L

  def allocate(
      groupKey: ShuffleRecoveryGroupKey,
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycleCapabilities: ShuffleRecoveryLifecycleCapabilities,
      retentionPolicy: ShuffleRecoveryRetentionPolicy,
      allocator: ShuffleRecoveryGenerationAllocator,
      nowEpochMillis: Long): Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAttemptContext] = {
    if (allocator == null) {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "generation-allocator-unavailable"))
    }
    validateBase(
      groupKey,
      attemptInstanceId,
      authorization,
      lifecycleCapabilities,
      retentionPolicy,
      nowEpochMillis) match {
      case Some(diagnostic) => Left(diagnostic)
      case None =>
        val request = ShuffleRecoveryGenerationRequest(
          groupKey,
          attemptInstanceId,
          authorization.principalRef)
        val result = try {
          allocator.allocate(request)
        } catch {
          case NonFatal(_) => ShuffleRecoveryGenerationAllocationUnavailable
        }
        fromGenerationResult(
          groupKey,
          attemptInstanceId,
          authorization,
          lifecycleCapabilities,
          retentionPolicy,
          result)
    }
  }

  def fromExternalAllocation(
      groupKey: ShuffleRecoveryGroupKey,
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycleCapabilities: ShuffleRecoveryLifecycleCapabilities,
      retentionPolicy: ShuffleRecoveryRetentionPolicy,
      allocator: ShuffleRecoveryGenerationAllocator,
      rawGeneration: Option[String],
      nowEpochMillis: Long): Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAttemptContext] = {
    if (allocator == null) {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "generation-allocator-unavailable"))
    }
    validateBase(
      groupKey,
      attemptInstanceId,
      authorization,
      lifecycleCapabilities,
      retentionPolicy,
      nowEpochMillis) match {
      case Some(diagnostic) => return Left(diagnostic)
      case None =>
    }
    if (rawGeneration == null) {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryGenerationInvalid,
        "missing-or-malformed-generation"))
    }
    val generation = rawGeneration.flatMap { value =>
      try {
        Some(java.lang.Long.parseLong(value))
      } catch {
        case _: NumberFormatException => None
      }
    }.filter(_ > 0L).getOrElse {
      return Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryGenerationInvalid,
        "missing-or-malformed-generation"))
    }
    val request = ShuffleRecoveryGenerationRequest(
      groupKey,
      attemptInstanceId,
      authorization.principalRef)
    val result = try {
      allocator.reserveAssigned(request, generation)
    } catch {
      case NonFatal(_) => ShuffleRecoveryGenerationAllocationUnavailable
    }
    fromGenerationResult(
      groupKey,
      attemptInstanceId,
      authorization,
      lifecycleCapabilities,
      retentionPolicy,
      result)
  }

  private[shuffle] def legacyFeasibility(
      recoveryGroup: String,
      generation: Long): ShuffleRecoveryAttemptContext = {
    val groupKey = ShuffleRecoveryGroupKey(recoveryGroup, "phase0-feasibility-lineage")
    val authorization = ShuffleRecoveryAuthorizationContext(
      "phase0-feasibility-principal",
      Some("phase0-feasibility-policy"),
      None)
    validateBase(
      groupKey,
      "phase0-feasibility-attempt",
      authorization,
      ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = false),
      ShuffleRecoveryRetentionPolicy(None),
      0L).foreach(diagnostic => throw new IllegalArgumentException(diagnostic.toString))
    if (generation <= 0L) {
      throw new IllegalArgumentException("generation must be positive")
    }
    new ShuffleRecoveryAttemptContext(
      groupKey,
      generation,
      "phase0-feasibility-attempt",
      authorization,
      ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = false),
      ShuffleRecoveryRetentionPolicy(None),
      new ShuffleRecoveryAttemptState)
  }

  private def fromGenerationResult(
      groupKey: ShuffleRecoveryGroupKey,
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycleCapabilities: ShuffleRecoveryLifecycleCapabilities,
      retentionPolicy: ShuffleRecoveryRetentionPolicy,
      result: ShuffleRecoveryGenerationResult):
      Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAttemptContext] = result match {
    case ShuffleRecoveryGenerationAllocated(generation) if generation > 0L =>
      Right(new ShuffleRecoveryAttemptContext(
        groupKey,
        generation,
        attemptInstanceId,
        authorization,
        lifecycleCapabilities,
        retentionPolicy,
        new ShuffleRecoveryAttemptState))
    case ShuffleRecoveryGenerationAllocationDuplicate =>
      Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryGenerationDuplicate,
        "generation-already-assigned"))
    case ShuffleRecoveryGenerationAllocationInvalid | ShuffleRecoveryGenerationAllocated(_) =>
      Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryGenerationInvalid,
        "generation-allocation-invalid"))
    case ShuffleRecoveryGenerationAllocationUnavailable =>
      Left(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "generation-allocation-unavailable"))
  }

  private def validateBase(
      groupKey: ShuffleRecoveryGroupKey,
      attemptInstanceId: String,
      authorization: ShuffleRecoveryAuthorizationContext,
      lifecycleCapabilities: ShuffleRecoveryLifecycleCapabilities,
      retentionPolicy: ShuffleRecoveryRetentionPolicy,
      nowEpochMillis: Long): Option[ShuffleRecoveryDiagnostic] = {
    if (groupKey == null || authorization == null || lifecycleCapabilities == null ||
        retentionPolicy == null || authorization.policyRef == null ||
        authorization.dataViewRef == null || retentionPolicy.expiresAtEpochMillis == null ||
        nowEpochMillis < 0L) {
      return Some(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "invalid-attempt-context"))
    }
    try {
      validateIdentifier(groupKey.recoveryGroup, "recovery group")
      validateIdentifier(groupKey.lineageIncarnationId, "lineage incarnation")
      validateIdentifier(attemptInstanceId, "attempt instance")
      validateIdentifier(authorization.principalRef, "principal reference")
      authorization.policyRef.foreach(validateIdentifier(_, "policy reference"))
      authorization.dataViewRef.foreach(validateIdentifier(_, "data-view reference"))
    } catch {
      case _: IllegalArgumentException =>
        return Some(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryContextUnavailable,
          "invalid-attempt-identifier"))
    }
    retentionPolicy.expiresAtEpochMillis match {
      case Some(expiry) if expiry <= 0L =>
        Some(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryRetentionExpired,
          "invalid-retention-expiry"))
      case Some(expiry) if expiry > nowEpochMillis &&
          expiry - nowEpochMillis > MaxRetentionMillis =>
        Some(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryContextUnavailable,
          "retention-window-out-of-bounds"))
      case Some(expiry) if expiry <= nowEpochMillis =>
        Some(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryRetentionExpired,
          "retention-expired"))
      case _ => None
    }
  }

  private[shuffle] def validateIdentifier(value: String, field: String): Unit = {
    ShuffleRecoveryManifestCodec.validateIdentifier(value, field)
  }
}

/** Deterministic current-policy authority for focused authorization and revocation tests. */
private[spark] final class ReferenceShuffleRecoveryAuthorizationAuthority
  extends ShuffleRecoveryAuthorizationAuthority {

  private final case class Policy(
      principalRef: String,
      dataViewRef: Option[String],
      var allowed: Boolean,
      fence: ShuffleRecoveryAuthorizationFence)

  private val policies = mutable.HashMap.empty[ShuffleRecoveryGroupKey, Policy]

  def allow(
      groupKey: ShuffleRecoveryGroupKey,
      principalRef: String,
      dataViewRef: Option[String]): Unit = synchronized {
    if (groupKey == null || dataViewRef == null) {
      throw new IllegalArgumentException("authorization policy contains a null field")
    }
    ShuffleRecoveryAttemptContext.validateIdentifier(groupKey.recoveryGroup, "recovery group")
    ShuffleRecoveryAttemptContext.validateIdentifier(
      groupKey.lineageIncarnationId, "lineage incarnation")
    ShuffleRecoveryAttemptContext.validateIdentifier(principalRef, "principal reference")
    dataViewRef.foreach(ShuffleRecoveryAttemptContext.validateIdentifier(_, "data-view reference"))
    policies.get(groupKey) match {
      case Some(policy) =>
        val revision = policy.fence.advance()
        policies.put(groupKey, Policy(
          principalRef,
          dataViewRef,
          allowed = revision > 0L,
          policy.fence))
      case None =>
        policies.put(groupKey, Policy(
          principalRef,
          dataViewRef,
          allowed = true,
          new ShuffleRecoveryAuthorizationFence(1L)))
    }
  }

  def revoke(groupKey: ShuffleRecoveryGroupKey): Unit = synchronized {
    if (groupKey != null) {
      policies.get(groupKey).foreach { policy =>
        policy.allowed = false
        policy.fence.advance()
      }
    }
  }

  override def authorize(
      request: ShuffleRecoveryAuthorizationRequest): ShuffleRecoveryAuthorizationDecision =
    synchronized {
      if (request == null || request.groupKey == null || request.authorization == null ||
          request.authorization.dataViewRef == null) {
        ShuffleRecoveryAuthorizationDenied("invalid authorization request")
      } else {
        policies.get(request.groupKey) match {
          case Some(policy)
              if policy.allowed &&
                policy.fence.currentRevision > 0L &&
                policy.principalRef == request.authorization.principalRef &&
                policy.dataViewRef == request.authorization.dataViewRef =>
            ShuffleRecoveryAuthorizationAllowed(
              policy.fence.currentRevision,
              policy.fence)
          case Some(_) =>
            ShuffleRecoveryAuthorizationDenied("current policy rejected the request")
          case None =>
            ShuffleRecoveryAuthorizationDenied("no policy for recovery lineage")
        }
      }
    }
}

private[spark] sealed trait ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupFinished
  extends ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupAlreadyFinished
  extends ShuffleRecoveryGroupFinishResult
private[spark] final case class ShuffleRecoveryGroupFinishRejected(
    diagnostic: ShuffleRecoveryDiagnostic)
  extends ShuffleRecoveryGroupFinishResult
private[spark] case object ShuffleRecoveryGroupFinishUnavailable
  extends ShuffleRecoveryGroupFinishResult

/** Exact lineage-incarnation cleanup boundary implemented by a provider/store integration. */
private[spark] trait ShuffleRecoveryGroupLifecycleBackend {
  def finishGroup(groupKey: ShuffleRecoveryGroupKey): ShuffleRecoveryGroupFinishResult
}

private[spark] final class ShuffleRecoveryGroupLifecycleManager(
    authority: ShuffleRecoveryAuthorizationAuthority,
    backend: ShuffleRecoveryGroupLifecycleBackend) {

  def finishGroup(context: ShuffleRecoveryAttemptContext): ShuffleRecoveryGroupFinishResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery group finish")
    if (context == null || authority == null || backend == null || !context.isActive) {
      return ShuffleRecoveryGroupFinishRejected(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryContextUnavailable,
        "group-finish-context-unavailable"))
    }
    if (!context.lifecycleCapabilities.mayFinishGroup) {
      return ShuffleRecoveryGroupFinishRejected(ShuffleRecoveryDiagnostic(
        ShuffleRecoveryAuthorizationRejected,
        "group-finish-capability-absent"))
    }
    context.authorize(authority, ShuffleRecoveryFinishGroup) match {
      case Left(diagnostic) => ShuffleRecoveryGroupFinishRejected(diagnostic)
      case Right(grant) if grant.isCurrent =>
        try {
          if (grant.isCurrent) backend.finishGroup(context.groupKey)
          else ShuffleRecoveryGroupFinishRejected(ShuffleRecoveryDiagnostic(
            ShuffleRecoveryAuthorizationRejected,
            "group-finish-authorization-stale"))
        } catch {
          case NonFatal(_) => ShuffleRecoveryGroupFinishUnavailable
        }
      case Right(_) =>
        ShuffleRecoveryGroupFinishRejected(ShuffleRecoveryDiagnostic(
          ShuffleRecoveryAuthorizationRejected,
          "group-finish-authorization-stale"))
    }
  }
}
