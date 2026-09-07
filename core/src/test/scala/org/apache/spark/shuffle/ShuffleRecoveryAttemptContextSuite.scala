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
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, CyclicBarrier, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryAttemptContextSuite extends SparkFunSuite {

  test("duplicate generation allocation admits exactly one concurrent attempt") {
    val barrier = new CyclicBarrier(2)
    val allocator = new ShuffleRecoveryGenerationAllocator {
      override def allocate(
          recoveryGroup: String,
          authorization: ShuffleRecoveryAuthorizationContext):
          ShuffleRecoveryGenerationAllocation = {
        barrier.await(30, TimeUnit.SECONDS)
        ShuffleRecoveryGenerationAllocated(7L)
      }
    }
    val factory = new ShuffleRecoveryAttemptContextFactory("group", allocator)
    val results = new ConcurrentLinkedQueue[
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryAttemptContext]]()
    val first = allocationThread(factory, "attempt-a", results)
    val second = allocationThread(factory, "attempt-b", results)

    first.start()
    second.start()
    first.join(30000L)
    second.join(30000L)
    assert(!first.isAlive && !second.isAlive)

    val observed = Array(results.poll(), results.poll())
    assert(observed.count(_.isRight) == 1)
    assert(observed.count(_ == Left(ShuffleRecoveryGenerationInvalid)) == 1)
  }

  test("malformed missing and terminal generations disable recovery") {
    Seq(0L, -1L, Long.MinValue, Long.MaxValue).foreach { generation =>
      assert(ShuffleRecoveryAttemptContext.create(
        "group",
        generation,
        "attempt",
        authorization(),
        lifecycleCapabilities(),
        retention()) == Left(ShuffleRecoveryGenerationInvalid))
    }

    val unavailable = new ShuffleRecoveryGenerationAllocator {
      override def allocate(
          recoveryGroup: String,
          authorization: ShuffleRecoveryAuthorizationContext):
          ShuffleRecoveryGenerationAllocation = ShuffleRecoveryGenerationAllocationUnavailable
    }
    val factory = new ShuffleRecoveryAttemptContextFactory("group", unavailable)
    assert(factory.allocate(
      "attempt",
      authorization(),
      lifecycleCapabilities(),
      retention()) == Left(ShuffleRecoveryContextUnavailable))
  }

  test("a current generation can discover only a strictly earlier unexpired generation") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(2L, authority)
    val identity = feasibilityIdentity()
    val lookup = new RecordingLookup(manifest(1L, 900L, identity))

    assert(lifecycle.findCompatible(identity, lookup, 1000L).isRight)
    assert(lookup.lastGeneration.get() == 2L)

    lookup.result.set(Some(manifest(2L, 900L, identity)))
    assert(lifecycle.findCompatible(identity, lookup, 1000L) ==
      Left(ShuffleRecoveryGenerationInvalid))

    lookup.result.set(Some(manifest(3L, 900L, identity)))
    assert(lifecycle.findCompatible(identity, lookup, 1000L) ==
      Left(ShuffleRecoveryGenerationInvalid))
  }

  test("group knowledge cannot bypass principal authorization before discovery IO") {
    val authority = new TestAuthority("principal-a")
    val lifecycle = attemptLifecycle(2L, authority, authorization("principal-b"))
    val lookup = new RecordingLookup(manifest(1L, 900L, feasibilityIdentity()))

    assert(lifecycle.findCompatible(feasibilityIdentity(), lookup, 1000L) ==
      Left(ShuffleRecoveryAuthorizationRejected))
    assert(lookup.calls.get() == 0)
  }

  test("authorization revoked between discovery and provider claim prevents adoption") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(2L, authority)
    val candidate = manifest(1L, 900L, feasibilityIdentity())
    val lookup = new RecordingLookup(candidate)
    assert(lifecycle.findCompatible(candidate.identity, lookup, 1000L).isRight)

    val delegateCalls = new AtomicInteger(0)
    val delegate = new ShuffleRecoveryClaimProvider {
      override def compatibilityId: String = candidate.identity.providerCompatibilityId
      override def claim(request: ShuffleRecoveryClaimRequest): ShuffleRecoveryClaimResult = {
        delegateCalls.incrementAndGet()
        ShuffleRecoveryClaimMissing
      }
      override def release(binding: ShuffleRecoveryBinding): Unit = {}
    }
    val provider = new ShuffleRecoveryAuthorizingClaimProvider(delegate, authority)
    authority.denyAfterFirstClaimAuthorization.set(true)

    assert(lifecycle.claim(candidate, target(), provider) ==
      Left(ShuffleRecoveryAuthorizationRejected))
    assert(delegateCalls.get() == 0)
  }

  test("authorization revoked after preparation but before install consumes no reservation") {
    val authority = new TestAuthority("principal")
    val reservations = new ShuffleRecoveryReservationManager
    val lifecycle = attemptLifecycle(2L, authority, reservations = reservations)
    val reservation = reservations.reserve(target()).toOption.get
    val committed = new AtomicBoolean(false)

    authority.installAllowed.set(false)
    assert(!lifecycle.consumeAuthorized(reservation) {
      committed.set(true)
      true
    })
    assert(!committed.get())
    assert(!reservations.isCurrent(reservation))
  }

  test("attempt stop while provider claim is blocked releases the late binding") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(2L, authority)
    val candidate = manifest(1L, 900L, feasibilityIdentity())
    val entered = new CountDownLatch(1)
    val continue = new CountDownLatch(1)
    val provider = new TestAuthenticatedProvider(candidate, entered, continue)
    val result = new AtomicReference[
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryClaimed]]()
    val thread = new Thread(() => result.set(lifecycle.claim(candidate, target(), provider)))

    thread.start()
    assert(entered.await(30, TimeUnit.SECONDS))
    lifecycle.stop()
    continue.countDown()
    thread.join(30000L)
    assert(!thread.isAlive)
    assert(result.get() == Left(ShuffleRecoveryContextUnavailable))
    assert(provider.releaseCount.get() == 1)
  }

  test("attempt stop releases local bindings without finishing durable group state") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(2L, authority)
    val candidate = manifest(1L, 900L, feasibilityIdentity())
    val provider = new TestAuthenticatedProvider(candidate)
    val finisher = new TestGroupFinisher("lineage")

    assert(lifecycle.claim(candidate, target(), provider).isRight)
    lifecycle.stop()
    lifecycle.stop()
    assert(provider.releaseCount.get() == 1)
    assert(finisher.calls.get() == 0)
  }

  test("stopping one attempt does not revoke another immutable-artifact reader") {
    val candidate = manifest(1L, 900L, feasibilityIdentity())
    val provider = new TestAuthenticatedProvider(candidate)
    val first = attemptLifecycle(2L, new TestAuthority("principal"))
    val second = attemptLifecycle(3L, new TestAuthority("principal"))

    assert(first.claim(candidate, target(42), provider).isRight)
    assert(second.claim(candidate, target(43), provider).isRight)
    first.stop()
    assert(provider.releaseCount.get() == 1)
    assert(second.consumeAuthorized(
      new ShuffleRecoveryReservationManager().reserve(target(43)).toOption.get) { true } == false)
    second.stop()
    assert(provider.releaseCount.get() == 2)
  }

  test("group completion is explicit authorized idempotent and independent of attempt stop") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(4L, authority)
    val finisher = new TestGroupFinisher("lineage-new")
    val stale = groupCompletion("lineage-old", 4L, "completion-old")
    val current = groupCompletion("lineage-new", 4L, "completion-new")

    lifecycle.stop()
    assert(lifecycle.finishGroup(stale, finisher) == ShuffleRecoveryGroupFinishRejected)
    assert(lifecycle.finishGroup(current, finisher) == ShuffleRecoveryGroupFinished)
    assert(lifecycle.finishGroup(current, finisher) == ShuffleRecoveryGroupAlreadyFinished)
    assert(finisher.finished.get() == 1)

    val newerGeneration = groupCompletion("lineage-new", 5L, "completion-future")
    assert(lifecycle.finishGroup(newerGeneration, finisher) ==
      ShuffleRecoveryGroupFinishRejected)
    assert(finisher.finished.get() == 1)
  }

  test("retention expiry is a safe miss and does not contact the claim provider") {
    val authority = new TestAuthority("principal")
    val lifecycle = attemptLifecycle(2L, authority, retentionPolicy = retention(100L))
    val candidate = manifest(1L, 1000L, feasibilityIdentity())
    val lookup = new RecordingLookup(candidate)
    val provider = new TestAuthenticatedProvider(candidate)

    assert(lifecycle.findCompatible(candidate.identity, lookup, 1100L) ==
      Left(ShuffleRecoveryLifecycleExpired))
    assert(provider.claimCount.get() == 0)
  }

  test("diagnostics and structured context rendering never expose authorization references") {
    val auth = ShuffleRecoveryAuthorizationContext(
      "principalSecret123",
      "workloadSecret456",
      "policySecret789",
      Some("dataViewSecret321"))
    val context = ShuffleRecoveryAttemptContext.create(
      "groupSecret111",
      2L,
      "attemptSecret222",
      auth,
      lifecycleCapabilities(),
      retention()).toOption.get
    val rendered = context.toString

    Seq(
      "principalSecret123",
      "workloadSecret456",
      "policySecret789",
      "dataViewSecret321",
      "groupSecret111",
      "attemptSecret222").foreach { secret =>
      assert(!rendered.contains(secret))
    }
    val encodedManifest = ShuffleRecoveryManifestCodec.encode(
      manifest(1L, 900L, feasibilityIdentity()))
    val encodedText = new String(encodedManifest, StandardCharsets.ISO_8859_1)
    Seq("principalSecret123", "policySecret789", "dataViewSecret321").foreach { secret =>
      assert(!encodedText.contains(secret))
    }
  }

  test("generation allocation never performs external work on the DAGScheduler event loop") {
    val allocatorCalls = new AtomicInteger(0)
    val allocator = new ShuffleRecoveryGenerationAllocator {
      override def allocate(
          recoveryGroup: String,
          authorization: ShuffleRecoveryAuthorizationContext):
          ShuffleRecoveryGenerationAllocation = {
        allocatorCalls.incrementAndGet()
        ShuffleRecoveryGenerationAllocated(1L)
      }
    }
    val factory = new ShuffleRecoveryAttemptContextFactory("group", allocator)
    val result = new AtomicReference[
      Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryAttemptContext]]()
    val thread = new Thread(() => result.set(factory.allocate(
      "attempt",
      authorization(),
      lifecycleCapabilities(),
      retention())))
    thread.setName("dag-scheduler-event-loop")

    thread.start()
    thread.join(30000L)
    assert(!thread.isAlive)
    assert(result.get() == Left(ShuffleRecoveryContextUnavailable))
    assert(allocatorCalls.get() == 0)
  }

  private def allocationThread(
      factory: ShuffleRecoveryAttemptContextFactory,
      attemptId: String,
      results: ConcurrentLinkedQueue[
        Either[ShuffleRecoveryAttemptDiagnostic, ShuffleRecoveryAttemptContext]]): Thread = {
    new Thread(() => results.add(factory.allocate(
      attemptId,
      authorization(),
      lifecycleCapabilities(),
      retention())))
  }

  private def authorization(
      principal: String = "principal"): ShuffleRecoveryAuthorizationContext = {
    ShuffleRecoveryAuthorizationContext(
      principal,
      "workload",
      "policy",
      Some("data-view"))
  }

  private def lifecycleCapabilities(): ShuffleRecoveryLifecycleCapabilities = {
    ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = true)
  }

  private def retention(ttlMillis: Long = 1000L): ShuffleRecoveryRetentionPolicy = {
    ShuffleRecoveryRetentionPolicy(ttlMillis, cleanupDeadlineMillis = 500L)
  }

  private def attemptContext(
      generation: Long,
      auth: ShuffleRecoveryAuthorizationContext = authorization(),
      retentionPolicy: ShuffleRecoveryRetentionPolicy = retention()):
      ShuffleRecoveryAttemptContext = {
    ShuffleRecoveryAttemptContext.create(
      "group",
      generation,
      s"attempt-$generation",
      auth,
      lifecycleCapabilities(),
      retentionPolicy).toOption.get
  }

  private def attemptLifecycle(
      generation: Long,
      authority: TestAuthority,
      auth: ShuffleRecoveryAuthorizationContext = authorization(),
      reservations: ShuffleRecoveryReservationManager = new ShuffleRecoveryReservationManager,
      retentionPolicy: ShuffleRecoveryRetentionPolicy = retention()):
      ShuffleRecoveryAttemptLifecycle = {
    new ShuffleRecoveryAttemptLifecycle(
      attemptContext(generation, auth, retentionPolicy),
      reservations,
      authority)
  }

  private def feasibilityIdentity(): ShuffleRecoveryFeasibilityIdentity = {
    ShuffleRecoveryFeasibilityIdentity.create(
      "source",
      "producer",
      "row-encoding",
      ShuffleRecoveryFeasibilityIdentity.SinglePartitioning,
      mapperCount = 0,
      reducerCount = 1,
      "literal")
  }

  private def manifest(
      generation: Long,
      publicationTimestampMillis: Long,
      identity: ShuffleRecoveryFeasibilityIdentity): ShuffleRecoveryManifest = {
    ShuffleRecoveryManifest(
      "group",
      generation,
      s"artifact-$generation",
      identity,
      mapperCount = 0,
      reducerCount = 1,
      Vector.empty,
      ShuffleRecoveryManifest.DescriptorVersion,
      Some(Vector(0L)),
      publicationTimestampMillis)
  }

  private def target(shuffleId: Int = 42): ShuffleRecoveryAdoptionTarget = {
    ShuffleRecoveryAdoptionTarget(
      ShuffleRecoveryMaterializationId(shuffleId.toLong, 1L),
      shuffleId,
      dependencyIdentity = 7L,
      mapperCount = 0,
      reducerCount = 1)
  }

  private def groupCompletion(
      lineageIncarnationId: String,
      terminalGeneration: Long,
      completionId: String): ShuffleRecoveryGroupCompletion = {
    ShuffleRecoveryGroupCompletion(
      "group",
      lineageIncarnationId,
      terminalGeneration,
      completionId,
      authorization())
  }

  private final class RecordingLookup(initial: ShuffleRecoveryManifest)
    extends ShuffleRecoveryCandidateLookup {
    val calls = new AtomicInteger(0)
    val lastGeneration = new AtomicReference[Long]()
    val result = new AtomicReference[Option[ShuffleRecoveryManifest]](Some(initial))

    override def findCompatible(
        recoveryGroup: String,
        identity: ShuffleRecoveryFeasibilityIdentity,
        currentGeneration: Long): Option[ShuffleRecoveryManifest] = {
      calls.incrementAndGet()
      lastGeneration.set(currentGeneration)
      result.get()
    }
  }

  private final class TestAuthority(expectedPrincipal: String)
    extends ShuffleRecoveryAuthorizationAuthority {
    val installAllowed = new AtomicBoolean(true)
    val denyAfterFirstClaimAuthorization = new AtomicBoolean(false)
    private val claimAuthorizations = new AtomicInteger(0)

    override def authorize(
        context: ShuffleRecoveryAttemptContext,
        action: ShuffleRecoveryAuthorizationAction): ShuffleRecoveryAuthorizationResult = {
      if (context.authorization.principalRef != expectedPrincipal) {
        ShuffleRecoveryAuthorizationDenied
      } else if (action == ShuffleRecoveryInstall && !installAllowed.get()) {
        ShuffleRecoveryAuthorizationDenied
      } else if (action == ShuffleRecoveryClaim && denyAfterFirstClaimAuthorization.get() &&
          claimAuthorizations.incrementAndGet() > 1) {
        ShuffleRecoveryAuthorizationDenied
      } else {
        if (action == ShuffleRecoveryClaim && !denyAfterFirstClaimAuthorization.get()) {
          claimAuthorizations.incrementAndGet()
        }
        ShuffleRecoveryAuthorized(1L)
      }
    }
  }

  private final class TestAuthenticatedProvider(
      candidate: ShuffleRecoveryManifest,
      entered: CountDownLatch = null,
      continue: CountDownLatch = null) extends ShuffleRecoveryAuthenticatedClaimProvider {
    val claimCount = new AtomicInteger(0)
    val releaseCount = new AtomicInteger(0)

    override def compatibilityId: String = candidate.identity.providerCompatibilityId

    override def claim(
        request: ShuffleRecoveryAuthenticatedClaimRequest): ShuffleRecoveryClaimResult = {
      val invocation = claimCount.incrementAndGet()
      if (entered != null) {
        entered.countDown()
      }
      if (continue != null) {
        assert(continue.await(30, TimeUnit.SECONDS))
      }
      val binding = ShuffleRecoveryBinding(
        s"binding-$invocation-${request.request.targetShuffleId}",
        request.request.targetShuffleId,
        candidate.recoveryGroup,
        candidate.generation,
        candidate.incarnationId)
      val descriptor = ShuffleRecoveryClaimDescriptor(
        candidate.recoveryGroup,
        candidate.generation,
        candidate.incarnationId,
        candidate.identity.providerCompatibilityId,
        request.request.targetShuffleId,
        ShuffleRecoveryManifest.DescriptorVersion,
        Array.empty[ShuffleRecoveryClaimedMapDescriptor])
      ShuffleRecoveryClaimed(binding, descriptor)
    }

    override def release(binding: ShuffleRecoveryBinding): Unit = {
      releaseCount.incrementAndGet()
    }
  }

  private final class TestGroupFinisher(expectedLineageIncarnation: String)
    extends ShuffleRecoveryGroupFinisher {
    val calls = new AtomicInteger(0)
    val finished = new AtomicInteger(0)
    private val completionId = new AtomicReference[String]()

    override def finishGroup(
        completion: ShuffleRecoveryGroupCompletion): ShuffleRecoveryGroupFinishResult = {
      calls.incrementAndGet()
      if (completion.lineageIncarnationId != expectedLineageIncarnation) {
        ShuffleRecoveryGroupFinishRejected
      } else if (completionId.compareAndSet(null, completion.completionId)) {
        finished.incrementAndGet()
        ShuffleRecoveryGroupFinished
      } else if (completionId.get() == completion.completionId) {
        ShuffleRecoveryGroupAlreadyFinished
      } else {
        ShuffleRecoveryGroupFinishRejected
      }
    }
  }
}
