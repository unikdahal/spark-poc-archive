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

import java.nio.file.{Files, Path}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

class ShuffleRecoveryAttemptContextSuite extends SparkFunSuite {
  private val AwaitSeconds = 30L
  private val Group = ShuffleRecoveryGroupKey("attempt-context-group", "lineage-a")
  private val Authorization = ShuffleRecoveryAuthorizationContext(
    "principal-a",
    Some("policy-a"),
    Some("view-a"))
  private val NoGroupFinish = ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = false)
  private val CanFinishGroup = ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = true)
  private val NoExpiry = ShuffleRecoveryRetentionPolicy(None)

  private final class BlockingLookup(manifest: ShuffleRecoveryManifest)
    extends ShuffleRecoveryCandidateLookup {
    val entered = new CountDownLatch(1)
    val proceed = new CountDownLatch(1)

    override def findCompatible(
        recoveryGroup: String,
        identity: ShuffleRecoveryFeasibilityIdentity,
        currentGeneration: Long): Option[ShuffleRecoveryManifest] = {
      entered.countDown()
      if (!proceed.await(AwaitSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("blocked lookup test barrier timed out")
      }
      Option(manifest)
    }
  }

  private final class FixedLookup(manifest: Option[ShuffleRecoveryManifest])
    extends ShuffleRecoveryCandidateLookup {
    override def findCompatible(
        recoveryGroup: String,
        identity: ShuffleRecoveryFeasibilityIdentity,
        currentGeneration: Long): Option[ShuffleRecoveryManifest] = manifest
  }

  private final class RecordingGroupBackend extends ShuffleRecoveryGroupLifecycleBackend {
    private val finished = new ConcurrentHashMap[ShuffleRecoveryGroupKey, java.lang.Boolean]()
    val calls = new AtomicInteger(0)

    override def finishGroup(
        groupKey: ShuffleRecoveryGroupKey): ShuffleRecoveryGroupFinishResult = {
      calls.incrementAndGet()
      if (finished.putIfAbsent(groupKey, java.lang.Boolean.TRUE) == null) {
        ShuffleRecoveryGroupFinished
      } else {
        ShuffleRecoveryGroupAlreadyFinished
      }
    }

    def contains(groupKey: ShuffleRecoveryGroupKey): Boolean = finished.containsKey(groupKey)
  }

  private final class DenyingAuthority(secretReason: String)
    extends ShuffleRecoveryAuthorizationAuthority {
    override def authorize(
        request: ShuffleRecoveryAuthorizationRequest): ShuffleRecoveryAuthorizationDecision = {
      ShuffleRecoveryAuthorizationDenied(secretReason)
    }
  }

  test("generation allocation rejects one duplicate generation under deterministic concurrency") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val requestA = ShuffleRecoveryGenerationRequest(Group, "attempt-a", "principal-a")
    val requestB = ShuffleRecoveryGenerationRequest(Group, "attempt-b", "principal-a")
    val ready = new CountDownLatch(2)
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(2)
    val resultA = new AtomicReference[ShuffleRecoveryGenerationResult]()
    val resultB = new AtomicReference[ShuffleRecoveryGenerationResult]()
    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.execute(() => {
        ready.countDown()
        start.await()
        try resultA.set(allocator.reserveAssigned(requestA, 7L))
        finally done.countDown()
      })
      executor.execute(() => {
        ready.countDown()
        start.await()
        try resultB.set(allocator.reserveAssigned(requestB, 7L))
        finally done.countDown()
      })
      assert(ready.await(AwaitSeconds, TimeUnit.SECONDS))
      start.countDown()
      assert(done.await(AwaitSeconds, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }

    assert(Set(resultA.get(), resultB.get()) == Set(
      ShuffleRecoveryGenerationAllocated(7L),
      ShuffleRecoveryGenerationAllocationDuplicate))
  }

  test("missing malformed negative and overflowed generation allocations fail closed") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    Seq(None, Some("not-a-generation"), Some("-1"), Some("9223372036854775808"))
      .zipWithIndex.foreach { case (raw, index) =>
        assertDiagnostic(
          externalContext(allocator, s"attempt-invalid-$index", raw),
          ShuffleRecoveryGenerationInvalid)
      }

    val maxRequest = ShuffleRecoveryGenerationRequest(Group, "attempt-max", "principal-a")
    assert(allocator.reserveAssigned(maxRequest, Long.MaxValue - 1L) ==
      ShuffleRecoveryGenerationAllocated(Long.MaxValue - 1L))
    assert(allocator.allocate(
      ShuffleRecoveryGenerationRequest(Group, "attempt-overflow", "principal-a")) ==
      ShuffleRecoveryGenerationAllocationInvalid)
  }

  test("a current attempt can discover only strictly earlier generations") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-2", 2L)
    val authority = authorizedAuthority(Group)
    val identity = testIdentity()

    assert(new ShuffleRecoveryAuthorizedCandidateLookup(
      new FixedLookup(Some(manifest(identity, 1L))), authority, () => 100L)
      .findCompatible(context, identity).isInstanceOf[ShuffleRecoveryAuthorizedCandidate])

    Seq(2L, 3L).foreach { generation =>
      assert(new ShuffleRecoveryAuthorizedCandidateLookup(
        new FixedLookup(Some(manifest(identity, generation))), authority, () => 100L)
        .findCompatible(context, identity) match {
        case ShuffleRecoveryAuthorizedMiss(diagnostic) =>
          diagnostic.code == ShuffleRecoveryGenerationInvalid
        case _ => false
      })
    }
  }

  test("group knowledge does not authorize a principal mismatch") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(
      allocator,
      "attempt-wrong-principal",
      2L,
      Authorization.copy(principalRef = "principal-b"))
    val authority = authorizedAuthority(Group)
    withTempDir("shuffle-recovery-auth-principal-") { root =>
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root, new SparkConf(false), authority)
      assert(provider.claim(emptyClaim(context, 3)) match {
        case ShuffleRecoveryClaimRejected(reason) =>
          reason == ShuffleRecoveryAuthorizationRejected.name
        case _ => false
      })
    }
  }

  test("authorization revoked between lookup and claim prevents adoption") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-revoked-before-claim", 2L)
    val authority = authorizedAuthority(Group)
    val identity = testIdentity()
    assert(new ShuffleRecoveryAuthorizedCandidateLookup(
      new FixedLookup(Some(manifest(identity, 1L))), authority, () => 100L)
      .findCompatible(context, identity).isInstanceOf[ShuffleRecoveryAuthorizedCandidate])

    authority.revoke(Group)
    withTempDir("shuffle-recovery-auth-revoked-") { root =>
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root, new SparkConf(false), authority)
      assert(provider.claim(emptyClaim(context, 4)) match {
        case ShuffleRecoveryClaimRejected(reason) =>
          reason == ShuffleRecoveryAuthorizationRejected.name
        case _ => false
      })
    }
  }

  test("authorization revoked after claim fences scheduler installation locally") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-revoked-before-install", 2L)
    val authority = authorizedAuthority(Group)
    withTempDir("shuffle-recovery-auth-install-") { root =>
      ReferenceShuffleProvider.open(root, Group.recoveryGroup, 1L, "incarnation-a")
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root, new SparkConf(false), authority)
      val claimed = provider.claim(emptyClaim(context, 5)) match {
        case value: ShuffleRecoveryClaimed => value
        case other => fail(s"provider claim failed unexpectedly: $other")
      }
      assert(provider.isBound(claimed.binding))

      authority.revoke(Group)
      assert(!provider.isBound(claimed.binding))
      val resolver = new ShuffleRecoveryIndexShuffleBlockResolver(
        new SparkConf(false),
        new ConcurrentHashMap[Int, OpenHashSet[Long]]())
      try {
        assert(!resolver.installRecoveredBinding(
          5,
          provider,
          claimed.binding,
          0,
          1,
          1L,
          Vector.empty))
      } finally {
        provider.release(claimed.binding)
        resolver.stop()
      }
    }
  }

  test("stopping an attempt while lookup is blocked fences the late success") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-stop-blocked-lookup", 2L)
    val authority = authorizedAuthority(Group)
    val identity = testIdentity()
    val delegate = new BlockingLookup(manifest(identity, 1L))
    val lookup = new ShuffleRecoveryAuthorizedCandidateLookup(delegate, authority, () => 100L)
    val result = new AtomicReference[ShuffleRecoveryAuthorizedLookupResult]()
    val worker = new Thread(
      () => result.set(lookup.findCompatible(context, identity)),
      "shuffle-recovery-blocked-lookup-test")
    worker.start()

    assert(delegate.entered.await(AwaitSeconds, TimeUnit.SECONDS))
    assert(context.stop())
    delegate.proceed.countDown()
    worker.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
    assert(!worker.isAlive)
    assert(result.get() match {
      case ShuffleRecoveryAuthorizedMiss(diagnostic) =>
        diagnostic.code == ShuffleRecoveryAuthorizationRejected
      case _ => false
    })
  }

  test("attempt stop releases only local binding and preserves durable group artifacts") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-cleanup", 2L)
    val authority = authorizedAuthority(Group)
    withTempDir("shuffle-recovery-attempt-cleanup-") { root =>
      val durable = ReferenceShuffleProvider.open(root, Group.recoveryGroup, 1L, "incarnation-a")
      val incarnationDirectory = durable.attemptsPath.getParent
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root, new SparkConf(false), authority)
      val claimed = provider.claim(emptyClaim(context, 6)) match {
        case value: ShuffleRecoveryClaimed => value
        case other => fail(s"provider claim failed unexpectedly: $other")
      }

      assert(context.stop())
      provider.release(claimed.binding)
      assert(!provider.isBound(claimed.binding))
      assert(Files.isDirectory(incarnationDirectory))
    }
  }

  test("one attempt stopping cannot revoke another reader of the same immutable artifact") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context2 = contextAt(allocator, "attempt-reader-2", 2L)
    val context3 = contextAt(allocator, "attempt-reader-3", 3L)
    val authority = authorizedAuthority(Group)
    withTempDir("shuffle-recovery-concurrent-readers-") { root =>
      ReferenceShuffleProvider.open(root, Group.recoveryGroup, 1L, "incarnation-a")
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root, new SparkConf(false), authority)
      val binding2 = provider.claim(emptyClaim(context2, 7)) match {
        case value: ShuffleRecoveryClaimed => value.binding
        case other => fail(s"generation 2 claim failed: $other")
      }
      val binding3 = provider.claim(emptyClaim(context3, 8)) match {
        case value: ShuffleRecoveryClaimed => value.binding
        case other => fail(s"generation 3 claim failed: $other")
      }
      assert(provider.activeBindingCount == 2)

      assert(context2.stop())
      provider.release(binding2)
      assert(!provider.isBound(binding2))
      assert(provider.isBound(binding3))
      provider.release(binding3)
    }
  }

  test("group finish is explicit authorized idempotent and exact-lineage scoped") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(
      allocator, "attempt-group-finish", 2L, Authorization, CanFinishGroup)
    val authority = authorizedAuthority(Group)
    val backend = new RecordingGroupBackend
    val lifecycle = new ShuffleRecoveryGroupLifecycleManager(authority, backend)

    assert(lifecycle.finishGroup(context) == ShuffleRecoveryGroupFinished)
    assert(lifecycle.finishGroup(context) == ShuffleRecoveryGroupAlreadyFinished)
    assert(backend.calls.get() == 2)
    assert(backend.contains(Group))
    assert(!backend.contains(ShuffleRecoveryGroupKey(Group.recoveryGroup, "lineage-b")))
  }

  test("stale group finish racing newer attempt cleanup cannot cross lineage incarnation") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val oldContext = contextAt(
      allocator, "attempt-old-lineage", 2L, Authorization, CanFinishGroup)
    val newGroup = ShuffleRecoveryGroupKey(Group.recoveryGroup, "lineage-new")
    val newContext = contextAt(
      allocator, "attempt-new-lineage", 2L, Authorization, NoGroupFinish, newGroup)
    val authority = authorizedAuthority(Group)
    val backend = new RecordingGroupBackend
    val lifecycle = new ShuffleRecoveryGroupLifecycleManager(authority, backend)
    val ready = new CountDownLatch(2)
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(2)
    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.execute(() => {
        ready.countDown()
        start.await()
        try lifecycle.finishGroup(oldContext)
        finally done.countDown()
      })
      executor.execute(() => {
        ready.countDown()
        start.await()
        try newContext.stop()
        finally done.countDown()
      })
      assert(ready.await(AwaitSeconds, TimeUnit.SECONDS))
      start.countDown()
      assert(done.await(AwaitSeconds, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }
    assert(backend.contains(Group))
    assert(!backend.contains(newGroup))
  }

  test("retention expiry is a safe discovery miss") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(
      allocator,
      "attempt-expiring",
      2L,
      Authorization,
      NoGroupFinish,
      Group,
      ShuffleRecoveryRetentionPolicy(Some(200L)),
      100L)
    val authority = authorizedAuthority(Group)
    val lookup = new ShuffleRecoveryAuthorizedCandidateLookup(
      new FixedLookup(Some(manifest(testIdentity(), 1L))), authority, () => 200L)
    assert(lookup.findCompatible(context, testIdentity()) match {
      case ShuffleRecoveryAuthorizedMiss(diagnostic) =>
        diagnostic.code == ShuffleRecoveryRetentionExpired
      case _ => false
    })
  }

  test("authorization diagnostics never surface authority-provided secret reasons") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(allocator, "attempt-redaction", 2L)
    val secret = "credential-that-must-not-appear"
    val result = context.authorize(
      new DenyingAuthority(s"provider-secret=$secret"),
      ShuffleRecoveryDiscover)
    assert(result match {
      case Left(diagnostic) =>
        diagnostic.code == ShuffleRecoveryAuthorizationRejected &&
          !diagnostic.toString.contains(secret)
      case _ => false
    })
  }

  test("authorization and group finish external calls are rejected on DAGScheduler thread") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = contextAt(
      allocator, "attempt-scheduler-thread", 2L, Authorization, CanFinishGroup)
    val authority = authorizedAuthority(Group)
    val backend = new RecordingGroupBackend
    assertSchedulerThreadRejected {
      context.authorize(authority, ShuffleRecoveryDiscover)
    }
    assertSchedulerThreadRejected {
      new ShuffleRecoveryGroupLifecycleManager(authority, backend).finishGroup(context)
    }
    assert(backend.calls.get() == 0)
  }

  private def assertSchedulerThreadRejected(body: => Unit): Unit = {
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() => {
      try body catch {
        case NonFatal(error) => failure.set(error)
      }
    }, "dag-scheduler-event-loop")
    thread.start()
    thread.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
    assert(!thread.isAlive)
    assert(failure.get().isInstanceOf[IllegalStateException])
  }

  private def assertDiagnostic(
      result: Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAttemptContext],
      expected: ShuffleRecoveryDiagnosticCode): Unit = {
    assert(result match {
      case Left(diagnostic) => diagnostic.code == expected
      case _ => false
    })
  }

  private def externalContext(
      allocator: ShuffleRecoveryGenerationAllocator,
      attemptInstanceId: String,
      rawGeneration: Option[String]):
      Either[ShuffleRecoveryDiagnostic, ShuffleRecoveryAttemptContext] = {
    ShuffleRecoveryAttemptContext.fromExternalAllocation(
      Group,
      attemptInstanceId,
      Authorization,
      NoGroupFinish,
      NoExpiry,
      allocator,
      rawGeneration,
      100L)
  }

  private def contextAt(
      allocator: ShuffleRecoveryGenerationAllocator,
      attemptInstanceId: String,
      generation: Long,
      authorization: ShuffleRecoveryAuthorizationContext = Authorization,
      lifecycle: ShuffleRecoveryLifecycleCapabilities = NoGroupFinish,
      group: ShuffleRecoveryGroupKey = Group,
      retention: ShuffleRecoveryRetentionPolicy = NoExpiry,
      nowEpochMillis: Long = 100L): ShuffleRecoveryAttemptContext = {
    ShuffleRecoveryAttemptContext.fromExternalAllocation(
      group,
      attemptInstanceId,
      authorization,
      lifecycle,
      retention,
      allocator,
      Some(generation.toString),
      nowEpochMillis) match {
      case Right(context) => context
      case Left(diagnostic) => fail(diagnostic.toString)
    }
  }

  private def authorizedAuthority(
      group: ShuffleRecoveryGroupKey): ReferenceShuffleRecoveryAuthorizationAuthority = {
    val authority = new ReferenceShuffleRecoveryAuthorizationAuthority
    authority.allow(group, Authorization.principalRef, Authorization.dataViewRef)
    authority
  }

  private def emptyClaim(
      context: ShuffleRecoveryAttemptContext,
      targetShuffleId: Int): ShuffleRecoveryClaimRequest = {
    ShuffleRecoveryClaimRequest(
      context,
      1L,
      "incarnation-a",
      ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId,
      targetShuffleId,
      0,
      1,
      Vector.empty)
  }

  private def testIdentity(): ShuffleRecoveryFeasibilityIdentity = {
    ShuffleRecoveryFeasibilityIdentity.create(
      "attempt-context-source",
      "attempt-context-producer",
      "row-v1",
      "hash-v1",
      0,
      1,
      "literal-a")
  }

  private def manifest(
      identity: ShuffleRecoveryFeasibilityIdentity,
      generation: Long): ShuffleRecoveryManifest = {
    ShuffleRecoveryManifest(
      Group.recoveryGroup,
      generation,
      s"incarnation-$generation",
      identity,
      0,
      1,
      Vector.empty,
      ShuffleRecoveryManifest.DescriptorVersion,
      None,
      1L)
  }

  private def withTempDir(prefix: String)(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory(prefix)
    try body(root) finally Utils.deleteRecursively(root.toFile)
  }
}
