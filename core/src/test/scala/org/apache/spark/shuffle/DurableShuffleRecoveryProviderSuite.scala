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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.util.Base64
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

class DurableShuffleRecoveryProviderSuite extends SparkFunSuite {
  private val AwaitSeconds = 30L
  private val ProviderId = ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId
  private val Conf = new SparkConf(false)

  private final case class TestAuthority(recoveryGroup: String)
    extends DurableShuffleRecoveryGroupLifecycleAuthority

  test("capability negotiation snapshots mutable input and rejects unknown compatibility") {
    withRoot { root =>
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val raw = provider.capabilityDescriptor
      val negotiated = DurableShuffleRecoveryContract.negotiate(raw, ProviderId, 1, 2)
        .fold(reason => fail(reason), value => value)

      assert(negotiated.contractVersion == DurableShuffleRecoveryContract.ContractVersion)
      assert(negotiated.artifactFormatVersion ==
        DurableShuffleRecoveryContract.ArtifactFormatVersion)
      assert(negotiated.readVersion == DurableShuffleRecoveryContract.ReadVersion)
      assert(negotiated.exactFetchRepresentations ==
        Vector(DurableShuffleRecoveryContract.ExactReducerRangeFetch))
      assert(!negotiated.exactReducerAggregateStatistics)
      assert(negotiated.mapperLocalBlockMetadataQueryable)
      assert(negotiated.immutableIncarnations)
      assert(negotiated.conditionalRetirement)
      assert(!negotiated.retirementRequiresRevision)

      raw.exactFetchRepresentations(0) = "future-fetch-v2"
      assert(negotiated.exactFetchRepresentations ==
        Vector(DurableShuffleRecoveryContract.ExactReducerRangeFetch))
      assert(DurableShuffleRecoveryContract.negotiate(raw, ProviderId, 1, 2).isLeft)

      val fresh = provider.capabilityDescriptor
      assert(fresh.exactFetchRepresentations.sameElements(
        Array(DurableShuffleRecoveryContract.ExactReducerRangeFetch)))
      assert(DurableShuffleRecoveryContract.negotiate(
        fresh.copy(contractVersion = fresh.contractVersion + 1),
        ProviderId,
        1,
        2).isLeft)
      assert(DurableShuffleRecoveryContract.negotiate(
        fresh.copy(providerCapabilityId = "different-provider"),
        ProviderId,
        1,
        2).isLeft)
      assert(DurableShuffleRecoveryContract.negotiate(
        fresh.copy(exactFetchRepresentations = Array(
          DurableShuffleRecoveryContract.ExactReducerRangeFetch,
          "unknown-future-capability")),
        ProviderId,
        1,
        2).isLeft)
      assert(DurableShuffleRecoveryContract.negotiate(
        fresh,
        ProviderId,
        fresh.maxMappers + 1,
        2).isLeft)
    }
  }

  test("certification freezes Spark's exact winner and rejects a late loser substitution") {
    withRoot { root =>
      val group = "winner-certification"
      val generation = 1L
      val incarnation = "incarnation-a"
      val lowLevel = ReferenceShuffleProvider.open(root, group, generation, incarnation, Conf)
      val winner = writeCandidate(
        lowLevel,
        mapTaskId = 11L,
        reducerCount = 2,
        Map(1 -> "winner".getBytes(StandardCharsets.UTF_8)))
      val loser = writeCandidate(
        lowLevel,
        mapTaskId = 12L,
        reducerCount = 2,
        Map(1 -> "loser".getBytes(StandardCharsets.UTF_8)))
      assert(winner.candidateName != loser.candidateName)

      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val request = DurableShuffleRecoveryCertificationRequest(
        group,
        generation,
        incarnation,
        Vector(11L),
        reducerCount = 2)
      val certified = provider.certifyWinningSelection(request)
      assert(certified.providerCapabilityId == ProviderId)
      assert(certified.winningMapTaskIds == Vector(11L))
      assert(certified.mapArtifacts.map(_.mapTaskId) == Vector(11L))
      assert(certified.mapArtifacts.map(_.mapIndex) == Vector(0))

      intercept[IOException] {
        provider.certifyWinningSelection(request.copy(winningMapTaskIds = Vector(12L)))
      }
      assert(lowLevel.openMap(0).dataLength == "winner".getBytes(StandardCharsets.UTF_8).length)
    }
  }

  test("concurrent replacement attempts bind one immutable incarnation to current shuffle ids") {
    withRoot { root =>
      val group = "concurrent-bindings"
      val generation = 1L
      val incarnation = "incarnation-a"
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val certified = certifyMaps(
        root,
        provider,
        group,
        generation,
        incarnation,
        Vector(21L, 22L),
        reducerCount = 2)

      val ready = new CountDownLatch(2)
      val start = new CountDownLatch(1)
      val done = new CountDownLatch(2)
      val results = Array.fill(2)(new AtomicReference[ShuffleRecoveryClaimed]())
      val failures = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
      val executor = Executors.newFixedThreadPool(2)
      try {
        Seq(101, 202).zipWithIndex.foreach { case (targetShuffleId, index) =>
          executor.submit(new Runnable {
            override def run(): Unit = {
              ready.countDown()
              try {
                start.await()
                results(index).set(claim(
                  provider,
                  certified,
                  group,
                  generation,
                  incarnation,
                  targetShuffleId))
              } catch {
                case error: Throwable => failures.add(error)
              } finally {
                done.countDown()
              }
            }
          })
        }
        assert(ready.await(AwaitSeconds, TimeUnit.SECONDS))
        start.countDown()
        assert(done.await(AwaitSeconds, TimeUnit.SECONDS))
      } finally {
        executor.shutdownNow()
      }

      assert(failures.isEmpty, failures.toString)
      val first = results(0).get()
      val second = results(1).get()
      assert(first.binding.targetShuffleId == 101)
      assert(second.binding.targetShuffleId == 202)
      assert(first.binding.bindingId != second.binding.bindingId)
      assert(provider.isBound(first.binding))
      assert(provider.isBound(second.binding))

      val firstMap = provider.openBoundMap(first.binding, 0)
      val secondMap = provider.openBoundMap(second.binding, 0)
      assert(firstMap.dataLength == secondMap.dataLength)
      assert(firstMap.blockMetadata(0).length == secondMap.blockMetadata(0).length)

      provider.release(first.binding)
      provider.release(first.binding)
      assert(!provider.isBound(first.binding))
      assert(provider.isBound(second.binding))
      assert(provider.openBoundMap(second.binding, 1).numReducers == 2)
      provider.release(second.binding)
    }
  }

  test("attempt finish is idempotent and cannot destroy group-scoped artifacts") {
    withRoot { root =>
      val group = "attempt-lifecycle"
      val generation = 1L
      val incarnation = "incarnation-a"
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val certified = certifyMaps(
        root,
        provider,
        group,
        generation,
        incarnation,
        Vector(31L),
        reducerCount = 2)
      val claimed = claim(
        provider,
        certified,
        group,
        generation,
        incarnation,
        targetShuffleId = 301)

      assert(provider.finishGroup(TestAuthority(group)) ==
        DurableShuffleRecoveryGroupFinishRefused)
      provider.finishAttempt()
      provider.finishAttempt()
      assert(!provider.isBound(claimed.binding))

      val replacement = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val replacementClaim = claim(
        replacement,
        certified,
        group,
        generation,
        incarnation,
        targetShuffleId = 302)
      assert(replacement.openBoundMap(replacementClaim.binding, 0).numReducers == 2)
      replacement.release(replacementClaim.binding)

      assert(replacement.finishGroup(TestAuthority(group)) ==
        DurableShuffleRecoveryGroupFinished)
      assert(replacement.finishGroup(TestAuthority(group)) ==
        DurableShuffleRecoveryGroupAlreadyAbsent)
    }
  }

  test("exact retirement of A cannot delete successor B") {
    withRoot { root =>
      val group = "retirement-aba"
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      certifyMaps(
        root,
        provider,
        group,
        generation = 1L,
        incarnation = "incarnation-a",
        Vector(41L),
        reducerCount = 2)
      val successor = certifyMaps(
        root,
        provider,
        group,
        generation = 1L,
        incarnation = "incarnation-b",
        Vector(42L),
        reducerCount = 2)

      assert(provider.retireExact(DurableShuffleRecoveryRetirementRequest(
        group,
        publishingGeneration = 1L,
        incarnationId = "incarnation-a",
        examinedRevision = None)) == DurableShuffleRecoveryArtifactRetired)
      assert(provider.retireExact(DurableShuffleRecoveryRetirementRequest(
        group,
        publishingGeneration = 1L,
        incarnationId = "incarnation-a",
        examinedRevision = None)) == DurableShuffleRecoveryArtifactAlreadyAbsent)

      val successorProvider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val claimedB = claim(
        successorProvider,
        successor,
        group,
        generation = 1L,
        incarnation = "incarnation-b",
        targetShuffleId = 401)
      assert(successorProvider.openBoundMap(claimedB.binding, 0).numReducers == 2)
      successorProvider.release(claimedB.binding)
    }
  }

  test("provider TTL expires an abandoned group without a Spark heartbeat") {
    withRoot { root =>
      val group = "provider-retention"
      val provider = new ReferenceShuffleRecoveryClaimProvider(
        root,
        Conf,
        retentionMillis = Some(1000L))
      certifyMaps(
        root,
        provider,
        group,
        generation = 1L,
        incarnation = "incarnation-a",
        Vector(51L),
        reducerCount = 1)

      val groupDirectory = encodedGroupPath(root, group)
      Files.setLastModifiedTime(groupDirectory, FileTime.fromMillis(1000L))
      assert(!provider.expireAbandonedGroup(group, nowMillis = 1999L))
      assert(Files.exists(groupDirectory))
      assert(provider.expireAbandonedGroup(group, nowMillis = 2000L))
      assert(!Files.exists(groupDirectory))
      assert(!provider.expireAbandonedGroup(group, nowMillis = 3000L))
    }
  }

  test("Spark snapshots mutable claim metadata before it can reach scheduler state") {
    withRoot { root =>
      val group = "claim-snapshot"
      val generation = 1L
      val incarnation = "incarnation-a"
      val targetShuffleId = 601
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val certified = certifyMaps(
        root,
        provider,
        group,
        generation,
        incarnation,
        Vector(61L),
        reducerCount = 2)
      val claimed = claim(
        provider,
        certified,
        group,
        generation,
        incarnation,
        targetShuffleId)

      val target = ShuffleRecoveryAdoptionTarget(
        ShuffleRecoveryMaterializationId(1L, 1L),
        targetShuffleId,
        dependencyIdentity = 1L,
        mapperCount = 1,
        reducerCount = 2)
      val feasibility = ShuffleRecoveryFeasibilityInputs(
        "snapshot-source",
        "snapshot-producer",
        "row-v1",
        "hash-v1",
        "literal=v1")
      val identity = feasibility.identityFor(target)
      val request = ShuffleRecoveryPreparationRequest(
        group,
        currentGeneration = 2L,
        target,
        feasibility)
      val candidate = ShuffleRecoveryValidatedCandidate(
        group,
        generation,
        incarnation,
        identity,
        mapperCount = 1,
        reducerCount = 2,
        ShuffleRecoveryManifest.DescriptorVersion,
        certified.mapArtifacts)
      val manager = new ShuffleRecoveryReservationManager
      val reservation = manager.reserve(target).fold(reason => fail(reason), value => value)
      val prepared = new ShuffleRecoveryUntrustedBoundary()
        .validateClaim(request, reservation, candidate, claimed)
        .fold(reason => fail(reason), value => value)

      val digest = prepared.maps.head.exactIndexDigest
      claimed.descriptor.maps(0).exactIndexDigest(0) =
        (claimed.descriptor.maps(0).exactIndexDigest(0) ^ 1).toByte
      claimed.descriptor.maps(0) = null

      assert(prepared.maps.head.exactIndexDigest == digest)
      assert(prepared.maps.head.providerHandle == certified.mapArtifacts.head.providerHandle)
      provider.release(claimed.binding)
      manager.shutdown()
    }
  }

  test("external provider operations are rejected on the DAGScheduler event-loop thread") {
    withRoot { root =>
      val group = "thread-guard"
      val generation = 1L
      val incarnation = "incarnation-a"
      val provider = new ReferenceShuffleRecoveryClaimProvider(root, Conf)
      val certified = certifyMaps(
        root,
        provider,
        group,
        generation,
        incarnation,
        Vector(71L),
        reducerCount = 1)
      val claimed = claim(
        provider,
        certified,
        group,
        generation,
        incarnation,
        targetShuffleId = 701)
      val certificationRequest = DurableShuffleRecoveryCertificationRequest(
        group,
        generation,
        incarnation,
        Vector(71L),
        reducerCount = 1)
      val claimRequest = claimRequestFor(
        certified,
        group,
        generation,
        incarnation,
        targetShuffleId = 702)

      assertDagSchedulerRejected {
        provider.certifyWinningSelection(certificationRequest)
      }
      assertDagSchedulerRejected {
        provider.claim(claimRequest)
      }
      assertDagSchedulerRejected {
        provider.release(claimed.binding)
      }
      assertDagSchedulerRejected {
        provider.finishAttempt()
      }
      assertDagSchedulerRejected {
        provider.retireExact(DurableShuffleRecoveryRetirementRequest(
          group,
          generation,
          incarnation,
          examinedRevision = None))
      }
      assertDagSchedulerRejected {
        provider.finishGroup(TestAuthority(group))
      }

      assert(provider.isBound(claimed.binding))
      provider.release(claimed.binding)
    }
  }

  private def certifyMaps(
      root: Path,
      provider: ReferenceShuffleRecoveryClaimProvider,
      group: String,
      generation: Long,
      incarnation: String,
      taskIds: Vector[Long],
      reducerCount: Int): DurableShuffleRecoveryCertifiedSelection = {
    val lowLevel = ReferenceShuffleProvider.open(root, group, generation, incarnation, Conf)
    taskIds.zipWithIndex.foreach { case (taskId, mapIndex) =>
      val bytes = s"$incarnation:$mapIndex".getBytes(StandardCharsets.UTF_8)
      writeCandidate(
        lowLevel,
        taskId,
        reducerCount,
        Map(mapIndex % reducerCount -> bytes))
    }
    provider.certifyWinningSelection(DurableShuffleRecoveryCertificationRequest(
      group,
      generation,
      incarnation,
      taskIds,
      reducerCount))
  }

  private def claim(
      provider: ReferenceShuffleRecoveryClaimProvider,
      certified: DurableShuffleRecoveryCertifiedSelection,
      group: String,
      generation: Long,
      incarnation: String,
      targetShuffleId: Int): ShuffleRecoveryClaimed = {
    provider.claim(claimRequestFor(
      certified,
      group,
      generation,
      incarnation,
      targetShuffleId)) match {
      case value: ShuffleRecoveryClaimed => value
      case other => fail(s"provider claim failed: $other")
    }
  }

  private def claimRequestFor(
      certified: DurableShuffleRecoveryCertifiedSelection,
      group: String,
      generation: Long,
      incarnation: String,
      targetShuffleId: Int): ShuffleRecoveryClaimRequest = {
    ShuffleRecoveryClaimRequest(
      group,
      generation,
      incarnation,
      certified.providerCapabilityId,
      targetShuffleId,
      certified.mapArtifacts.size,
      certified.reducerCount,
      certified.mapArtifacts)
  }

  private def writeCandidate(
      provider: ReferenceShuffleProvider,
      mapTaskId: Long,
      reducerCount: Int,
      blocks: Map[Int, Array[Byte]]): ReferenceShuffleOutputDescriptor = {
    val writer = provider.createMapOutputWriter(mapTaskId, reducerCount)
    blocks.toSeq.sortBy(_._1).foreach { case (reduceId, bytes) =>
      val stream = writer.getPartitionWriter(reduceId).openStream()
      try {
        stream.write(bytes)
      } finally {
        stream.close()
      }
    }
    descriptorOf(writer.commitAllPartitions(Array.empty[Long]))
  }

  private def descriptorOf(message: MapOutputCommitMessage): ReferenceShuffleOutputDescriptor = {
    message.getMapOutputMetadata.get().asInstanceOf[ReferenceShuffleOutputDescriptor]
  }

  private def assertDagSchedulerRejected(operation: => Any): Unit = {
    val failure = new AtomicReference[Throwable]()
    val completed = new CountDownLatch(1)
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          operation
        } catch {
          case error: Throwable => failure.set(error)
        } finally {
          completed.countDown()
        }
      }
    }, "dag-scheduler-event-loop")
    thread.start()
    assert(completed.await(AwaitSeconds, TimeUnit.SECONDS))
    thread.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
    assert(!thread.isAlive)
    assert(failure.get().isInstanceOf[IllegalStateException], String.valueOf(failure.get()))
  }

  private def encodedGroupPath(root: Path, recoveryGroup: String): Path = {
    val encoded = Base64.getUrlEncoder.withoutPadding().encodeToString(
      recoveryGroup.getBytes(StandardCharsets.UTF_8))
    root.toAbsolutePath.normalize().resolve(encoded)
  }

  private def withRoot(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory("durable-shuffle-recovery-provider-")
    try {
      body(root)
    } finally {
      ReferenceShuffleProvider.deleteRecursively(root)
    }
  }
}
