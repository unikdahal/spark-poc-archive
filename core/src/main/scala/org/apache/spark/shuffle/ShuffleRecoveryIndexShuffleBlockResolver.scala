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
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.{MapOutputTrackerMaster, ShuffleRecoverySchedulerAdoptionState, SparkConf}
import org.apache.spark.SparkEnv
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.collection.OpenHashSet

private[spark] final case class ShuffleRecoveryReadMetrics(
    blockReads: Long,
    nonEmptyBlockReads: Long,
    emptyBlockReads: Long,
    bytesRead: Long)

/**
 * Feasibility-only read indirection for an adopted durable-provider shuffle.
 *
 * The scheduler installs only an immutable current-shuffle-id binding. Provider access remains on
 * shuffle fetch threads or MapOutputTracker's dedicated dispatcher, never on the DAGScheduler
 * event loop. Exact reducer ranges are resolved lazily and no provider-specific M x R descriptor
 * is retained in driver memory.
 */
private[spark] final class ShuffleRecoveryIndexShuffleBlockResolver(
    conf: SparkConf,
    taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]])
  extends IndexShuffleBlockResolver(conf, null, taskIdMapsForShuffle) {

  private final class RecoveredReadBinding(
      val provider: DurableShuffleRecoveryProvider,
      val binding: ShuffleRecoveryBinding,
      val mapperCount: Int,
      val reducerCount: Int,
      val localBindingGeneration: Long,
      val maps: Vector[ShuffleRecoveryPreparedMap],
      val capabilities: DurableShuffleRecoveryCapabilities,
      val statistics: ShuffleRecoveryStatistics) {
    val usable = new AtomicBoolean(true)

    val readCapabilities: ShuffleRecoveryReadCapabilities = ShuffleRecoveryReadCapabilities(
      reducerAggregateStatisticsAvailable = statistics.bytesByReducer.isDefined,
      // The provider can answer exact ranges, but the prototype deliberately does not expose
      // mapper-local distribution as an adaptive capability. This prevents local/skew/partial-map
      // readers from treating on-demand physical metadata as a materialized mapper distribution.
      exactMapperLocalDistributionAvailable = false,
      partitionSpecCompatible = true)
  }

  private final class RecoveredReadCounters {
    val blockReads = new AtomicLong(0L)
    val nonEmptyBlockReads = new AtomicLong(0L)
    val emptyBlockReads = new AtomicLong(0L)
    val bytesRead = new AtomicLong(0L)
  }

  private val recoveredBindings = new ConcurrentHashMap[Int, RecoveredReadBinding]()
  private val recoveredReadCounters = new ConcurrentHashMap[Int, RecoveredReadCounters]()
  private val observedFailures =
    new ConcurrentHashMap[Int, ShuffleRecoveryObservedFetchFailure]()

  private[spark] val schedulerAdoption = new ShuffleRecoverySchedulerAdoptionState(this)

  private[spark] def installRecoveredBinding(
      targetShuffleId: Int,
      provider: DurableShuffleRecoveryProvider,
      binding: ShuffleRecoveryBinding,
      mapperCount: Int,
      reducerCount: Int,
      localBindingGeneration: Long,
      maps: Vector[ShuffleRecoveryPreparedMap],
      capabilities: DurableShuffleRecoveryCapabilities,
      statistics: ShuffleRecoveryStatistics): Boolean = {
    if (targetShuffleId < 0 || provider == null || binding == null || maps == null ||
        capabilities == null || statistics == null || binding.targetShuffleId != targetShuffleId ||
        mapperCount < 0 || reducerCount <= 0 || localBindingGeneration <= 0L ||
        maps.size != mapperCount || statistics.bytesByReducer.forall(_.size != reducerCount) ||
        maps.indices.exists(index => maps(index) == null || maps(index).mapIndex != index)) {
      false
    } else {
      val candidate = new RecoveredReadBinding(
        provider,
        binding,
        mapperCount,
        reducerCount,
        localBindingGeneration,
        maps,
        capabilities,
        statistics)
      val existing = recoveredBindings.putIfAbsent(targetShuffleId, candidate)
      val installed = existing == null ||
        ((existing.provider eq provider) &&
          existing.binding == binding &&
          existing.mapperCount == mapperCount &&
          existing.reducerCount == reducerCount &&
          existing.localBindingGeneration == localBindingGeneration)
      if (installed) {
        observedFailures.remove(targetShuffleId)
        recoveredReadCounters.putIfAbsent(targetShuffleId, new RecoveredReadCounters)
      }
      installed
    }
  }

  private[spark] def invalidateRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding,
      localBindingGeneration: Long): Boolean = {
    if (binding == null || localBindingGeneration <= 0L) {
      false
    } else {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding &&
          existing.localBindingGeneration == localBindingGeneration) {
        existing.usable.set(false)
        recoveredBindings.remove(targetShuffleId, existing)
      } else {
        false
      }
    }
  }

  private[spark] def removeRecoveredBinding(
      targetShuffleId: Int,
      binding: ShuffleRecoveryBinding): Unit = {
    if (binding != null) {
      val existing = recoveredBindings.get(targetShuffleId)
      if (existing != null && existing.binding == binding &&
          recoveredBindings.remove(targetShuffleId, existing)) {
        existing.usable.set(false)
        recoveredReadCounters.remove(targetShuffleId)
        observedFailures.remove(targetShuffleId)
      }
    }
  }

  private[spark] def observedFetchFailure(
      targetShuffleId: Int): Option[ShuffleRecoveryObservedFetchFailure] =
    Option(observedFailures.get(targetShuffleId))

  private[shuffle] def recordObservedFailure(
      targetShuffleId: Int,
      observed: ShuffleRecoveryObservedFetchFailure): Unit = {
    if (targetShuffleId < 0 || observed == null || observed.localBindingGeneration <= 0L ||
        observed.bindingId == null) {
      return
    }
    observedFailures.compute(targetShuffleId, (_, previous) => {
      if (previous == null ||
          observed.localBindingGeneration > previous.localBindingGeneration ||
          (observed.localBindingGeneration == previous.localBindingGeneration &&
            previous.bindingId == observed.bindingId &&
            !previous.failureClass.authorizesRetirement &&
            observed.failureClass.authorizesRetirement)) {
        observed
      } else {
        previous
      }
    })

    Option(SparkEnv.get).foreach { env =>
      env.mapOutputTracker match {
        case tracker: MapOutputTrackerMaster =>
          schedulerAdoption.invalidateObservedFetchFailure(tracker, targetShuffleId)
        case _ =>
      }
    }
  }

  private[spark] def clearRecoveryState(targetShuffleId: Int): Unit = {
    observedFailures.remove(targetShuffleId)
    recoveredReadCounters.remove(targetShuffleId)
  }

  private[spark] def isRecovered(targetShuffleId: Int): Boolean =
    recoveredBindings.containsKey(targetShuffleId)

  private[spark] def recoveredBindingCount: Int = recoveredBindings.size()

  private[spark] def recoveredReadMetrics(targetShuffleId: Int): ShuffleRecoveryReadMetrics = {
    val counters = recoveredReadCounters.get(targetShuffleId)
    if (counters == null) {
      ShuffleRecoveryReadMetrics(0L, 0L, 0L, 0L)
    } else {
      ShuffleRecoveryReadMetrics(
        counters.blockReads.get(),
        counters.nonEmptyBlockReads.get(),
        counters.emptyBlockReads.get(),
        counters.bytesRead.get())
    }
  }

  private[spark] def recoveredStatistics(
      targetShuffleId: Int): Option[(Long, ShuffleRecoveryStatistics)] = {
    Option(recoveredBindings.get(targetShuffleId)).filter(_.usable.get()).map { recovered =>
      (recovered.localBindingGeneration, recovered.statistics)
    }
  }

  private[spark] def recoveredReadCapabilities(
      targetShuffleId: Int): Option[(Long, ShuffleRecoveryReadCapabilities)] = {
    Option(recoveredBindings.get(targetShuffleId)).filter(_.usable.get()).map { recovered =>
      (recovered.localBindingGeneration, recovered.readCapabilities)
    }
  }

  /**
   * Resolves exact physical lengths for one bounded fetch range.
   *
   * The current Phase 1 representation permits reducer ranges only when every mapper is included.
   * Partial mapper ranges imply skew/local/mapper-local semantics and are rejected at this final
   * boundary. Rejection atomically abandons the adoption but never authorizes durable retirement.
   */
  private[spark] def queryExactBlocks(
      query: ShuffleRecoveryExactBlockQuery): ShuffleRecoveryExactBlockQueryResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery exact metadata query")
    if (query == null || query.targetShuffleId < 0 || query.localBindingGeneration <= 0L) {
      return ShuffleRecoveryExactBlocksUnsupported("recovered metadata query is malformed")
    }
    val recovered = recoveredBindings.get(query.targetShuffleId)
    if (recovered == null || !recovered.usable.get()) {
      return ShuffleRecoveryExactBlocksNotAdopted
    }
    if (recovered.localBindingGeneration != query.localBindingGeneration) {
      return ShuffleRecoveryExactBlocksNotAdopted
    }
    if (query.startMapIndex != 0 || query.endMapIndex != recovered.mapperCount) {
      invalidateUnsupportedRead(query.targetShuffleId, recovered.localBindingGeneration)
      return ShuffleRecoveryExactBlocksUnsupported(
        "partial mapper reads are disabled for recovered shuffles")
    }
    if (query.startReduceId < 0 || query.endReduceId < query.startReduceId ||
        query.endReduceId > recovered.reducerCount) {
      invalidateUnsupportedRead(query.targetShuffleId, recovered.localBindingGeneration)
      return ShuffleRecoveryExactBlocksUnsupported("reducer range is outside the recovered shape")
    }
    val requestedBlocks = try {
      Math.multiplyExact(
        (query.endMapIndex - query.startMapIndex).toLong,
        (query.endReduceId - query.startReduceId).toLong)
    } catch {
      case _: ArithmeticException =>
        invalidateUnsupportedRead(query.targetShuffleId, recovered.localBindingGeneration)
        return ShuffleRecoveryExactBlocksUnsupported("recovered metadata range overflowed")
    }
    if (requestedBlocks > ShuffleRecoveryExactBlockQuery.MaxRequestedBlocks) {
      invalidateUnsupportedRead(query.targetShuffleId, recovered.localBindingGeneration)
      return ShuffleRecoveryExactBlocksUnsupported("recovered metadata range exceeds the bound")
    }

    val blocks = new ArrayBuffer[ShuffleRecoveryExactBlock](
      math.min(requestedBlocks, Int.MaxValue.toLong).toInt)
    var mapIndex = query.startMapIndex
    while (mapIndex < query.endMapIndex) {
      if (!recovered.usable.get()) {
        return ShuffleRecoveryExactBlocksUnavailable("recovered binding was invalidated")
      }
      val resolved = recovered.provider.openBoundMapForFetch(
        recovered.binding, mapIndex, recovered.maps(mapIndex)) match {
        case ShuffleRecoveryBoundMapOpened(value) => value
        case ShuffleRecoveryBoundMapFailed(failureClass) =>
          recordQueryFailure(query, recovered, mapIndex, query.startReduceId, failureClass)
          return ShuffleRecoveryExactBlocksUnavailable("provider could not open recovered map")
      }
      if (resolved.numReducers != recovered.reducerCount) {
        recordQueryFailure(
          query, recovered, mapIndex, query.startReduceId, ShuffleRecoveryAdoptedCorrupt)
        return ShuffleRecoveryExactBlocksUnavailable("recovered reducer shape changed")
      }
      var reduceId = query.startReduceId
      while (reduceId < query.endReduceId) {
        val metadata = try {
          resolved.blockMetadata(reduceId)
        } catch {
          case NonFatal(_) =>
            recordQueryFailure(query, recovered, mapIndex, reduceId, ShuffleRecoveryAdoptedCorrupt)
            return ShuffleRecoveryExactBlocksUnavailable("recovered block metadata is invalid")
        }
        if (metadata == null || metadata.offset < 0L || metadata.length < 0L ||
            metadata.offset > resolved.dataLength ||
            metadata.length > resolved.dataLength - metadata.offset) {
          recordQueryFailure(query, recovered, mapIndex, reduceId, ShuffleRecoveryAdoptedCorrupt)
          return ShuffleRecoveryExactBlocksUnavailable("recovered block range is invalid")
        }
        if (metadata.length > 0L) {
          blocks += ShuffleRecoveryExactBlock(mapIndex, reduceId, metadata.length)
        }
        reduceId += 1
      }
      mapIndex += 1
    }
    if (!recovered.usable.get()) {
      ShuffleRecoveryExactBlocksUnavailable("recovered binding was invalidated during query")
    } else {
      ShuffleRecoveryExactBlocksAvailable(recovered.localBindingGeneration, blocks.toVector)
    }
  }

  private def recordQueryFailure(
      query: ShuffleRecoveryExactBlockQuery,
      recovered: RecoveredReadBinding,
      mapIndex: Int,
      reduceId: Int,
      failureClass: ShuffleRecoveryAdoptedReadFailureClass): Unit = {
    recordObservedFailure(
      query.targetShuffleId,
      ShuffleRecoveryObservedFetchFailure(
        recovered.localBindingGeneration,
        recovered.binding.bindingId,
        mapIndex,
        reduceId,
        failureClass))
  }

  private def invalidateUnsupportedRead(
      targetShuffleId: Int,
      localBindingGeneration: Long): Unit = {
    Option(SparkEnv.get).foreach { env =>
      env.mapOutputTracker match {
        case tracker: MapOutputTrackerMaster =>
          schedulerAdoption.invalidateUnsupportedRead(
            tracker, targetShuffleId, localBindingGeneration)
        case _ =>
      }
    }
  }

  private[spark] def openBoundMapForPreparation(
      provider: DurableShuffleRecoveryProvider,
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): DurableShuffleRecoveryResolvedMap = {
    provider.openBoundMap(binding, mapIndex)
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    blockId match {
      case id: ShuffleBlockId =>
        val recovered = recoveredBindings.get(id.shuffleId)
        if (recovered == null) {
          super.getBlockData(blockId, dirs)
        } else {
          readRecoveredBlock(id, recovered)
        }

      case batch: ShuffleBlockBatchId if recoveredBindings.containsKey(batch.shuffleId) =>
        throw new IOException("batch fetch is disabled for an adopted durable shuffle")

      case _ =>
        super.getBlockData(blockId, dirs)
    }
  }

  private def readRecoveredBlock(
      id: ShuffleBlockId,
      recovered: RecoveredReadBinding): ManagedBuffer = {
    if (!recovered.usable.get()) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle binding was invalidated before fetch")
    }
    if (id.mapId < 0L || id.mapId > Int.MaxValue.toLong) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle map id is outside the supported range")
    }
    val mapIndex = id.mapId.toInt
    if (mapIndex >= recovered.mapperCount ||
        id.reduceId < 0 || id.reduceId >= recovered.reducerCount) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle block coordinates are outside the binding")
    }

    val resolved = recovered.provider.openBoundMapForFetch(
      recovered.binding, mapIndex, recovered.maps(mapIndex)) match {
      case ShuffleRecoveryBoundMapOpened(value) => value
      case ShuffleRecoveryBoundMapFailed(failureClass) =>
        recordFailure(id, recovered, failureClass)
        throw new IOException(s"adopted shuffle provider read failed: $failureClass")
    }
    if (resolved.numReducers != recovered.reducerCount) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
      throw new IOException("adopted shuffle reducer shape changed after validation")
    }

    val metadata = try {
      resolved.blockMetadata(id.reduceId)
    } catch {
      case _: IllegalArgumentException =>
        recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
        throw new IOException("adopted shuffle block metadata is corrupt")
    }
    if (metadata.offset < 0L || metadata.length < 0L ||
        metadata.offset > resolved.dataLength ||
        metadata.length > resolved.dataLength - metadata.offset) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedCorrupt)
      throw new IOException("adopted shuffle block range is corrupt")
    }

    if (!recovered.usable.get()) {
      recordFailure(id, recovered, ShuffleRecoveryAdoptedUnavailable)
      throw new IOException("recovered shuffle binding was invalidated during fetch")
    }

    val counters = recoveredReadCounters.get(id.shuffleId)
    if (counters != null) {
      counters.blockReads.incrementAndGet()
      if (metadata.isEmpty) {
        counters.emptyBlockReads.incrementAndGet()
      } else {
        counters.nonEmptyBlockReads.incrementAndGet()
        counters.bytesRead.addAndGet(metadata.length)
      }
    }
    resolved.getBlockData(id.reduceId).getOrElse {
      new NioManagedBuffer(ByteBuffer.allocate(0))
    }
  }

  private def recordFailure(
      id: ShuffleBlockId,
      recovered: RecoveredReadBinding,
      failureClass: ShuffleRecoveryAdoptedReadFailureClass): Unit = {
    val mapIndex = if (id.mapId >= 0L && id.mapId <= Int.MaxValue.toLong) {
      id.mapId.toInt
    } else {
      -1
    }
    recordObservedFailure(
      id.shuffleId,
      ShuffleRecoveryObservedFetchFailure(
        recovered.localBindingGeneration,
        recovered.binding.bindingId,
        mapIndex,
        id.reduceId,
        failureClass))
  }
}
