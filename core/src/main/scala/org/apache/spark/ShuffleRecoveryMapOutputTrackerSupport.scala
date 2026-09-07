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

package org.apache.spark

import scala.collection
import scala.collection.mutable.ListBuffer

import org.apache.spark.scheduler.{MapStatus, ShuffleRecoveryMapStatus}
import org.apache.spark.shuffle._
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

/**
 * Small bridge between compact recovered MapStatus markers and exact provider metadata.
 *
 * The bridge never performs provider work on the scheduler event loop. Remote requests are routed
 * through MapOutputTracker's existing map-output dispatcher; local-mode calls reach the same
 * resolver directly from the task thread. Returned fetch sizes are exact physical lengths.
 */
private[spark] object ShuffleRecoveryMapOutputTrackerSupport {

  final case class MarkerContext(
      location: BlockManagerId,
      localBindingGeneration: Long,
      mapperCount: Int)

  def markerContext(
      shuffleId: Int,
      statuses: Array[MapStatus],
      partition: Int): Option[MarkerContext] = {
    if (statuses == null || statuses.isEmpty) {
      None
    } else {
      val recoveredCount = statuses.count(_.isInstanceOf[ShuffleRecoveryMapStatus])
      if (recoveredCount == 0) {
        None
      } else if (recoveredCount != statuses.length || statuses.exists(_ == null)) {
        throw metadataFailure(shuffleId, partition, "recovered and ordinary map statuses are mixed")
      } else {
        val first = statuses(0).asInstanceOf[ShuffleRecoveryMapStatus]
        if (first.localBindingGeneration <= 0L || first.location == null) {
          throw metadataFailure(shuffleId, partition, "recovered map status marker is malformed")
        }
        var index = 0
        while (index < statuses.length) {
          val marker = statuses(index).asInstanceOf[ShuffleRecoveryMapStatus]
          if (marker.localBindingGeneration != first.localBindingGeneration ||
              marker.location != first.location || marker.mapId != index.toLong) {
            throw metadataFailure(
              shuffleId, partition, "recovered map status markers are not one coherent generation")
          }
          index += 1
        }
        Some(MarkerContext(first.location, first.localBindingGeneration, statuses.length))
      }
    }
  }

  def queryFor(
      shuffleId: Int,
      context: MarkerContext,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): ShuffleRecoveryExactBlockQuery = {
    if (context == null) {
      throw metadataFailure(shuffleId, startPartition, "recovered marker context is null")
    }
    val actualEndMapIndex = if (endMapIndex == Int.MaxValue) context.mapperCount else endMapIndex
    ShuffleRecoveryExactBlockQuery(
      shuffleId,
      context.localBindingGeneration,
      startMapIndex,
      actualEndMapIndex,
      startPartition,
      endPartition)
  }

  def queryOnDriver(
      query: ShuffleRecoveryExactBlockQuery): ShuffleRecoveryExactBlockQueryResult = {
    Option(SparkEnv.get).flatMap { env =>
      Option(env.shuffleManager).flatMap { manager =>
        manager.shuffleBlockResolver match {
          case resolver: ShuffleRecoveryIndexShuffleBlockResolver => Some(resolver)
          case _ => None
        }
      }
    }.map(_.queryExactBlocks(query)).getOrElse(ShuffleRecoveryExactBlocksNotAdopted)
  }

  def recoveredStatistics(shuffleId: Int): Option[ShuffleRecoveryStatistics] = {
    Option(SparkEnv.get).flatMap { env =>
      Option(env.shuffleManager).flatMap { manager =>
        manager.shuffleBlockResolver match {
          case resolver: ShuffleRecoveryIndexShuffleBlockResolver =>
            resolver.recoveredStatistics(shuffleId).map(_._2)
          case _ => None
        }
      }
    }
  }

  def toMapSizes(
      shuffleId: Int,
      partition: Int,
      context: MarkerContext,
      result: ShuffleRecoveryExactBlockQueryResult): MapSizesByExecutorId = {
    result match {
      case ShuffleRecoveryExactBlocksAvailable(generation, blocks)
          if generation == context.localBindingGeneration =>
        val entries = new ListBuffer[(BlockId, Long, Int)]
        blocks.foreach { block =>
          if (block.length <= 0L || block.mapIndex < 0 || block.reduceId < 0 ||
              block.mapIndex >= context.mapperCount) {
            throw metadataFailure(shuffleId, partition, "exact recovered block metadata is invalid")
          }
          entries += ((ShuffleBlockId(shuffleId, block.mapIndex.toLong, block.reduceId),
            block.length, block.mapIndex))
        }
        val iter: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] =
          if (entries.isEmpty) Iterator.empty else Iterator.single(context.location -> entries)
        MapSizesByExecutorId(iter, enableBatchFetch = false)

      case ShuffleRecoveryExactBlocksAvailable(_, _) =>
        throw metadataFailure(shuffleId, partition, "recovered metadata generation changed")
      case ShuffleRecoveryExactBlocksNotAdopted =>
        throw metadataFailure(shuffleId, partition, "recovered shuffle is no longer adopted")
      case ShuffleRecoveryExactBlocksUnavailable(reason) =>
        throw metadataFailure(shuffleId, partition, reason)
      case ShuffleRecoveryExactBlocksUnsupported(reason) =>
        throw metadataFailure(shuffleId, partition, reason)
    }
  }

  private def metadataFailure(
      shuffleId: Int,
      partition: Int,
      reason: String): MetadataFetchFailedException = {
    new MetadataFetchFailedException(
      shuffleId,
      partition,
      s"Unable to resolve exact recovered shuffle metadata: $reason")
  }
}
