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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.{BlockingShuffleManager, ShuffleBlockResolver, ShuffleHandle}
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter

/** Adapt the native Celeborn manager to this Spark branch's blocking-manager interface. */
class ShuffleRecoveryCelebornManager(conf: SparkConf, isDriver: Boolean)
  extends BlockingShuffleManager {
  private[celeborn] val delegate = new SparkShuffleManager(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    delegate.registerShuffle[K, V, C](shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    delegate.getWriter[K, V](handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    delegate.getReader[K, C](handle, startMapIndex, endMapIndex, startPartition, endPartition,
      context, metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = delegate.shuffleBlockResolver()
  override def unregisterShuffle(shuffleId: Int): Boolean = delegate.unregisterShuffle(shuffleId)
  override def stop(): Unit = delegate.stop()
}
