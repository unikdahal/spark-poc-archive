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

import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/** Out-of-tree conformance bridge; not a public connector API or automatic recovery hook. */
object ShuffleRecoveryIcebergIdentityBridge {
  def identity(
      root: SparkPlan,
      scan: BatchScanExec,
      certificate: Array[Byte],
      decompositionDigest: Array[Byte],
      mapperCount: Int,
      providerReadFormatId: String): String = {
    val exchanges = root.collect { case exchange: ShuffleExchangeExec => exchange }
    require(exchanges.size == 1, "expected exactly one real shuffle boundary")
    val exchange = exchanges.head
    require(exchange.child.collect { case batch: BatchScanExec => batch }
      .exists(_ eq scan), "certificate scan must belong to the shuffle producer")
    val partitions = scan.inputPartitions
    require(partitions.size == mapperCount, "Iceberg task groups must match Spark partitions")
    require(decompositionDigest.length == 32, "expected SHA-256 decomposition digest")
    // The connector has verified each partition's task-group object. The complete ordered
    // decomposition digest plus ordinal identifies its split without serializing runtime objects.
    val descriptors = partitions.toVector.zipWithIndex.map { case (partition, ordinal) =>
      partition -> ByteBuffer.allocate(40).putInt(mapperCount).putInt(ordinal)
        .put(decompositionDigest).array()
    }
    val binding = ShuffleRecoverySourceBinding.bind(scan, "org.apache.iceberg.resolved-scan", 1,
      certificate, descriptors).fold(reason =>
        throw new IllegalArgumentException(s"Iceberg binding refused: $reason"),
        binding => binding)
    val conf = SparkEnv.get.conf
    val inputs = ShuffleRecoveryResolvedIdentityInputs.create(
      Nil, binding.decomposition, Map.empty,
      ShuffleRecoveryIdentitySemanticConfig(
        exchange.conf.ansiEnabled,
        exchange.conf.sessionLocalTimeZone,
        conf.getBoolean("spark.shuffle.compress", true),
        conf.get("spark.io.compression.codec", "lz4"),
        conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k").toInt,
        conf.getBoolean("spark.io.encryption.enabled", false)),
      providerReadFormatId, Some(binding))
    require(exchange.shuffleDependency.rdd.partitions.length == mapperCount,
      "source partitions must map one-to-one to actual shuffle mappers")
    ShuffleRecoveryComputationIdentityBuilder.build(exchange, inputs) match {
      case ShuffleRecoveryIdentityBuilt(result) => result.digest
      case ShuffleRecoveryIdentityRejected(reason) =>
        throw new IllegalArgumentException(s"Iceberg canonical identity refused: $reason")
    }
  }
}
