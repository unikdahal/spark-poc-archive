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

package org.apache.spark.scheduler

import java.io.{ObjectInput, ObjectOutput}

import org.apache.spark.storage.BlockManagerId

/**
 * Compact marker for one mapper of an adopted shuffle.
 *
 * No reducer sizes are stored here. A recovered marker must be recognized by MapOutputTracker and
 * resolved through a bounded, generation-fenced exact metadata query before fetch tuples are
 * created. Calling [[getSizeForBlock]] directly is therefore a correctness error rather than an
 * invitation to use an approximate synthetic size.
 */
private[spark] final class ShuffleRecoveryMapStatus(
    location: BlockManagerId,
    mapTaskId: Long,
    private var _localBindingGeneration: Long)
  extends CompressedMapStatus(location, Array.emptyByteArray, mapTaskId, 0L) {

  def this() = this(null, -1L, -1L)

  def localBindingGeneration: Long = _localBindingGeneration

  override def getSizeForBlock(reduceId: Int): Long = {
    throw new IllegalStateException(
      "recovered shuffle block sizes require an exact generation-fenced metadata query")
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    super.writeExternal(out)
    out.writeLong(_localBindingGeneration)
  }

  override def readExternal(in: ObjectInput): Unit = {
    super.readExternal(in)
    _localBindingGeneration = in.readLong()
  }
}
