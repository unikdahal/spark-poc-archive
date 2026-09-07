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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}

class ShuffleRecoverySourceAdapterBindingSuite extends SparkFunSuite {
  import ShuffleRecoverySourceAdapterResult._
  import ShuffleRecoverySourceIdentityMiss._
  import ShuffleRecoverySourceIdentityResult._

  test("candidate adapter id must match the exact adapter selected for the plan class") {
    val registry = ShuffleRecoverySourceReadIdentity.registry(
      Seq(new MismatchedReferenceAdapter, new SecondaryRangeAdapter))
    val plan = ReferenceSnapshotScanExec(
      sourceId = "source-binding",
      sourceName = "orders",
      snapshotVersion = 1L,
      schemaFingerprint = "schema-v1:id:int:not-null",
      projection = Seq("value"),
      filter = None,
      readOptions = Map.empty,
      selectedObjects = Seq("part-000"),
      splits = Seq("part-000:0-9"),
      rows = Seq(1))

    assert(registry.identify(plan) === Miss(UnknownAdapter))
  }

  private final class MismatchedReferenceAdapter extends ShuffleRecoverySourceReadAdapter {
    override val planClass: Class[_ <: SparkPlan] = classOf[ReferenceSnapshotScanExec]
    override val adapterId: String = "spark.test.selected.v1"

    override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = {
      Candidate(ShuffleRecoverySourceTokenCandidate(
        schemaVersion = ShuffleRecoverySourceReadIdentity.CurrentTokenSchemaVersion,
        adapterId = "spark.test.other.v1",
        canonicalBytes = Array[Byte](1),
        decompositionCertificate = Some(Array[Byte](2))))
    }
  }

  private final class SecondaryRangeAdapter extends ShuffleRecoverySourceReadAdapter {
    override val planClass: Class[_ <: SparkPlan] = classOf[RangeExec]
    override val adapterId: String = "spark.test.other.v1"

    override def sourceToken(plan: SparkPlan): ShuffleRecoverySourceAdapterResult = Unavailable
  }
}
