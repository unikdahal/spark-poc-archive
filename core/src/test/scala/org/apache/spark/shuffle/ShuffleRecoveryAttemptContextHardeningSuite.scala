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

import org.apache.spark.SparkFunSuite

class ShuffleRecoveryAttemptContextHardeningSuite extends SparkFunSuite {

  private val Group = ShuffleRecoveryGroupKey("hardening-group", "hardening-lineage")
  private val Authorization = ShuffleRecoveryAuthorizationContext(
    "hardening-principal",
    Some("hardening-policy"),
    Some("hardening-view"))
  private val Capabilities = ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = false)
  private val NoExpiry = ShuffleRecoveryRetentionPolicy(None)

  test("attempt context diagnostics redact all authorization and lineage identifiers") {
    val secretGroup = ShuffleRecoveryGroupKey("secret-group", "secret-lineage")
    val secretAuthorization = ShuffleRecoveryAuthorizationContext(
      "secret-principal",
      Some("secret-policy"),
      Some("secret-view"))
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = ShuffleRecoveryAttemptContext.fromExternalAllocation(
      secretGroup,
      "secret-attempt",
      secretAuthorization,
      Capabilities,
      NoExpiry,
      allocator,
      Some("7"),
      100L) match {
      case Right(value) => value
      case Left(diagnostic) => fail(diagnostic.toString)
    }

    val rendered = context.toString
    Seq(
      "secret-group",
      "secret-lineage",
      "secret-attempt",
      "secret-principal",
      "secret-policy",
      "secret-view").foreach { secret =>
      assert(!rendered.contains(secret))
    }
    assert(rendered.contains("generation=7"))
    assert(rendered.contains("principal=redacted"))
  }

  test("reference generation allocator refuses new state after bounded capacity") {
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator(
      maxGroups = 1,
      maxAttemptsPerGroup = 1)
    val first = ShuffleRecoveryGenerationRequest(Group, "attempt-1", "hardening-principal")
    val second = ShuffleRecoveryGenerationRequest(Group, "attempt-2", "hardening-principal")
    val otherGroup = ShuffleRecoveryGenerationRequest(
      ShuffleRecoveryGroupKey("other-group", "other-lineage"),
      "attempt-1",
      "hardening-principal")

    assert(allocator.allocate(first) == ShuffleRecoveryGenerationAllocated(1L))
    assert(allocator.allocate(first) == ShuffleRecoveryGenerationAllocated(1L))
    assert(allocator.allocate(second) == ShuffleRecoveryGenerationAllocationUnavailable)
    assert(allocator.allocate(otherGroup) == ShuffleRecoveryGenerationAllocationUnavailable)
    assert(allocator.trackedGroupCount == 1)
    assert(allocator.trackedAttemptCount(Group) == 1)
  }

  test("authorization revision exhaustion invalidates the maximum revision grant") {
    val state = new ShuffleRecoveryAttemptState
    val fence = new ShuffleRecoveryAuthorizationFence(Long.MaxValue)
    val grant = new ShuffleRecoveryAuthorizationGrant(
      ShuffleRecoveryDiscover,
      Long.MaxValue,
      state,
      fence)

    assert(grant.isCurrent)
    assert(fence.advance() == 0L)
    assert(!grant.isCurrent)
    assert(fence.advance() == 0L)
  }

  test("generation allocator failures disable recovery instead of escaping") {
    val allocator = new ShuffleRecoveryGenerationAllocator {
      override def allocate(
          request: ShuffleRecoveryGenerationRequest): ShuffleRecoveryGenerationResult =
        throw new IllegalStateException("allocator unavailable")

      override def reserveAssigned(
          request: ShuffleRecoveryGenerationRequest,
          generation: Long): ShuffleRecoveryGenerationResult =
        throw new IllegalStateException("allocator unavailable")
    }

    val allocated = ShuffleRecoveryAttemptContext.allocate(
      Group,
      "attempt-allocate-failure",
      Authorization,
      Capabilities,
      NoExpiry,
      allocator,
      100L)
    assert(allocated.left.exists(_.code == ShuffleRecoveryContextUnavailable))

    val assigned = ShuffleRecoveryAttemptContext.fromExternalAllocation(
      Group,
      "attempt-reserve-failure",
      Authorization,
      Capabilities,
      NoExpiry,
      allocator,
      Some("2"),
      100L)
    assert(assigned.left.exists(_.code == ShuffleRecoveryContextUnavailable))
  }

  test("authorization authority failures become safe provider-unavailable diagnostics") {
    val context = contextAt(2L)
    val authority = new ShuffleRecoveryAuthorizationAuthority {
      override def authorize(
          request: ShuffleRecoveryAuthorizationRequest): ShuffleRecoveryAuthorizationDecision =
        throw new IllegalStateException("policy service unavailable")
    }

    val result = context.authorize(authority, ShuffleRecoveryDiscover)
    assert(result.left.exists(_.code == ShuffleRecoveryProviderUnavailable))
  }

  test("null optional authorization state is rejected without dereference") {
    val invalidAuthorization = ShuffleRecoveryAuthorizationContext(
      "hardening-principal",
      null,
      None)
    val result = ShuffleRecoveryAttemptContext.fromExternalAllocation(
      Group,
      "attempt-invalid-options",
      invalidAuthorization,
      Capabilities,
      NoExpiry,
      new ReferenceShuffleRecoveryGenerationAllocator,
      Some("2"),
      100L)
    assert(result.left.exists(_.code == ShuffleRecoveryContextUnavailable))
  }

  test("group lifecycle backend failure is observability only") {
    val finishCapabilities = ShuffleRecoveryLifecycleCapabilities(mayFinishGroup = true)
    val allocator = new ReferenceShuffleRecoveryGenerationAllocator
    val context = ShuffleRecoveryAttemptContext.fromExternalAllocation(
      Group,
      "attempt-group-backend-failure",
      Authorization,
      finishCapabilities,
      NoExpiry,
      allocator,
      Some("2"),
      100L) match {
      case Right(value) => value
      case Left(diagnostic) => fail(diagnostic.toString)
    }
    val authority = new ReferenceShuffleRecoveryAuthorizationAuthority
    authority.allow(Group, Authorization.principalRef, Authorization.dataViewRef)
    val backend = new ShuffleRecoveryGroupLifecycleBackend {
      override def finishGroup(
          groupKey: ShuffleRecoveryGroupKey): ShuffleRecoveryGroupFinishResult =
        throw new IllegalStateException("cleanup backend unavailable")
    }

    assert(new ShuffleRecoveryGroupLifecycleManager(authority, backend).finishGroup(context) ==
      ShuffleRecoveryGroupFinishUnavailable)
  }

  private def contextAt(generation: Long): ShuffleRecoveryAttemptContext = {
    ShuffleRecoveryAttemptContext.fromExternalAllocation(
      Group,
      s"attempt-$generation",
      Authorization,
      Capabilities,
      NoExpiry,
      new ReferenceShuffleRecoveryGenerationAllocator,
      Some(generation.toString),
      100L) match {
      case Right(value) => value
      case Left(diagnostic) => fail(diagnostic.toString)
    }
  }
}
