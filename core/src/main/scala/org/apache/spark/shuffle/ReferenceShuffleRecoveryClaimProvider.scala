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

import java.io.{BufferedInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{AccessDeniedException, Files, LinkOption, NoSuchFileException, Path}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.{Base64, UUID}
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.network.buffer.ManagedBuffer

/**
 * Local-filesystem reference implementation of the private durable recovery capability.
 *
 * A binding is an attempt-local alias only. It never moves, rewrites, revokes, or deletes the
 * immutable group-scoped artifacts that it references. Group deletion is reachable only through
 * explicit lifecycle authority or the provider's own retention policy.
 */
private[spark] final class ReferenceShuffleRecoveryClaimProvider(
    providerRoot: Path,
    conf: SparkConf = new SparkConf(false),
    retentionMillis: Option[Long] = None) extends DurableShuffleRecoveryProvider {

  import ReferenceShuffleRecoveryClaimProvider._

  if (providerRoot == null || conf == null || retentionMillis == null ||
      retentionMillis.exists(_ <= 0L)) {
    throw new IllegalArgumentException(
      "provider root, SparkConf, and positive retention policy must be valid")
  }

  private final class ActiveBinding(
      val binding: ShuffleRecoveryBinding,
      val provider: ReferenceShuffleProvider)

  private final case class InspectedMap(
      descriptor: ShuffleRecoveryClaimedMapDescriptor,
      bytesByReducer: Array[Long])

  private final class ReferenceBlockMetadataAdapter(
      delegate: ReferenceShuffleBlockMetadata) extends DurableShuffleRecoveryBlockMetadata {
    override val offset: Long = delegate.offset
    override val length: Long = delegate.length
  }

  private final class ReferenceResolvedMapAdapter(
      delegate: ReferenceShuffleResolvedMap) extends DurableShuffleRecoveryResolvedMap {
    override def numReducers: Int = delegate.numReducers
    override def dataLength: Long = delegate.dataLength
    override def indexBytes: Long = delegate.indexBytes

    override def blockMetadata(reduceId: Int): DurableShuffleRecoveryBlockMetadata =
      new ReferenceBlockMetadataAdapter(delegate.blockMetadata(reduceId))

    override def getBlockData(reduceId: Int): Option[ManagedBuffer] =
      delegate.getBlockData(reduceId)
  }

  private val normalizedRoot = providerRoot.toAbsolutePath.normalize()
  private val activeBindings = new ConcurrentHashMap[String, ActiveBinding]()

  val compatibilityId: String =
    ShuffleRecoveryFeasibilityIdentity.ProviderCompatibilityId

  override def capabilityDescriptor: DurableShuffleRecoveryCapabilityDescriptor =
    DurableShuffleRecoveryCapabilityDescriptor(
      compatibilityId,
      DurableShuffleRecoveryContract.ContractVersion,
      DurableShuffleRecoveryContract.ArtifactFormatVersion,
      DurableShuffleRecoveryContract.ReadVersion,
      Array(DurableShuffleRecoveryContract.ExactReducerRangeFetch),
      exactReducerAggregateStatistics = true,
      mapperLocalBlockMetadataQueryable = true,
      DurableShuffleRecoveryContract.ExactIndexSha256AndReducerChecksum,
      immutableIncarnations = true,
      conditionalRetirement = true,
      retirementRequiresRevision = false,
      DurableShuffleRecoveryContract.AttemptBindingIndependentArtifacts,
      DurableShuffleRecoveryContract.CurrentAttemptAuthorization,
      ShuffleRecoveryManifestCodec.MaxMaps,
      ShuffleRecoveryManifestCodec.MaxReducers,
      DurableShuffleRecoveryContract.MaxMetadataBytes)

  override def certifyWinningSelection(
      request: DurableShuffleRecoveryCertificationRequest):
      DurableShuffleRecoveryCertifiedSelection = {
    ShuffleRecoveryExternalCallGuard.assertAllowed(
      "shuffle recovery provider winner certification")
    validateCertificationRequest(request)
    val capabilities = DurableShuffleRecoveryContract.negotiate(
      capabilityDescriptor,
      compatibilityId,
      request.winningMapTaskIds.size,
      request.reducerCount) match {
      case Right(value) => value
      case Left(reason) => throw new IOException(reason)
    }
    val provider = ReferenceShuffleProvider.open(
      providerRoot,
      request.recoveryGroup,
      request.publishingGeneration,
      request.incarnationId,
      conf)
    val artifacts = ShuffleRecoveryWinningSelection.certify(
      provider,
      request.winningMapTaskIds,
      request.reducerCount)
    if (artifacts.size != request.winningMapTaskIds.size ||
        artifacts.zip(request.winningMapTaskIds).exists {
          case (artifact, taskId) => artifact.mapTaskId != taskId
        }) {
      throw new IOException("provider certification changed Spark's frozen winner selection")
    }
    DurableShuffleRecoveryCertifiedSelection(
      capabilities.providerCapabilityId,
      capabilities.contractVersion,
      capabilities.artifactFormatVersion,
      capabilities.readVersion,
      request.winningMapTaskIds,
      request.reducerCount,
      artifacts)
  }

  override def claim(request: ShuffleRecoveryClaimRequest): ShuffleRecoveryClaimResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider claim")
    validateRequest(request) match {
      case Some(reason) => return ShuffleRecoveryClaimRejected(reason)
      case None =>
    }
    val capabilities = DurableShuffleRecoveryContract.negotiate(
      capabilityDescriptor,
      request.providerCompatibilityId,
      request.mapperCount,
      request.reducerCount) match {
      case Right(value) => value
      case Left(reason) => return ShuffleRecoveryClaimRejected(reason)
    }

    val provider = openExistingProvider(request) match {
      case Right(value) => value
      case Left(result) => return result
    }

    val maps = new Array[ShuffleRecoveryClaimedMapDescriptor](request.mapperCount)
    val bytesByReducer = new Array[Long](request.reducerCount)
    var totalDataSize = 0L
    var mapIndex = 0
    while (mapIndex < request.mapperCount) {
      inspectMap(provider, request, request.mapArtifacts(mapIndex)) match {
        case Left(result) => return result
        case Right(inspected) =>
          maps(mapIndex) = inspected.descriptor
          try {
            totalDataSize = Math.addExact(totalDataSize, inspected.descriptor.dataLength)
            var reduceId = 0
            while (reduceId < request.reducerCount) {
              bytesByReducer(reduceId) = Math.addExact(
                bytesByReducer(reduceId), inspected.bytesByReducer(reduceId))
              reduceId += 1
            }
          } catch {
            case _: ArithmeticException => return ShuffleRecoveryClaimCorrupt
          }
      }
      mapIndex += 1
    }

    val binding = ShuffleRecoveryBinding(
      UUID.randomUUID().toString,
      request.targetShuffleId,
      request.recoveryGroup,
      request.publishingGeneration,
      request.incarnationId)
    val active = new ActiveBinding(binding, provider)
    if (activeBindings.putIfAbsent(binding.bindingId, active) != null) {
      return ShuffleRecoveryClaimUnavailable
    }
    ShuffleRecoveryClaimed(
      binding,
      ShuffleRecoveryClaimDescriptor(
        request.recoveryGroup,
        request.publishingGeneration,
        request.incarnationId,
        capabilities.providerCapabilityId,
        request.targetShuffleId,
        capabilities.readVersion,
        maps,
        ShuffleRecoveryClaimedStatistics(
          totalDataSize = Some(totalDataSize),
          bytesByReducer = Some(bytesByReducer),
          numOutputRows = None)))
  }

  override def release(binding: ShuffleRecoveryBinding): Unit = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider release")
    if (binding == null) {
      throw new IllegalArgumentException("shuffle recovery binding must not be null")
    }
    val active = activeBindings.get(binding.bindingId)
    if (active != null && active.binding == binding) {
      activeBindings.remove(binding.bindingId, active)
    }
  }

  override def finishAttempt(): Unit = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider attempt finish")
    activeBindings.clear()
  }

  override def finishGroup(
      authority: DurableShuffleRecoveryGroupLifecycleAuthority):
      DurableShuffleRecoveryGroupFinishResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider group finish")
    if (authority == null || !safeIdentifier(authority.recoveryGroup)) {
      return DurableShuffleRecoveryGroupFinishRefused
    }
    if (hasActiveBindingFor(authority.recoveryGroup, None)) {
      return DurableShuffleRecoveryGroupFinishRefused
    }
    val group = groupPath(authority.recoveryGroup)
    try {
      if (!Files.exists(group, LinkOption.NOFOLLOW_LINKS)) {
        DurableShuffleRecoveryGroupAlreadyAbsent
      } else if (!Files.isDirectory(group, LinkOption.NOFOLLOW_LINKS) ||
          Files.isSymbolicLink(group)) {
        DurableShuffleRecoveryGroupFinishRefused
      } else {
        ReferenceShuffleProvider.deleteRecursively(group)
        DurableShuffleRecoveryGroupFinished
      }
    } catch {
      case _: IOException => DurableShuffleRecoveryGroupFinishUnavailable
      case _: SecurityException => DurableShuffleRecoveryGroupFinishUnavailable
    }
  }

  override def retireExact(
      request: DurableShuffleRecoveryRetirementRequest):
      DurableShuffleRecoveryRetirementResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider exact retirement")
    if (!validRetirementRequest(request) ||
        hasActiveBindingFor(
          request.recoveryGroup,
          Some(request.publishingGeneration -> request.incarnationId))) {
      return DurableShuffleRecoveryArtifactRetirementRefused
    }
    val incarnation = incarnationPath(
      request.recoveryGroup,
      request.publishingGeneration,
      request.incarnationId)
    try {
      if (!Files.exists(incarnation, LinkOption.NOFOLLOW_LINKS)) {
        DurableShuffleRecoveryArtifactAlreadyAbsent
      } else if (!Files.isDirectory(incarnation, LinkOption.NOFOLLOW_LINKS) ||
          Files.isSymbolicLink(incarnation)) {
        DurableShuffleRecoveryArtifactRetirementRefused
      } else {
        ReferenceShuffleProvider.deleteRecursively(incarnation)
        DurableShuffleRecoveryArtifactRetired
      }
    } catch {
      case _: IOException => DurableShuffleRecoveryArtifactRetirementUnavailable
      case _: SecurityException => DurableShuffleRecoveryArtifactRetirementUnavailable
    }
  }

  /**
   * Runs the reference provider's optional abandoned-group TTL policy.
   *
   * This is provider-owned retention, not a Spark lease heartbeat. Active bindings in this driver
   * prevent local expiry; a real remote implementation is responsible for its own global policy.
   */
  private[shuffle] def expireAbandonedGroup(
      recoveryGroup: String,
      nowMillis: Long): Boolean = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery provider retention sweep")
    val ttl = retentionMillis.getOrElse(return false)
    if (!safeIdentifier(recoveryGroup) || nowMillis < 0L ||
        hasActiveBindingFor(recoveryGroup, None)) {
      return false
    }
    val group = groupPath(recoveryGroup)
    if (!Files.isDirectory(group, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(group)) {
      return false
    }
    val modified = Files.getLastModifiedTime(group, LinkOption.NOFOLLOW_LINKS).toMillis
    val age = try {
      Math.subtractExact(nowMillis, modified)
    } catch {
      case _: ArithmeticException => return false
    }
    if (age < ttl) {
      false
    } else {
      ReferenceShuffleProvider.deleteRecursively(group)
      true
    }
  }

  private[shuffle] def isBound(binding: ShuffleRecoveryBinding): Boolean = {
    if (binding == null) {
      false
    } else {
      val active = activeBindings.get(binding.bindingId)
      active != null && active.binding == binding
    }
  }

  private[shuffle] override def openBoundMap(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int): DurableShuffleRecoveryResolvedMap = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery bound map open")
    if (binding == null) {
      throw new IllegalArgumentException("shuffle recovery binding must not be null")
    }
    val active = activeBindings.get(binding.bindingId)
    if (active == null || active.binding != binding) {
      throw new IOException("shuffle recovery binding is not active")
    }
    new ReferenceResolvedMapAdapter(active.provider.openMap(mapIndex))
  }

  /**
   * Opens one map for an adopted fetch and classifies failure using the exact validated snapshot.
   *
   * This method runs on the shuffle-read path, never on the scheduler event loop. A generic I/O
   * failure is deliberately classified as unavailable unless a fresh examination proves that the
   * immutable winner, data, or exact index is missing or has changed.
   */
  private[shuffle] override def openBoundMapForFetch(
      binding: ShuffleRecoveryBinding,
      mapIndex: Int,
      expected: ShuffleRecoveryPreparedMap): ShuffleRecoveryBoundMapReadResult = {
    ShuffleRecoveryExternalCallGuard.assertAllowed("shuffle recovery adopted map fetch")
    if (binding == null || expected == null || mapIndex < 0 || expected.mapIndex != mapIndex) {
      return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }
    val active = activeBindings.get(binding.bindingId)
    if (active == null || active.binding != binding) {
      return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }

    validateRuntimeArtifact(active.provider, expected) match {
      case Some(failureClass) => return ShuffleRecoveryBoundMapFailed(failureClass)
      case None =>
    }

    val resolved = try {
      active.provider.openMap(mapIndex)
    } catch {
      case _: AccessDeniedException =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
      case _: NoSuchFileException =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedMissing)
      case _: IOException =>
        val failureClass = validateRuntimeArtifact(active.provider, expected)
          .getOrElse(ShuffleRecoveryAdoptedUnavailable)
        return ShuffleRecoveryBoundMapFailed(failureClass)
      case NonFatal(_) =>
        return ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedUnavailable)
    }

    if (resolved.numReducers <= 0 ||
        resolved.dataLength != expected.dataLength ||
        resolved.indexBytes != expected.indexLength) {
      ShuffleRecoveryBoundMapFailed(ShuffleRecoveryAdoptedCorrupt)
    } else {
      ShuffleRecoveryBoundMapOpened(new ReferenceResolvedMapAdapter(resolved))
    }
  }

  private def openExistingProvider(
      request: ShuffleRecoveryClaimRequest):
      Either[ShuffleRecoveryClaimResult, ReferenceShuffleProvider] = {
    val incarnationDirectory = incarnationPath(
      request.recoveryGroup,
      request.publishingGeneration,
      request.incarnationId)
    val requiredDirectories = Seq(
      incarnationDirectory,
      incarnationDirectory.resolve(".attempts"),
      incarnationDirectory.resolve("maps"))
    requiredDirectories.foreach { path =>
      validateExistingPath(path, expectDirectory = true) match {
        case Some(result) => return Left(result)
        case None =>
      }
    }
    try {
      Right(ReferenceShuffleProvider.open(
        providerRoot,
        request.recoveryGroup,
        request.publishingGeneration,
        request.incarnationId,
        conf))
    } catch {
      case _: AccessDeniedException => Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => Left(ShuffleRecoveryClaimMissing)
      case _: IOException => Left(ShuffleRecoveryClaimUnavailable)
      case NonFatal(_) => Left(ShuffleRecoveryClaimUnavailable)
    }
  }

  private def validateExistingPath(
      path: Path,
      expectDirectory: Boolean): Option[ShuffleRecoveryClaimResult] = {
    try {
      val attributes = Files.readAttributes(
        path,
        classOf[BasicFileAttributes],
        LinkOption.NOFOLLOW_LINKS)
      if (attributes.isSymbolicLink ||
          (expectDirectory && !attributes.isDirectory) ||
          (!expectDirectory && !attributes.isRegularFile)) {
        Some(ShuffleRecoveryClaimCorrupt)
      } else {
        None
      }
    } catch {
      case _: NoSuchFileException => Some(ShuffleRecoveryClaimMissing)
      case _: AccessDeniedException => Some(ShuffleRecoveryClaimUnavailable)
      case _: IOException => Some(ShuffleRecoveryClaimUnavailable)
      case NonFatal(_) => Some(ShuffleRecoveryClaimUnavailable)
    }
  }

  private def classifyValidatedReadFailure(
      mapDirectory: Path,
      requiredFiles: Seq[Path]): ShuffleRecoveryClaimResult = {
    validateExistingPath(mapDirectory, expectDirectory = true) match {
      case Some(result) => return result
      case None =>
    }
    var unavailable = false
    requiredFiles.foreach { path =>
      validateExistingPath(path, expectDirectory = false) match {
        case Some(ShuffleRecoveryClaimMissing) => return ShuffleRecoveryClaimMissing
        case Some(ShuffleRecoveryClaimCorrupt) => return ShuffleRecoveryClaimCorrupt
        case Some(ShuffleRecoveryClaimUnavailable) => unavailable = true
        case Some(_) => unavailable = true
        case None =>
      }
    }
    if (unavailable) ShuffleRecoveryClaimUnavailable else ShuffleRecoveryClaimCorrupt
  }

  private def validateRuntimeArtifact(
      provider: ReferenceShuffleProvider,
      expected: ShuffleRecoveryPreparedMap):
      Option[ShuffleRecoveryAdoptedReadFailureClass] = {
    val mapDirectory = provider.committedMapDirectory(expected.mapIndex)
    val winner = mapDirectory.resolveSibling(s"map-${expected.mapIndex}.winner")
    val ready = mapDirectory.resolve(ReferenceShuffleProvider.ReadyFileName)
    val data = mapDirectory.resolve(ReferenceShuffleProvider.DataFileName)
    val index = mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName)

    runtimePathFailure(mapDirectory, expectDirectory = true) match {
      case Some(failureClass) => return Some(failureClass)
      case None =>
    }
    Seq(winner, ready, data, index).foreach { path =>
      runtimePathFailure(path, expectDirectory = false) match {
        case Some(failureClass) => return Some(failureClass)
        case None =>
      }
    }

    try {
      val winnerSize = Files.size(winner)
      if (winnerSize <= 0L || winnerSize > MaxWinnerHandleBytes) {
        return Some(ShuffleRecoveryAdoptedCorrupt)
      }
      val winnerHandle = new String(Files.readAllBytes(winner), StandardCharsets.UTF_8).trim
      if (winnerHandle != expected.providerHandle ||
          Files.size(data) != expected.dataLength ||
          Files.size(index) != expected.indexLength ||
          Files.size(ready) != 1L) {
        return Some(ShuffleRecoveryAdoptedCorrupt)
      }
      val digest = toHex(digestExactIndex(index, expected.indexLength))
      if (digest != expected.exactIndexDigest) {
        Some(ShuffleRecoveryAdoptedCorrupt)
      } else {
        None
      }
    } catch {
      case _: NoSuchFileException => Some(ShuffleRecoveryAdoptedMissing)
      case _: AccessDeniedException => Some(ShuffleRecoveryAdoptedUnavailable)
      case _: IOException => Some(ShuffleRecoveryAdoptedUnavailable)
      case NonFatal(_) => Some(ShuffleRecoveryAdoptedUnavailable)
    }
  }

  private def runtimePathFailure(
      path: Path,
      expectDirectory: Boolean): Option[ShuffleRecoveryAdoptedReadFailureClass] = {
    validateExistingPath(path, expectDirectory) match {
      case Some(ShuffleRecoveryClaimMissing) => Some(ShuffleRecoveryAdoptedMissing)
      case Some(ShuffleRecoveryClaimCorrupt) => Some(ShuffleRecoveryAdoptedCorrupt)
      case Some(_) => Some(ShuffleRecoveryAdoptedUnavailable)
      case None => None
    }
  }

  private def validateCertificationRequest(
      request: DurableShuffleRecoveryCertificationRequest): Unit = {
    if (request == null || request.winningMapTaskIds == null ||
        !safeIdentifier(request.recoveryGroup) || !safeIdentifier(request.incarnationId)) {
      throw new IllegalArgumentException("winner certification request contains an invalid field")
    }
    if (request.publishingGeneration <= 0L ||
        request.winningMapTaskIds.size > ShuffleRecoveryManifestCodec.MaxMaps ||
        request.winningMapTaskIds.exists(_ < 0L) ||
        request.winningMapTaskIds.distinct.size != request.winningMapTaskIds.size) {
      throw new IllegalArgumentException("winner certification request has an invalid selection")
    }
    if (request.reducerCount <= 0 ||
        request.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      throw new IllegalArgumentException(
        "winner certification request has an invalid reducer shape")
    }
  }

  private def validateRequest(request: ShuffleRecoveryClaimRequest): Option[String] = {
    if (request == null || request.recoveryGroup == null || request.incarnationId == null ||
        request.providerCompatibilityId == null || request.mapArtifacts == null) {
      Some("claim request contains a null field")
    } else if (!safeIdentifier(request.recoveryGroup) ||
        !safeIdentifier(request.incarnationId)) {
      Some("claim request contains an invalid provider namespace")
    } else if (request.publishingGeneration <= 0L || request.targetShuffleId < 0) {
      Some("claim request contains an invalid generation or target shuffle id")
    } else if (request.mapperCount < 0 ||
        request.mapperCount > ShuffleRecoveryManifestCodec.MaxMaps ||
        request.mapArtifacts.size != request.mapperCount) {
      Some("claim request contains an invalid mapper shape")
    } else if (request.reducerCount <= 0 ||
        request.reducerCount > ShuffleRecoveryManifestCodec.MaxReducers) {
      Some("claim request contains an invalid reducer shape")
    } else {
      None
    }
  }

  private def validRetirementRequest(
      request: DurableShuffleRecoveryRetirementRequest): Boolean = {
    request != null &&
      request.examinedRevision != null &&
      safeIdentifier(request.recoveryGroup) &&
      safeIdentifier(request.incarnationId) &&
      request.publishingGeneration > 0L &&
      request.examinedRevision.forall { revision =>
        revision != null && revision.nonEmpty &&
          revision.getBytes(StandardCharsets.UTF_8).length <=
            DurableShuffleRecoveryContract.MaxCapabilityStringBytes
      }
  }

  private def safeIdentifier(value: String): Boolean = {
    try {
      ShuffleRecoveryManifestCodec.validateIdentifier(value, "provider recovery identifier")
      true
    } catch {
      case NonFatal(_) => false
    }
  }

  private def hasActiveBindingFor(
      recoveryGroup: String,
      exact: Option[(Long, String)]): Boolean = {
    activeBindings.values().asScala.exists { active =>
      active.binding.recoveryGroup == recoveryGroup && exact.forall {
        case (generation, incarnationId) =>
          active.binding.publishingGeneration == generation &&
            active.binding.incarnationId == incarnationId
      }
    }
  }

  private def groupPath(recoveryGroup: String): Path = {
    val encoded = Base64.getUrlEncoder.withoutPadding().encodeToString(
      recoveryGroup.getBytes(StandardCharsets.UTF_8))
    val group = normalizedRoot.resolve(encoded).normalize()
    if (!group.startsWith(normalizedRoot)) {
      throw new IllegalArgumentException("provider recovery group escapes configured root")
    }
    group
  }

  private def incarnationPath(
      recoveryGroup: String,
      publishingGeneration: Long,
      incarnationId: String): Path = {
    val encodedIncarnation = Base64.getUrlEncoder.withoutPadding().encodeToString(
      incarnationId.getBytes(StandardCharsets.UTF_8))
    val incarnation = groupPath(recoveryGroup)
      .resolve(publishingGeneration.toString)
      .resolve(encodedIncarnation)
      .normalize()
    if (!incarnation.startsWith(normalizedRoot)) {
      throw new IllegalArgumentException("provider incarnation escapes configured root")
    }
    incarnation
  }

  private def inspectMap(
      provider: ReferenceShuffleProvider,
      request: ShuffleRecoveryClaimRequest,
      artifact: ShuffleRecoveryMapArtifact):
      Either[ShuffleRecoveryClaimResult, InspectedMap] = {
    if (artifact == null || artifact.mapIndex < 0 || artifact.mapIndex >= request.mapperCount) {
      return Left(ShuffleRecoveryClaimRejected("claim request contains an invalid map artifact"))
    }
    val mapDirectory = provider.committedMapDirectory(artifact.mapIndex)
    val winner = mapDirectory.resolveSibling(s"map-${artifact.mapIndex}.winner")
    validateExistingPath(mapDirectory, expectDirectory = true) match {
      case Some(result) => return Left(result)
      case None =>
    }

    val requiredFiles = Seq(
      winner,
      mapDirectory.resolve(ReferenceShuffleProvider.ReadyFileName),
      mapDirectory.resolve(ReferenceShuffleProvider.DataFileName),
      mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName))
    requiredFiles.foreach { path =>
      validateExistingPath(path, expectDirectory = false) match {
        case Some(result) => return Left(result)
        case None =>
      }
    }

    val winnerHandle = try {
      if (Files.size(winner) <= 0L || Files.size(winner) > MaxWinnerHandleBytes) {
        return Left(ShuffleRecoveryClaimCorrupt)
      }
      new String(Files.readAllBytes(winner), StandardCharsets.UTF_8).trim
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException => return Left(ShuffleRecoveryClaimUnavailable)
    }
    if (winnerHandle != artifact.providerHandle) {
      return Left(ShuffleRecoveryClaimRejected(
        "manifest provider handle does not match the committed winner"))
    }

    val resolved = try {
      provider.openMap(artifact.mapIndex)
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException =>
        return Left(classifyValidatedReadFailure(mapDirectory, requiredFiles))
    }
    if (resolved.numReducers != request.reducerCount ||
        resolved.dataLength != artifact.dataLength ||
        resolved.indexBytes != artifact.indexLength) {
      return Left(ShuffleRecoveryClaimRejected(
        "manifest artifact shape does not match immutable provider bytes"))
    }

    val reducerBytes = new Array[Long](request.reducerCount)
    var emptyBlocks = 0
    var nonEmptyBlocks = 0
    var physicalBlockBytes = 0L
    var reduceId = 0
    try {
      while (reduceId < request.reducerCount) {
        val block = resolved.blockMetadata(reduceId)
        if (block.offset < 0L || block.length < 0L ||
            block.offset > resolved.dataLength ||
            block.length > resolved.dataLength - block.offset) {
          return Left(ShuffleRecoveryClaimCorrupt)
        }
        reducerBytes(reduceId) = block.length
        if (block.isEmpty) {
          emptyBlocks = Math.addExact(emptyBlocks, 1)
        } else {
          nonEmptyBlocks = Math.addExact(nonEmptyBlocks, 1)
          physicalBlockBytes = Math.addExact(physicalBlockBytes, block.length)
        }
        reduceId += 1
      }
    } catch {
      case _: ArithmeticException => return Left(ShuffleRecoveryClaimCorrupt)
      case _: IllegalArgumentException => return Left(ShuffleRecoveryClaimCorrupt)
    }
    if (physicalBlockBytes != resolved.dataLength) {
      return Left(ShuffleRecoveryClaimCorrupt)
    }

    val indexPath = mapDirectory.resolve(ReferenceShuffleProvider.IndexFileName)
    val digest = try {
      digestExactIndex(indexPath, resolved.indexBytes)
    } catch {
      case _: AccessDeniedException => return Left(ShuffleRecoveryClaimUnavailable)
      case _: NoSuchFileException => return Left(ShuffleRecoveryClaimMissing)
      case _: IOException =>
        return Left(classifyValidatedReadFailure(mapDirectory, requiredFiles))
    }
    Right(InspectedMap(
      ShuffleRecoveryClaimedMapDescriptor(
        artifact.mapIndex,
        winnerHandle,
        resolved.numReducers,
        resolved.dataLength,
        resolved.indexBytes,
        digest,
        emptyBlocks,
        nonEmptyBlocks,
        physicalBlockBytes),
      reducerBytes))
  }

  private def digestExactIndex(indexPath: Path, expectedLength: Long): Array[Byte] = {
    if (!Files.isRegularFile(indexPath, LinkOption.NOFOLLOW_LINKS) ||
        Files.isSymbolicLink(indexPath)) {
      throw new IOException("shuffle recovery index is not a safe regular file")
    }
    val actualLength = Files.size(indexPath)
    if (actualLength != expectedLength || actualLength <= 0L ||
        actualLength > MaxExactIndexBytes) {
      throw new IOException("shuffle recovery index length is outside the safe bound")
    }
    val digest = MessageDigest.getInstance("SHA-256")
    val in = new BufferedInputStream(Files.newInputStream(indexPath, LinkOption.NOFOLLOW_LINKS))
    try {
      val buffer = new Array[Byte](8192)
      var read = in.read(buffer)
      while (read >= 0) {
        if (read > 0) {
          digest.update(buffer, 0, read)
        }
        read = in.read(buffer)
      }
    } finally {
      in.close()
    }
    digest.digest()
  }

  private def toHex(bytes: Array[Byte]): String = {
    val builder = new StringBuilder(bytes.length * 2)
    bytes.foreach { value =>
      builder.append(f"${value & 0xff}%02x")
    }
    builder.result()
  }
}

private[shuffle] object ReferenceShuffleRecoveryClaimProvider {
  private val MaxWinnerHandleBytes = 256L
  private val MaxExactIndexBytes = 64L * 1024L * 1024L
}
