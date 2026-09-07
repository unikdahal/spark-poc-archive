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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;

/**
 * Isolated Celeborn client probe used to determine whether a new client process can discover and
 * read a completed shuffle after the producer process and its LifecycleManager have disappeared.
 *
 * <p>The producer intentionally publishes only a small evidence file containing non-secret artifact
 * facts. The reader creates a new LifecycleManager and does not receive the producer endpoint,
 * client object, committed file-group metadata, or authentication secret.
 */
public final class ColdDriverProviderProbe {
  private static final int SHUFFLE_ID = 1;
  private static final int MAP_ID = 0;
  private static final int ATTEMPT_ID = 0;
  private static final int PARTITION_ID = 0;
  private static final byte[] PAYLOAD =
      "celeborn-cold-driver-provider-proof-v0.6.3".getBytes(StandardCharsets.UTF_8);

  private ColdDriverProviderProbe() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
          "usage: ColdDriverProviderProbe <producer|reader> <master-endpoint> <app-id> <state-file>");
    }
    String mode = args[0];
    String masterEndpoint = args[1];
    String appId = args[2];
    Path stateFile = Path.of(args[3]);

    if ("producer".equals(mode)) {
      runProducer(masterEndpoint, appId, stateFile);
    } else if ("reader".equals(mode)) {
      runReader(masterEndpoint, appId, stateFile);
    } else {
      throw new IllegalArgumentException("unknown mode: " + mode);
    }
  }

  private static void runProducer(String masterEndpoint, String appId, Path stateFile)
      throws Exception {
    CelebornConf conf = clientConf(masterEndpoint);
    LifecycleManager lifecycleManager = new LifecycleManager(appId, conf);
    ShuffleClient client =
        ShuffleClient.get(
            appId,
            lifecycleManager.getHost(),
            lifecycleManager.getPort(),
            conf,
            new UserIdentifier("shuffle-recovery", "provider-probe"));

    int pushed =
        client.pushData(
            SHUFFLE_ID,
            MAP_ID,
            ATTEMPT_ID,
            PARTITION_ID,
            PAYLOAD,
            0,
            PAYLOAD.length,
            1,
            1);
    client.mapperEnd(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, 1, 1);

    ReadCounter controlCounter = new ReadCounter();
    byte[] controlRead = readAll(client, controlCounter);
    requireSamePayload(controlRead, "producer control read");

    Properties state = new Properties();
    state.setProperty("appId", appId);
    state.setProperty("shuffleId", Integer.toString(SHUFFLE_ID));
    state.setProperty("partitionId", Integer.toString(PARTITION_ID));
    state.setProperty("payloadBytes", Integer.toString(PAYLOAD.length));
    state.setProperty("payloadSha256", sha256(PAYLOAD));
    state.setProperty("producerControlReadBytes", Long.toString(controlCounter.bytesRead()));
    state.setProperty(
        "producerLifecycleEndpoint", lifecycleManager.getHost() + ":" + lifecycleManager.getPort());
    try (FileOutputStream out = new FileOutputStream(stateFile.toFile())) {
      state.store(out, "Celeborn provider probe - contains no credential material");
    }

    System.out.printf(
        "PRODUCER_READY pid=%d app=%s shuffle=%d partition=%d pushed=%d controlRead=%d "
            + "payloadSha256=%s lifecycleEndpoint=%s:%d%n",
        ProcessHandle.current().pid(),
        appId,
        SHUFFLE_ID,
        PARTITION_ID,
        pushed,
        controlCounter.bytesRead(),
        sha256(PAYLOAD),
        lifecycleManager.getHost(),
        lifecycleManager.getPort());
    System.out.flush();

    // The harness kills this process with SIGKILL. Do not run LifecycleManager shutdown: the exact
    // condition under test is loss of the producer driver without an orderly application finish.
    while (true) {
      Thread.sleep(60_000L);
    }
  }

  private static void runReader(String masterEndpoint, String appId, Path stateFile)
      throws Exception {
    Properties state = new Properties();
    try (FileInputStream in = new FileInputStream(stateFile.toFile())) {
      state.load(in);
    }
    if (!appId.equals(state.getProperty("appId"))) {
      throw new IllegalStateException("reader app id does not match the producer evidence");
    }
    if (!sha256(PAYLOAD).equals(state.getProperty("payloadSha256"))) {
      throw new IllegalStateException("producer evidence payload digest is not the expected probe payload");
    }

    CelebornConf conf = clientConf(masterEndpoint);
    LifecycleManager lifecycleManager = new LifecycleManager(appId, conf);
    System.out.printf(
        "READER_STARTED pid=%d app=%s lifecycleEndpoint=%s:%d producerEndpoint=%s%n",
        ProcessHandle.current().pid(),
        appId,
        lifecycleManager.getHost(),
        lifecycleManager.getPort(),
        state.getProperty("producerLifecycleEndpoint"));
    System.out.flush();

    ShuffleClient client =
        ShuffleClient.get(
            appId,
            lifecycleManager.getHost(),
            lifecycleManager.getPort(),
            conf,
            new UserIdentifier("shuffle-recovery", "provider-probe"));
    ReadCounter coldCounter = new ReadCounter();
    byte[] coldRead;
    try {
      coldRead = readAll(client, coldCounter);
    } catch (Exception failure) {
      System.out.printf(
          "EXPECTED_DISCOVERY_GAP failureType=%s bytesRead=%d message=%s%n",
          failure.getClass().getName(),
          coldCounter.bytesRead(),
          oneLine(failure.getMessage()));
      System.out.flush();
      orderlyReaderShutdown(client, lifecycleManager);
      return;
    }

    if (coldRead.length == 0) {
      System.out.printf("EXPECTED_DISCOVERY_GAP_EMPTY_READ bytesRead=%d%n", coldCounter.bytesRead());
      System.out.flush();
      orderlyReaderShutdown(client, lifecycleManager);
      return;
    }

    if (!MessageDigest.isEqual(PAYLOAD, coldRead)) {
      System.err.printf(
          "UNEXPECTED_COLD_READ_WRONG_BYTES bytes=%d metricsBytes=%d payloadSha256=%s%n",
          coldRead.length,
          coldCounter.bytesRead(),
          sha256(coldRead));
      orderlyReaderShutdown(client, lifecycleManager);
      System.exit(4);
    }

    System.err.printf(
        "UNEXPECTED_COLD_READ_SUCCESS bytes=%d metricsBytes=%d payloadSha256=%s%n",
        coldRead.length,
        coldCounter.bytesRead(),
        sha256(coldRead));
    orderlyReaderShutdown(client, lifecycleManager);
    System.exit(3);
  }

  private static void orderlyReaderShutdown(
      ShuffleClient client, LifecycleManager lifecycleManager) throws Exception {
    client.shutdown();
    lifecycleManager.rpcEnv().shutdown();
    lifecycleManager.rpcEnv().awaitTermination();
    System.out.println("READER_SHUTDOWN=ORDERLY_CLIENT_AND_LIFECYCLE_MANAGER");
    System.out.flush();
  }

  private static CelebornConf clientConf(String masterEndpoint) {
    return new CelebornConf()
        .set("celeborn.master.endpoints", masterEndpoint)
        .set("celeborn.client.application.unregister.enabled", "true")
        .set("celeborn.client.push.replicate.enabled", "false");
  }

  private static byte[] readAll(ShuffleClient client, ReadCounter counter) throws Exception {
    try (InputStream in =
        client.readPartition(
            SHUFFLE_ID,
            PARTITION_ID,
            0,
            0L,
            0,
            Integer.MAX_VALUE,
            counter)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static void requireSamePayload(byte[] actual, String operation) {
    if (!MessageDigest.isEqual(PAYLOAD, actual)) {
      throw new IllegalStateException(
          operation
              + " returned unexpected bytes: expected="
              + sha256(PAYLOAD)
              + " actual="
              + sha256(actual));
    }
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String oneLine(String message) {
    if (message == null) {
      return "<none>";
    }
    return message.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ").trim();
  }

  private static final class ReadCounter implements MetricsCallback {
    private final AtomicLong bytesRead = new AtomicLong();

    @Override
    public void incBytesRead(long bytes) {
      bytesRead.addAndGet(bytes);
    }

    @Override
    public void incReadTime(long time) {}

    private long bytesRead() {
      return bytesRead.get();
    }
  }
}
