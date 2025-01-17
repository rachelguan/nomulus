// Copyright 2020 The Nomulus Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package google.registry.rde;

import static com.google.common.truth.Truth.assertThat;
import static google.registry.model.ofy.ObjectifyService.auditedOfy;
import static google.registry.model.rde.RdeMode.FULL;
import static google.registry.model.rde.RdeMode.THIN;
import static google.registry.persistence.transaction.TransactionManagerFactory.tm;
import static google.registry.testing.DatabaseHelper.createTld;
import static google.registry.util.ResourceUtils.readResourceUtf8;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.ImmutableList;
import google.registry.beam.rde.RdePipelineTest;
import google.registry.gcs.GcsUtils;
import google.registry.keyring.api.PgpHelper;
import google.registry.model.common.Cursor;
import google.registry.model.common.Cursor.CursorType;
import google.registry.model.rde.RdeMode;
import google.registry.model.rde.RdeRevision;
import google.registry.model.tld.Registry;
import google.registry.request.RequestParameters;
import google.registry.testing.AppEngineExtension;
import google.registry.testing.CloudTasksHelper;
import google.registry.testing.CloudTasksHelper.TaskMatcher;
import google.registry.testing.FakeKeyringModule;
import google.registry.testing.FakeLockHandler;
import google.registry.xml.ValidationMode;
import java.io.IOException;
import java.util.Iterator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for {@link RdeStagingReducer}. */
class RdeStagingReducerTest {

  @RegisterExtension
  AppEngineExtension appEngineExtension =
      AppEngineExtension.builder().withDatastoreAndCloudSql().withTaskQueue().build();

  private static final String GCS_BUCKET = "test-rde-bucket";
  private static final GcsUtils gcsUtils = new GcsUtils(LocalStorageHelper.getOptions());
  private static final PGPPrivateKey decryptionKey =
      new FakeKeyringModule().get().getRdeStagingDecryptionKey();
  private static final PGPPublicKey encryptionKey =
      new FakeKeyringModule().get().getRdeStagingEncryptionKey();
  private static final DateTime now = DateTime.parse("2000-01-01TZ");
  private final CloudTasksHelper cloudTasksHelper = new CloudTasksHelper();

  private Fragments brdaFragments =
      new Fragments(
          ImmutableList.of(
              DepositFragment.create(RdeResourceType.DOMAIN, "<rdeDomain:domain/>\n", ""),
              DepositFragment.create(
                  RdeResourceType.REGISTRAR, "<rdeRegistrar:registrar/>\n", "")));

  private Fragments rdeFragments =
      new Fragments(
          ImmutableList.of(
              DepositFragment.create(RdeResourceType.DOMAIN, "<rdeDomain:domain/>\n", ""),
              DepositFragment.create(RdeResourceType.REGISTRAR, "<rdeRegistrar:registrar/>\n", ""),
              DepositFragment.create(RdeResourceType.CONTACT, "<rdeContact:contact/>\n", ""),
              DepositFragment.create(RdeResourceType.HOST, "<rdeHost:host/>\n", "")));

  private PendingDeposit key;

  private RdeStagingReducer reducer =
      new RdeStagingReducer(
          cloudTasksHelper.getTestCloudTasksUtils(),
          new FakeLockHandler(true),
          GCS_BUCKET,
          Duration.ZERO,
          PgpHelper.convertPublicKeyToBytes(encryptionKey),
          ValidationMode.STRICT,
          gcsUtils);

  @BeforeEach
  void beforeEach() {
    createTld("soy");
    tm().transact(
            () -> {
              tm().put(Cursor.create(CursorType.BRDA, now, Registry.get("soy")));
              tm().put(Cursor.create(CursorType.RDE_STAGING, now, Registry.get("soy")));
              RdeRevision.saveRevision("soy", now, THIN, 0);
              RdeRevision.saveRevision("soy", now, FULL, 0);
            });
  }

  @Test
  void testSuccess_BRDA() throws Exception {
    key = PendingDeposit.create("soy", now, THIN, CursorType.BRDA, Duration.standardDays(1));
    reducer.reduce(key, brdaFragments);
    String outputFile = decryptGhostrydeGcsFile("soy_2000-01-01_thin_S1_R1.xml.ghostryde");
    assertThat(outputFile)
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_brda.xml")
                .replace("%RESEND%", " resend=\"1\""));
    compareLength(outputFile, "soy_2000-01-01_thin_S1_R1.xml.length");
    // BRDA doesn't write a report file.
    assertThrows(
        StorageException.class,
        () ->
            gcsUtils.readBytesFrom(
                BlobId.of(GCS_BUCKET, "soy_2000-01-01_thin_S1_R1-report.xml.ghostryde")));
    assertThat(loadCursorTime(CursorType.BRDA))
        .isEquivalentAccordingToCompareTo(now.plus(Duration.standardDays(1)));
    assertThat(loadRevision(THIN)).isEqualTo(1);
    cloudTasksHelper.assertTasksEnqueued(
        "brda",
        new TaskMatcher()
            .url(BrdaCopyAction.PATH)
            .param(RequestParameters.PARAM_TLD, "soy")
            .param(RdeModule.PARAM_WATERMARK, now.toString()));
  }

  @Test
  void testSuccess_BRDA_manual() throws Exception {
    key = PendingDeposit.createInManualOperation("soy", now, THIN, "", 0);
    reducer.reduce(key, brdaFragments);
    String outputFile = decryptGhostrydeGcsFile("manual/soy_2000-01-01_thin_S1_R0.xml.ghostryde");
    assertThat(outputFile)
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_brda.xml").replace("%RESEND%", ""));
    compareLength(outputFile, "manual/soy_2000-01-01_thin_S1_R0.xml.length");
    // BRDA doesn't write a report file.
    assertThrows(
        StorageException.class,
        () ->
            gcsUtils.readBytesFrom(
                BlobId.of(GCS_BUCKET, "manual/soy_2000-01-01_thin_S1_R0-report.xml.ghostryde")));
    // No extra operations in manual mode.
    assertThat(loadCursorTime(CursorType.BRDA)).isEquivalentAccordingToCompareTo(now);
    assertThat(loadRevision(THIN)).isEqualTo(0);
    cloudTasksHelper.assertNoTasksEnqueued("brda");
  }

  @Test
  void testSuccess_RDE() throws Exception {
    key = PendingDeposit.create("soy", now, FULL, CursorType.RDE_STAGING, Duration.standardDays(1));
    reducer.reduce(key, rdeFragments);
    String outputFile = decryptGhostrydeGcsFile("soy_2000-01-01_full_S1_R1.xml.ghostryde");
    assertThat(outputFile)
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_rde.xml")
                .replace("%RESEND%", " resend=\"1\""));
    compareLength(outputFile, "soy_2000-01-01_full_S1_R1.xml.length");
    assertThat(decryptGhostrydeGcsFile("soy_2000-01-01_full_S1_R1-report.xml.ghostryde"))
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_rde_report.xml")
                .replace("%RESEND%", "1"));
    assertThat(loadCursorTime(CursorType.RDE_STAGING))
        .isEquivalentAccordingToCompareTo(now.plus(Duration.standardDays(1)));
    assertThat(loadRevision(FULL)).isEqualTo(1);
    cloudTasksHelper.assertTasksEnqueued(
        "rde-upload",
        new TaskMatcher().url(RdeUploadAction.PATH).param(RequestParameters.PARAM_TLD, "soy"));
  }

  @Test
  void testSuccess_RDE_manual() throws Exception {
    key = PendingDeposit.createInManualOperation("soy", now, FULL, "", 0);
    reducer.reduce(key, rdeFragments);
    String outputFile = decryptGhostrydeGcsFile("manual/soy_2000-01-01_full_S1_R0.xml.ghostryde");
    assertThat(outputFile)
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_rde.xml").replace("%RESEND%", ""));
    compareLength(outputFile, "manual/soy_2000-01-01_full_S1_R0.xml.length");
    assertThat(decryptGhostrydeGcsFile("manual/soy_2000-01-01_full_S1_R0-report.xml.ghostryde"))
        .isEqualTo(
            readResourceUtf8(RdePipelineTest.class, "reducer_rde_report.xml")
                .replace("%RESEND%", "0"));
    // No extra operations in manual mode.
    assertThat(loadCursorTime(CursorType.RDE_STAGING)).isEquivalentAccordingToCompareTo(now);
    assertThat(loadRevision(FULL)).isEqualTo(0);
    cloudTasksHelper.assertNoTasksEnqueued("rde-upload");
  }

  private static void compareLength(String outputFile, String lengthFilename) throws IOException {
    assertThat(String.valueOf(outputFile.getBytes(UTF_8).length))
        .isEqualTo(
            new String(gcsUtils.readBytesFrom(BlobId.of(GCS_BUCKET, lengthFilename)), UTF_8));
  }

  private static DateTime loadCursorTime(CursorType type) {
    return auditedOfy()
        .load()
        .key(Cursor.createKey(type, Registry.get("soy")))
        .now()
        .getCursorTime();
  }

  private static int loadRevision(RdeMode mode) {
    return auditedOfy()
        .load()
        .type(RdeRevision.class)
        .id("soy_2000-01-01_" + mode.getFilenameComponent())
        .now()
        .getRevision();
  }

  private static String decryptGhostrydeGcsFile(String filename) throws IOException, PGPException {
    return new String(
        Ghostryde.decode(gcsUtils.readBytesFrom(BlobId.of(GCS_BUCKET, filename)), decryptionKey),
        UTF_8);
  }

  private static class Fragments extends ReducerInput<DepositFragment> {
    private final Iterator<DepositFragment> iterator;

    Fragments(Iterable<DepositFragment> iterable) {
      this.iterator = iterable.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public DepositFragment next() {
      return iterator.next();
    }
  }
}
