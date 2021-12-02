// Copyright 2018 The Nomulus Authors. All Rights Reserved.
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

package google.registry.export;

import static com.google.common.truth.Truth.assertThat;
import static google.registry.export.CheckBackupAction.CHECK_BACKUP_KINDS_TO_LOAD_PARAM;
import static google.registry.export.CheckBackupAction.CHECK_BACKUP_NAME_PARAM;
import static google.registry.export.UploadDatastoreBackupAction.QUEUE;
import static google.registry.export.UploadDatastoreBackupAction.UPLOAD_BACKUP_FOLDER_PARAM;
import static google.registry.export.UploadDatastoreBackupAction.UPLOAD_BACKUP_ID_PARAM;
import static google.registry.export.UploadDatastoreBackupAction.UPLOAD_BACKUP_KINDS_PARAM;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.common.collect.ImmutableSet;
import google.registry.export.datastore.DatastoreAdmin;
import google.registry.export.datastore.DatastoreAdmin.Get;
import google.registry.export.datastore.Operation;
import google.registry.request.Action.Method;
import google.registry.request.HttpException.BadRequestException;
import google.registry.request.HttpException.NoContentException;
import google.registry.request.HttpException.NotModifiedException;
import google.registry.testing.AppEngineExtension;
import google.registry.testing.CloudTasksHelper;
import google.registry.testing.CloudTasksHelper.TaskMatcher;
import google.registry.testing.FakeClock;
import google.registry.testing.FakeResponse;
import google.registry.testing.TestDataHelper;
import google.registry.util.CloudTasksUtils;
import google.registry.util.CloudTasksUtils.TaskInfo;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** Unit tests for {@link CheckBackupAction}. */
@ExtendWith(MockitoExtension.class)
public class CheckBackupActionTest {

  private static final DateTime START_TIME = DateTime.parse("2014-08-01T01:02:03Z");
  private static final DateTime COMPLETE_TIME = START_TIME.plus(Duration.standardMinutes(30));

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @RegisterExtension
  public final AppEngineExtension appEngine = AppEngineExtension.builder().withTaskQueue().build();

  @Mock private DatastoreAdmin datastoreAdmin;
  @Mock private Get getNotFoundBackupProgressRequest;
  @Mock private Get getBackupProgressRequest;
  private Operation backupOperation;

  private final FakeResponse response = new FakeResponse();
  private final FakeClock clock = new FakeClock(COMPLETE_TIME.plusMillis(1000));
  private final CheckBackupAction action = new CheckBackupAction();
  private final CloudTasksHelper cloudTasksHelper = new CloudTasksHelper();
  @BeforeEach
  void beforeEach() throws Exception {
    action.requestMethod = Method.POST;
    action.datastoreAdmin = datastoreAdmin;
    action.clock = clock;
    action.cloudTasksUtils = cloudTasksHelper.getTestCloudTasksUtils();
    action.backupName = "some_backup";
    action.kindsToLoadParam = "one,two";
    action.response = response;

    when(datastoreAdmin.get(anyString())).thenReturn(getBackupProgressRequest);
    when(getBackupProgressRequest.execute()).thenAnswer(arg -> backupOperation);
  }

  private void setPendingBackup() throws Exception {
    backupOperation =
        JSON_FACTORY.fromString(
            TestDataHelper.loadFile(
                CheckBackupActionTest.class, "backup_operation_in_progress.json"),
            Operation.class);
  }

  private void setCompleteBackup() throws Exception {
    backupOperation =
        JSON_FACTORY.fromString(
            TestDataHelper.loadFile(CheckBackupActionTest.class, "backup_operation_success.json"),
            Operation.class);
  }

  private void setBackupNotFound() throws Exception {
    when(datastoreAdmin.get(anyString())).thenReturn(getNotFoundBackupProgressRequest);
    when(getNotFoundBackupProgressRequest.execute())
        .thenThrow(
            new GoogleJsonResponseException(
                new GoogleJsonResponseException.Builder(404, "NOT_FOUND", new HttpHeaders())
                    .setMessage("No backup found"),
                null));
  }

  private void assertLoadTaskEnqueued(String id, String folder, String kinds) {
    cloudTasksHelper.assertTasksEnqueued(
        BackupDatastoreAction.QUEUE,
        new TaskMatcher()
            .url("/_dr/task/uploadDatastoreBackup")
            .method(HttpMethod.POST)
            .param("id", id)
            .param("folder", folder)
            .param("kinds", kinds));
  }

  @MockitoSettings(strictness = Strictness.LENIENT)
  @Test
  void testSuccess_enqueuePollTask() {
    TaskInfo taskInfo =action.pollTaskInfo("some_backup_name", ImmutableSet.of("one", "two", "three"), Optional.of(0));
    action.cloudTasksUtils.enqueue(taskInfo.queueName(), CloudTasksUtils.createPostTask(taskInfo.path(), "backend", taskInfo.param(), clock, taskInfo.jitterSeconds()));
    cloudTasksHelper.assertTasksEnqueued(
        CheckBackupAction.QUEUE,
        new TaskMatcher()
            .url(CheckBackupAction.PATH)
            .param(CHECK_BACKUP_NAME_PARAM, "some_backup_name")
            .param(CHECK_BACKUP_KINDS_TO_LOAD_PARAM, "one,two,three")
            .method(HttpMethod.POST));
  }

  @Test
  void testPost_forPendingBackup_returnsNotModified() throws Exception {
    setPendingBackup();

    NotModifiedException thrown = assertThrows(NotModifiedException.class, action::run);
    assertThat(thrown)
        .hasMessageThat()
        .contains("Datastore backup some_backup still in progress: Progress: N/A");
  }

  @Test
  void testPost_forStalePendingBackupBackup_returnsNoContent() throws Exception {
    setPendingBackup();
    clock.setTo(
        START_TIME
            .plus(Duration.standardHours(20))
            .plus(Duration.standardMinutes(3))
            .plus(Duration.millis(1234)));

    NoContentException thrown = assertThrows(NoContentException.class, action::run);
    assertThat(thrown)
        .hasMessageThat()
        .contains(
            "Datastore backup some_backup abandoned - "
                + "not complete after 20 hours, 3 minutes and 1 second. Progress: Progress: N/A");
  }

  @Test
  void testPost_forCompleteBackup_enqueuesLoadTask() throws Exception {
    setCompleteBackup();
    action.run();
    assertLoadTaskEnqueued(
        "2014-08-01T01:02:03_99364",
        "gs://registry-project-id-datastore-export-test/2014-08-01T01:02:03_99364",
        "one,two");
  }

  @Test
  void testPost_forCompleteBackup_withExtraKindsToLoad_enqueuesLoadTask() throws Exception {
    setCompleteBackup();
    action.kindsToLoadParam = "one,foo";

    action.run();
    assertLoadTaskEnqueued(
        "2014-08-01T01:02:03_99364",
        "gs://registry-project-id-datastore-export-test/2014-08-01T01:02:03_99364",
        "one");
  }

  @Test
  void testPost_forCompleteBackup_withEmptyKindsToLoad_skipsLoadTask() throws Exception {
    setCompleteBackup();
    action.kindsToLoadParam = "";

    action.run();
    cloudTasksHelper.assertNoTasksEnqueued(BackupDatastoreAction.QUEUE);
  }

  @MockitoSettings(strictness = Strictness.LENIENT)
  @Test
  void testPost_forBadBackup_returnsBadRequest() throws Exception {
    setBackupNotFound();

    BadRequestException thrown = assertThrows(BadRequestException.class, action::run);
    assertThat(thrown).hasMessageThat().contains("Bad backup name some_backup: No backup found");
  }

  @Test
  void testGet_returnsInformation() throws Exception {
    setCompleteBackup();
    action.requestMethod = Method.GET;

    action.run();
    assertThat(response.getPayload())
        .isEqualTo(
            TestDataHelper.loadFile(
                    CheckBackupActionTest.class, "pretty_printed_success_backup_operation.json")
                .trim());
  }

  @MockitoSettings(strictness = Strictness.LENIENT)
  @Test
  void testGet_forBadBackup_returnsError() throws Exception {
    setBackupNotFound();
    action.requestMethod = Method.GET;

    BadRequestException thrown = assertThrows(BadRequestException.class, action::run);
    assertThat(thrown).hasMessageThat().contains("Bad backup name some_backup: No backup found");
  }

  @MockitoSettings(strictness = Strictness.LENIENT)
  @Test
  void testSuccess_enqueueLoadTask() {
    action.enqueueUploadBackupTask(UploadDatastoreBackupAction.QUEUE, UploadDatastoreBackupAction.PATH, "id12345", "gs://bucket/path", ImmutableSet.of("one", "two", "three"));
    cloudTasksHelper.assertTasksEnqueued(
        UploadDatastoreBackupAction.QUEUE,
        new TaskMatcher()
            .url(UploadDatastoreBackupAction.PATH)
            .method(HttpMethod.POST)
            .param(UPLOAD_BACKUP_ID_PARAM, "id12345")
            .param(UPLOAD_BACKUP_FOLDER_PARAM, "gs://bucket/path")
            .param(UPLOAD_BACKUP_KINDS_PARAM, "one,two,three"));
  }

}
