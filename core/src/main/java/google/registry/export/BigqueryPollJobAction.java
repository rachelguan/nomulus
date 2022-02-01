// Copyright 2017 The Nomulus Authors. All Rights Reserved.
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

import static google.registry.bigquery.BigqueryUtils.toJobReferenceString;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.cloud.tasks.v2.Task;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.flogger.FluentLogger;
import com.google.protobuf.ByteString;
import dagger.Lazy;
import google.registry.request.Action;
import google.registry.request.Action.Service;
import google.registry.request.Header;
import google.registry.request.HttpException.BadRequestException;
import google.registry.request.HttpException.NotModifiedException;
import google.registry.request.Payload;
import google.registry.request.auth.Auth;
import google.registry.util.Clock;
import google.registry.util.CloudTasksUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import javax.inject.Inject;
import org.joda.time.Duration;

/**
 * An action which polls the state of a bigquery job. If it is completed then it will log its
 * completion state; otherwise it will return a failure code so that the task will be retried.
 *
 * <p>Note that this is AUTH_INTERNAL_ONLY: we don't allow "admin" for this to mitigate a
 * vulnerability, see b/177308043.
 */
@Action(
    service = Action.Service.BACKEND,
    path = BigqueryPollJobAction.PATH,
    method = {Action.Method.GET, Action.Method.POST},
    automaticallyPrintOk = true,
    auth = Auth.AUTH_INTERNAL_ONLY)
public class BigqueryPollJobAction implements Runnable {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  static final String QUEUE = "export-bigquery-poll";  // See queue.xml
  static final String PATH = "/_dr/task/pollBigqueryJob";  // See web.xml
  static final String CHAINED_TASK_QUEUE_HEADER = "X-DomainRegistry-ChainedTaskQueue";
  static final String PROJECT_ID_HEADER = "X-DomainRegistry-ProjectId";
  static final String JOB_ID_HEADER = "X-DomainRegistry-JobId";
  static final Duration POLL_COUNTDOWN = Duration.standardSeconds(20);

  @Inject Bigquery bigquery;
  @Inject CloudTasksUtils cloudTasksUtils;

  @Inject @Header(CHAINED_TASK_QUEUE_HEADER) Lazy<String> chainedQueueName;
  @Inject @Header(PROJECT_ID_HEADER) String projectId;
  @Inject @Header(JOB_ID_HEADER) String jobId;

  @Inject Clock clock;

  @Inject @Payload byte[] payload;

  @Inject BigqueryPollJobAction() {}

  @Override
  public void run() {
    boolean jobOutcome =
        checkJobOutcome(); // Throws a NotModifiedException if the job hasn't completed.
    // If the job failed, do not enqueue the next step.
    if (!jobOutcome || payload == null || payload.length == 0) {
      return;
    }
    // If there is a payload, it's a chained task, so enqueue it.
    Task task;
    try {
      task = (Task) new ObjectInputStream(new ByteArrayInputStream(payload)).readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new BadRequestException("Cannot deserialize task from payload", e);
    }
    Task enqueuedTask = cloudTasksUtils.enqueue(chainedQueueName.get(), task);
    logger.atInfo().log(
        "Added chained task %s for %s to queue %s: %s",
        enqueuedTask.getName(),
        enqueuedTask.getAppEngineHttpRequest().getRelativeUri(),
        chainedQueueName.get(),
        enqueuedTask);
  }

  /**
   * Returns true if the provided job succeeded, false if it failed, and throws an exception if it
   * is still pending.
   */
  private boolean checkJobOutcome() {
    Job job = null;
    String jobRefString =
        toJobReferenceString(new JobReference().setProjectId(projectId).setJobId(jobId));

    try {
      job = bigquery.jobs().get(projectId, jobId).execute();
    } catch (IOException e) {
      // We will throw a new exception because done==false, but first log this exception.
      logger.atWarning().withCause(e).log("Error checking outcome of BigQuery job %s.", jobId);
    }
    // If job is not yet done, then throw an exception so that we'll return a failing HTTP status
    // code and the task will be retried.
    if (job == null || !job.getStatus().getState().equals("DONE")) {
      throw new NotModifiedException(jobRefString);
    }

    // Check if the job ended with an error.
    if (job.getStatus().getErrorResult() != null) {
      logger.atSevere().log("Bigquery job failed - %s - %s.", jobRefString, job);
      return false;
    }
    logger.atInfo().log("Bigquery job succeeded - %s.", jobRefString);
    return true;
  }

  /** Helper class to enqueue a bigquery poll job. */
  public static class BigqueryPollJob {
    /** Enqueue a task to poll for the success or failure of the referenced BigQuery job. */
    public static Task createGetTask(JobReference jobRef, Clock clock) {
      return Task.newBuilder()
          .setAppEngineHttpRequest(
              CloudTasksUtils.createGetTask(
                      PATH,
                      Service.BACKEND.toString(),
                      ImmutableMultimap.of(),
                      clock,
                      Optional.of((int) POLL_COUNTDOWN.getMillis()))
                  .getAppEngineHttpRequest()
                  .toBuilder()
                  .putHeaders(PROJECT_ID_HEADER, jobRef.getProjectId())
                  .putHeaders(JOB_ID_HEADER, jobRef.getJobId())
                  .build())
          .build();
    }

    /**
     * Enqueue a task to poll for the success or failure of the referenced BigQuery job and to
     * launch the provided task in the specified queue if the job succeeds.
     */
    public static Task createPostTask(
        JobReference jobRef, Task chainedTask, String chainedTaskQueue) throws IOException {
      // Serialize the chainedTask into a byte array to put in the task payload.
      ByteArrayOutputStream taskBytes = new ByteArrayOutputStream();
      new ObjectOutputStream(taskBytes).writeObject(chainedTask);
      return Task.newBuilder()
          .setAppEngineHttpRequest(
              CloudTasksUtils.createPostTask(
                      PATH, Service.BACKEND.toString(), ImmutableMultimap.of())
                  .getAppEngineHttpRequest()
                  .toBuilder()
                  .putHeaders(PROJECT_ID_HEADER, jobRef.getProjectId())
                  .putHeaders(JOB_ID_HEADER, jobRef.getJobId())
                  .putHeaders(CHAINED_TASK_QUEUE_HEADER, chainedTaskQueue)
                  .setBody(ByteString.copyFrom(taskBytes.toByteArray()))
                  .build())
          .build();
    }

  }
}
