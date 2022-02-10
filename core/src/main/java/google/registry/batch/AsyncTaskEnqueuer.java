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

package google.registry.batch;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.common.flogger.FluentLogger;
import google.registry.config.RegistryConfig.Config;
import google.registry.model.EppResource;
import google.registry.model.domain.RegistryLock;
import google.registry.model.eppcommon.Trid;
import google.registry.model.host.HostResource;
import google.registry.persistence.VKey;
import google.registry.util.AppEngineServiceUtils;
import google.registry.util.Retrier;
import javax.inject.Inject;
import javax.inject.Named;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/** Helper class to enqueue tasks for handling asynchronous operations in flows. */
public final class AsyncTaskEnqueuer {

  /** The HTTP parameter names used by async flows. */
  public static final String PARAM_RESOURCE_KEY = "resourceKey";
  public static final String PARAM_REQUESTING_CLIENT_ID = "requestingClientId";
  public static final String PARAM_CLIENT_TRANSACTION_ID = "clientTransactionId";
  public static final String PARAM_SERVER_TRANSACTION_ID = "serverTransactionId";
  public static final String PARAM_IS_SUPERUSER = "isSuperuser";
  public static final String PARAM_HOST_KEY = "hostKey";
  public static final String PARAM_REQUESTED_TIME = "requestedTime";
  public static final String PARAM_RESAVE_TIMES = "resaveTimes";

  /** The task queue names used by async flows. */
  public static final String QUEUE_ASYNC_ACTIONS = "async-actions";
  public static final String QUEUE_ASYNC_DELETE = "async-delete-pull";
  public static final String QUEUE_ASYNC_HOST_RENAME = "async-host-rename-pull";

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  public static final Duration MAX_ASYNC_ETA = Duration.standardDays(30);

  private final Duration asyncDeleteDelay;
  private final Queue asyncActionsPushQueue;
  private final Queue asyncDeletePullQueue;
  private final Queue asyncDnsRefreshPullQueue;
  private final AppEngineServiceUtils appEngineServiceUtils;
  private final Retrier retrier;

  @Inject
  public AsyncTaskEnqueuer(
      @Named(QUEUE_ASYNC_ACTIONS) Queue asyncActionsPushQueue,
      @Named(QUEUE_ASYNC_DELETE) Queue asyncDeletePullQueue,
      @Named(QUEUE_ASYNC_HOST_RENAME) Queue asyncDnsRefreshPullQueue,
      @Config("asyncDeleteFlowMapreduceDelay") Duration asyncDeleteDelay,
      AppEngineServiceUtils appEngineServiceUtils,
      Retrier retrier) {
    this.asyncActionsPushQueue = asyncActionsPushQueue;
    this.asyncDeletePullQueue = asyncDeletePullQueue;
    this.asyncDnsRefreshPullQueue = asyncDnsRefreshPullQueue;
    this.asyncDeleteDelay = asyncDeleteDelay;
    this.appEngineServiceUtils = appEngineServiceUtils;
    this.retrier = retrier;
  }

  /** Enqueues a task to asynchronously delete a contact or host, by key. */
  public void enqueueAsyncDelete(
      EppResource resourceToDelete,
      DateTime now,
      String requestingRegistrarId,
      Trid trid,
      boolean isSuperuser) {
    logger.atInfo().log(
        "Enqueuing async deletion of %s on behalf of registrar %s.",
        resourceToDelete.getRepoId(), requestingRegistrarId);
    TaskOptions task =
        TaskOptions.Builder.withMethod(Method.PULL)
            .countdownMillis(asyncDeleteDelay.getMillis())
            .param(PARAM_RESOURCE_KEY, resourceToDelete.createVKey().stringify())
            .param(PARAM_REQUESTING_CLIENT_ID, requestingRegistrarId)
            .param(PARAM_SERVER_TRANSACTION_ID, trid.getServerTransactionId())
            .param(PARAM_IS_SUPERUSER, Boolean.toString(isSuperuser))
            .param(PARAM_REQUESTED_TIME, now.toString());
    trid.getClientTransactionId()
        .ifPresent(clTrid -> task.param(PARAM_CLIENT_TRANSACTION_ID, clTrid));
    addTaskToQueueWithRetry(asyncDeletePullQueue, task);
  }

  /** Enqueues a task to asynchronously refresh DNS for a renamed host. */
  public void enqueueAsyncDnsRefresh(HostResource host, DateTime now) {
    VKey<HostResource> hostKey = host.createVKey();
    logger.atInfo().log("Enqueuing async DNS refresh for renamed host %s.", hostKey);
    addTaskToQueueWithRetry(
        asyncDnsRefreshPullQueue,
        TaskOptions.Builder.withMethod(Method.PULL)
            .param(PARAM_HOST_KEY, hostKey.stringify())
            .param(PARAM_REQUESTED_TIME, now.toString()));
  }

  /**
   * Enqueues a task to asynchronously re-lock a registry-locked domain after it was unlocked.
   *
   * <p>Note: the relockDuration must be present on the lock object.
   */
  public void enqueueDomainRelock(RegistryLock lock) {
    checkArgument(
        lock.getRelockDuration().isPresent(),
        "Lock with ID %s not configured for relock",
        lock.getRevisionId());
    enqueueDomainRelock(lock.getRelockDuration().get(), lock.getRevisionId(), 0);
  }

  /** Enqueues a task to asynchronously re-lock a registry-locked domain after it was unlocked. */
  void enqueueDomainRelock(Duration countdown, long lockRevisionId, int previousAttempts) {
    String backendHostname = appEngineServiceUtils.getServiceHostname("backend");
    addTaskToQueueWithRetry(
        asyncActionsPushQueue,
        TaskOptions.Builder.withUrl(RelockDomainAction.PATH)
            .method(Method.POST)
            .header("Host", backendHostname)
            .param(RelockDomainAction.OLD_UNLOCK_REVISION_ID_PARAM, String.valueOf(lockRevisionId))
            .param(RelockDomainAction.PREVIOUS_ATTEMPTS_PARAM, String.valueOf(previousAttempts))
            .countdownMillis(countdown.getMillis()));
  }

  /**
   * Adds a task to a queue with retrying, to avoid aborting the entire flow over a transient issue
   * enqueuing a task.
   */
  private void addTaskToQueueWithRetry(final Queue queue, final TaskOptions task) {
    retrier.callWithRetry(() -> queue.add(task), TransientFailureException.class);
  }
}
