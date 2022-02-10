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

package google.registry.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static google.registry.batch.AsyncTaskEnqueuer.MAX_ASYNC_ETA;
import static google.registry.batch.AsyncTaskEnqueuer.PARAM_REQUESTED_TIME;
import static google.registry.batch.AsyncTaskEnqueuer.PARAM_RESAVE_TIMES;
import static google.registry.batch.AsyncTaskEnqueuer.PARAM_RESOURCE_KEY;
import static google.registry.batch.AsyncTaskEnqueuer.QUEUE_ASYNC_ACTIONS;
import static google.registry.persistence.transaction.TransactionManagerFactory.tm;
import static google.registry.util.DateTimeUtils.isBeforeOrAt;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimap;
import com.google.common.flogger.FluentLogger;
import google.registry.model.EppResource;
import google.registry.model.ImmutableObject;
import google.registry.persistence.VKey;
import google.registry.request.Action;
import google.registry.request.Action.Method;
import google.registry.request.Action.Service;
import google.registry.request.Parameter;
import google.registry.request.Response;
import google.registry.request.auth.Auth;
import google.registry.util.Clock;
import google.registry.util.CloudTasksUtils;
import javax.inject.Inject;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * An action that re-saves a given entity, typically after a certain amount of time has passed.
 *
 * <p>{@link EppResource}s will be projected forward to the current time.
 */
@Action(
    service = Action.Service.BACKEND,
    path = ResaveEntityAction.PATH,
    auth = Auth.AUTH_INTERNAL_OR_ADMIN,
    method = Method.POST)
public class ResaveEntityAction implements Runnable {

  public static final String PATH = "/_dr/task/resaveEntity";

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final String resourceKey;
  private final DateTime requestedTime;
  private final ImmutableSortedSet<DateTime> resaveTimes;
  private final Response response;
  private CloudTasksUtils cloudTasksUtils;
  private Clock clock;

  @Inject
  ResaveEntityAction(
      @Parameter(PARAM_RESOURCE_KEY) String resourceKey,
      @Parameter(PARAM_REQUESTED_TIME) DateTime requestedTime,
      @Parameter(PARAM_RESAVE_TIMES) ImmutableSet<DateTime> resaveTimes,
      CloudTasksUtils cloudTasksUtils,
      Clock clock,
      Response response) {
    this.resourceKey = resourceKey;
    this.requestedTime = requestedTime;
    this.resaveTimes = ImmutableSortedSet.copyOf(resaveTimes);
    this.cloudTasksUtils = cloudTasksUtils;
    this.clock = clock;
    this.response = response;
  }

  @Override
  public void run() {
    logger.atInfo().log(
        "Re-saving entity %s which was enqueued at %s.", resourceKey, requestedTime);
    tm().transact(
            () -> {
              ImmutableObject entity = tm().loadByKey(VKey.create(resourceKey));
              tm().put(
                      (entity instanceof EppResource)
                          ? ((EppResource) entity).cloneProjectedAtTime(tm().getTransactionTime())
                          : entity);
              if (!resaveTimes.isEmpty()) {
                enqueueAsyncResave(VKey.create(resourceKey), requestedTime, resaveTimes);
              }
            });
    response.setPayload("Entity re-saved.");
  }
  /**
   * Enqueues a task to asynchronously re-save an entity at some point(s) in the future.
   *
   * <p>Multiple re-save times are chained one after the other, i.e. any given run will re-enqueue
   * itself to run at the next time if there are remaining re-saves scheduled.
   */
  private void enqueueAsyncResave(
      VKey<?> entityKey, DateTime now, ImmutableSortedSet<DateTime> whenToResave) {
    DateTime firstResave = whenToResave.first();
    checkArgument(isBeforeOrAt(now, firstResave), "Can't enqueue a resave " + "to run in the past");
    Duration etaDuration = new Duration(now, firstResave);
    if (etaDuration.isLongerThan(MAX_ASYNC_ETA)) {
      logger.atInfo().log(
          "Ignoring async re-save of %s; %s is past the ETA threshold of %s.",
          entityKey, firstResave, MAX_ASYNC_ETA);
      return;
    }
    Multimap<String, String> params = ArrayListMultimap.create();
    params.put(PARAM_RESOURCE_KEY, entityKey.stringify());
    params.put(PARAM_REQUESTED_TIME, now.toString());
    if (whenToResave.size() > 1) {
      params.put(PARAM_RESAVE_TIMES, Joiner.on(',').join(whenToResave.tailSet(firstResave, false)));
    }
    logger.atInfo().log("Enqueuing async re-save of %s to run at %s.", entityKey, whenToResave);
    cloudTasksUtils.enqueue(
        QUEUE_ASYNC_ACTIONS,
        CloudTasksUtils.createPostTask(
            ResaveEntityAction.PATH, Service.BACKEND.toString(), params, clock, etaDuration));
  }
}
