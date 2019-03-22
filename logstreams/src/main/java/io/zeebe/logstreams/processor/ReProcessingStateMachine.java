/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.processor;

import io.zeebe.db.TransactionOperation;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.util.retry.EndlessRetryStrategy;
import io.zeebe.util.retry.RetryStrategy;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the reprocessing state machine, which is executed on reprocessing.
 *
 * <pre>
 *   +------------------+
 *   |                  |
 *   |  startRecover()  |
 *   |                  |               +--------------------+
 *   +------------------+               |                    |
 *            |                   +---->|  reprocessEvent()  |--------+
 *            v                   |     |                    |        |
 * +------------------------+     |     +----+---------------+        | exception
 * |                        |     |          |                        |
 * |  reprocessNextEvent()  |-----+          |                        |
 * |                        |                |                 +------v------+
 * +------------------------+                |                 |             |-------+
 *            ^                              |        +-------->  onError()  |       | exception
 *            |                              |        |        |             |<------+
 *            | hasNext                      |        |        +----------+--+
 *            |                              |        |                   |
 * +----------+--------------+               |        | exception         |
 * |                         |               |        |                   |
 * |  onRecordReprocessed()  <------+        |        |                   |
 * |                         |      |        |        |                   |
 * +-----------+-------------+      |   +----v--------+---+               |
 *             |                    |   |                 |               |
 *             |                    +---|  updateState()  |<--------------+
 *             |                        |                 |
 *             v                        +-----------------+
 *    +-----------------+
 *    |                 |
 *    |  onRecovered()  |
 *    |                 |
 *    +-----------------+
 * </pre>
 *
 * See https://textik.com/#773271ce7ea2096a
 */
public final class ReProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;

  private static final String ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT =
      "Expected to find event processor for event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_REPROCESSING_NO_SOURCE_EVENT =
      "Expected to find last source event position '%d', but last position was '%d'. Failed to reprocess on processor '%s'";
  private static final String ERROR_MESSAGE_REPROCESSING_NO_NEXT_EVENT =
      "Expected to find last source event position '%d', but found no next event. Failed to reprocess on processor '%s'";

  private static final String LOG_STMT_REPROCESSING_FINISHED =
      "Processor {} finished reprocessing at event position {}";

  public static ReprocessingStateMachineBuilder builder() {
    return new ReprocessingStateMachineBuilder();
  }

  private final ActorControl actor;
  private final String streamProcessorName;
  private final StreamProcessor streamProcessor;
  private final EventFilter eventFilter;
  private final LogStreamReader logStreamReader;

  private final ZeebeDb zeebeDb;
  private final RetryStrategy updateStateRetryStrategy;
  private final RetryStrategy processRetryStrategy;

  private final BooleanSupplier abortCondition;
  private final long lastSourceEventPosition;
  private final Set<Long> blacklist = new HashSet<>();

  private ReProcessingStateMachine(
      StreamProcessorContext context,
      StreamProcessor streamProcessor,
      ZeebeDb zeebeDb,
      BooleanSupplier abortCondition,
      long lastSourceEventPosition) {
    this.actor = context.getActorControl();
    this.streamProcessorName = context.getName();
    this.eventFilter = context.getEventFilter();
    this.logStreamReader = context.getLogStreamReader();

    this.streamProcessor = streamProcessor;
    this.zeebeDb = zeebeDb;
    this.updateStateRetryStrategy = new EndlessRetryStrategy(actor);
    this.processRetryStrategy = new EndlessRetryStrategy(actor);
    this.abortCondition = abortCondition;
    this.lastSourceEventPosition = lastSourceEventPosition;
  }

  // current iteration
  private ActorFuture<Void> recoveryFuture;
  private LoggedEvent currentEvent;
  private EventProcessor eventProcessor;
  private ZeebeDbTransaction zeebeDbTransaction;

  ActorFuture<Void> startRecover() {
    recoveryFuture = new CompletableActorFuture<>();

    final long startPosition = logStreamReader.getPosition();

    LOG.info("Start scanning the logs for error events.");
    scanLog();

    logStreamReader.seek(startPosition);
    reprocessNextEvent();
    blacklist.clear();
    return recoveryFuture;
  }

  private void scanLog() {
    try {
      readNextEvent();

      final long errorPosition = streamProcessor.getFailedPosition(currentEvent);
      if (errorPosition >= 0) {
        LOG.info(
            "Found error-prone event {}, will add position {} to the blacklist.",
            currentEvent,
            errorPosition);
        blacklist.add(errorPosition);
      }

    } catch (final RuntimeException e) {
      recoveryFuture.completeExceptionally(e);
    }
  }

  private void readNextEvent() {
    if (!logStreamReader.hasNext()) {
      throw new IllegalStateException(
          String.format(
              ERROR_MESSAGE_REPROCESSING_NO_NEXT_EVENT,
              lastSourceEventPosition,
              streamProcessorName));
    }

    currentEvent = logStreamReader.next();
    if (currentEvent.getPosition() > lastSourceEventPosition) {
      throw new IllegalStateException(
          String.format(
              ERROR_MESSAGE_REPROCESSING_NO_SOURCE_EVENT,
              lastSourceEventPosition,
              currentEvent.getPosition(),
              streamProcessorName));
    }
  }

  private void reprocessNextEvent() {
    try {
      readNextEvent();

      if (eventFilter == null || eventFilter.applies(currentEvent)) {
        reprocessEvent(currentEvent);
      } else {
        onRecordReprocessed(currentEvent);
      }

    } catch (final RuntimeException e) {
      recoveryFuture.completeExceptionally(e);
    }
  }

  private void reprocessEvent(final LoggedEvent currentEvent) {
    try {
      eventProcessor = streamProcessor.onEvent(currentEvent);
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT, currentEvent, streamProcessorName, e);
    }

    if (eventProcessor == null) {
      onRecordReprocessed(currentEvent);
      return;
    }

    processUntilDone(currentEvent);
  }

  private void processUntilDone(LoggedEvent currentEvent) {
    final TransactionOperation operationOnProcessing;
    if (blacklist.contains(currentEvent.getPosition())) {
      LOG.info(
          "Event {} failed on processing last time, will call #onError to update workflow instance blacklist.",
          currentEvent);
      operationOnProcessing = () -> eventProcessor.onError(new Exception());
    } else {
      operationOnProcessing = eventProcessor::processEvent;
    }

    final ActorFuture<Boolean> resultFuture =
        processRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction = zeebeDb.transaction();
              zeebeDbTransaction.run(operationOnProcessing);
              return true;
            },
            () -> {
              // is checked on exception
              if (zeebeDbTransaction != null)
              {
                zeebeDbTransaction.rollback();
              }
              return abortCondition.getAsBoolean();
            });

    actor.runOnCompletion(
        resultFuture,
        (v, t) -> {
          // processing should be retried endless until it worked
          assert t == null : "On reprocessing there shouldn't be any exception thrown.";
          updateStateUntilDone();
        });
  }

  private void updateStateUntilDone() {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.commit();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          // update state should be retried endless until it worked
          assert throwable == null : "On reprocessing there shouldn't be any exception thrown.";
          onRecordReprocessed(currentEvent);
        });
  }

  private void onRecordReprocessed(final LoggedEvent currentEvent) {
    if (currentEvent.getPosition() == lastSourceEventPosition) {
      LOG.info(LOG_STMT_REPROCESSING_FINISHED, streamProcessorName, currentEvent.getPosition());
      onRecovered();
    } else {
      actor.submit(this::reprocessNextEvent);
    }
  }

  private void onRecovered() {
    recoveryFuture.complete(null);
  }

  public static class ReprocessingStateMachineBuilder {

    private StreamProcessor streamProcessor;

    private StreamProcessorContext streamProcessorContext;
    private ZeebeDb zeebeDb;
    private BooleanSupplier abortCondition;
    private long lastSourceEventPosition;

    public ReprocessingStateMachineBuilder setStreamProcessor(StreamProcessor streamProcessor) {
      this.streamProcessor = streamProcessor;
      return this;
    }

    public ReprocessingStateMachineBuilder setStreamProcessorContext(
        StreamProcessorContext context) {
      this.streamProcessorContext = context;
      return this;
    }

    public ReprocessingStateMachineBuilder setZeebeDb(ZeebeDb zeebeDb) {
      this.zeebeDb = zeebeDb;
      return this;
    }

    public ReprocessingStateMachineBuilder setAbortCondition(BooleanSupplier abortCondition) {
      this.abortCondition = abortCondition;
      return this;
    }

    public ReprocessingStateMachineBuilder setLastSourceEventPosition(
        long lastSourceEventPosition) {
      this.lastSourceEventPosition = lastSourceEventPosition;
      return this;
    }

    public ReProcessingStateMachine build() {
      Objects.requireNonNull(streamProcessorContext);
      Objects.requireNonNull(streamProcessor);
      Objects.requireNonNull(zeebeDb);
      Objects.requireNonNull(abortCondition);
      return new ReProcessingStateMachine(
          streamProcessorContext,
          streamProcessor,
          zeebeDb,
          abortCondition,
          lastSourceEventPosition);
    }
  }
}
