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

import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.util.exception.RecoverableException;
import io.zeebe.util.retry.AbortableRetryStrategy;
import io.zeebe.util.retry.RecoverableRetryStrategy;
import io.zeebe.util.retry.RetryStrategy;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the processing state machine, which is executed on normal processing.
 *
 * <pre>
 *
 * +-----------------+             +--------------------+
 * |                 |             |                    |      exception
 * | readNextEvent() |------------>|   processEvent()   |------------------+
 * |                 |             |                    |                  v
 * +-----------------+             +--------------------+            +---------------+
 *           ^                             |                         |               |------+
 *           |                             |         +-------------->|   onError()   |      | exception
 *           |                             |         |  exception    |               |<-----+
 *           |                     +-------v-------------+           +---------------+
 *           |                     |                     |                 |
 *           |                     |    writeEvent()     |                 |
 *           |                     |                     |<----------------+
 * +----------------------+        +---------------------+
 * |                      |                 |
 * | executeSideEffects() |                 v
 * |                      |       +----------------------+
 * +----------------------+       |                      |
 *           ^                    |     updateState()    |
 *           +--------------------|                      |
 *                                +----------------------+
 *                                       ^      |
 *                                       |      | exception
 *                                       |      |
 *                                    +---------v----+
 *                                    |              |
 *                                    |   onError()  |
 *                                    |              |
 *                                    +--------------+
 *                                       ^     |
 *                                       |     |  exception
 *                                       +-----+
 *
 * </pre>
 */
public final class ProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;

  public static final String ERROR_MESSAGE_WRITE_EVENT_ABORTED =
      "Expected to write one or more follow up events for event '{}' without errors, but exception was thrown.";
  private static final String ERROR_MESSAGE_ROLLBACK_ABORTED =
      "Expected to roll back the current transaction for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED =
      "Expected to execute side effects for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_UPDATE_STATE_FAILED =
      "Expected to successfully update state for event '{}' with processor '{}', but caught an exception. Retry.";
  private static final String ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT =
      "Expected to find event processor for event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT =
      "Expected to successfully process event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING =
      "Expected to process event '{}' successfully on stream processor '{}', but caught recoverable exception. Retry processing.";

  private static final Duration PROCESSING_RETRY_DELAY = Duration.ofMillis(250);
  private final LogStream logStream;

  public static ProcessingStateMachineBuilder builder() {
    return new ProcessingStateMachineBuilder();
  }

  private final ActorControl actor;
  private final int producerId;
  private final String streamProcessorName;
  private final StreamProcessorMetrics metrics;
  private final StreamProcessor streamProcessor;
  private final EventFilter eventFilter;
  private final LogStreamReader logStreamReader;
  private final LogStreamRecordWriter logStreamWriter;

  private final ZeebeDb zeebeDb;
  private final RetryStrategy writeRetryStrategy;
  private final RetryStrategy sideEffectsRetryStrategy;
  private final RetryStrategy updateStateRetryStrategy;

  private final BooleanSupplier shouldProcessNext;
  private final BooleanSupplier abortCondition;

  private ProcessingStateMachine(
      StreamProcessorContext context,
      StreamProcessorMetrics metrics,
      StreamProcessor streamProcessor,
      ZeebeDb zeebeDb,
      BooleanSupplier shouldProcessNext,
      BooleanSupplier abortCondition) {
    this.actor = context.getActorControl();
    this.producerId = context.getId();
    this.streamProcessorName = context.getName();
    this.eventFilter = context.getEventFilter();
    this.logStreamReader = context.getLogStreamReader();
    this.logStreamWriter = context.logStreamWriter;
    this.logStream = context.getLogStream();

    this.metrics = metrics;
    this.streamProcessor = streamProcessor;
    this.zeebeDb = zeebeDb;
    this.writeRetryStrategy = new AbortableRetryStrategy(actor);
    this.sideEffectsRetryStrategy = new AbortableRetryStrategy(actor);
    this.updateStateRetryStrategy = new RecoverableRetryStrategy(actor);
    this.shouldProcessNext = shouldProcessNext;
    this.abortCondition = abortCondition;
  }

  // current iteration
  private LoggedEvent currentEvent;
  private EventProcessor eventProcessor;
  private ZeebeDbTransaction zeebeDbTransaction;

  private long eventPosition = -1L;
  private long lastSuccessfulProcessedEventPosition = -1L;
  private long lastWrittenEventPosition = -1L;

  private boolean onErrorHandling;
  private long errorRecordPosition = -1;

  private void skipRecord() {
    actor.submit(this::readNextEvent);
    metrics.incrementEventsSkippedCount();
  }

  void readNextEvent() {
    if (shouldProcessNext.getAsBoolean()
        && logStreamReader.hasNext()
        && eventProcessor == null
        && logStream.getCommitPosition() >= errorRecordPosition) {
      currentEvent = logStreamReader.next();

      if (eventFilter == null || eventFilter.applies(currentEvent)) {
        processEvent(currentEvent);
      } else {
        skipRecord();
      }
    }
  }

  private void processEvent(final LoggedEvent event) {
    try {
      eventProcessor = streamProcessor.onEvent(event);
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      skipRecord();
      return;
    }

    if (eventProcessor == null) {
      skipRecord();
      return;
    }

    try {
      zeebeDbTransaction = zeebeDb.transaction();
      zeebeDbTransaction.run(eventProcessor::processEvent);
      metrics.incrementEventsProcessedCount();
      writeEvent();
    } catch (final RecoverableException recoverableException) {
      // recoverable
      LOG.error(
          ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING,
          event,
          streamProcessorName,
          recoverableException);
      actor.runDelayed(PROCESSING_RETRY_DELAY, () -> processEvent(currentEvent));
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      onError(e, this::writeEvent);
    }
  }

  private void onError(Throwable t, Runnable nextStep) {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.rollback();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_ROLLBACK_ABORTED, currentEvent, throwable);
          }
          try {
            zeebeDbTransaction = zeebeDb.transaction();
            zeebeDbTransaction.run(() -> eventProcessor.onError(t));
            onErrorHandling = true;
            nextStep.run();
          } catch (Exception ex) {
            onError(ex, nextStep);
          }
        });
  }

  private void writeEvent() {
    logStreamWriter.producerId(producerId).sourceRecordPosition(currentEvent.getPosition());

    final ActorFuture<Boolean> retryFuture =
        writeRetryStrategy.runWithRetry(
            () -> {
              eventPosition = eventProcessor.writeEvent(logStreamWriter);
              return eventPosition >= 0;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, t) -> {
          if (t != null) {
            LOG.error(ERROR_MESSAGE_WRITE_EVENT_ABORTED, currentEvent, t);
            onError(t, this::writeEvent);
          } else {
            metrics.incrementEventsWrittenCount();
            updateState();
          }
        });
  }

  private void updateState() {
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
          if (throwable != null) {
            LOG.error(
                ERROR_MESSAGE_UPDATE_STATE_FAILED, currentEvent, streamProcessorName, throwable);
            onError(throwable, this::updateState);
          } else {

            if (onErrorHandling) {
              errorRecordPosition = eventPosition;
              onErrorHandling = false;
            }
            lastSuccessfulProcessedEventPosition = currentEvent.getPosition();
            lastWrittenEventPosition = eventPosition;

            executeSideEffects();
          }
        });
  }

  private void executeSideEffects() {
    final ActorFuture<Boolean> retryFuture =
        sideEffectsRetryStrategy.runWithRetry(eventProcessor::executeSideEffects, abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED, currentEvent, throwable);
          }

          // continue with next event
          eventProcessor = null;
          actor.submit(this::readNextEvent);
        });
  }

  public long getLastSuccessfulProcessedEventPosition() {
    return lastSuccessfulProcessedEventPosition;
  }

  public long getLastWrittenEventPosition() {
    return lastWrittenEventPosition;
  }

  public static class ProcessingStateMachineBuilder {

    private StreamProcessorMetrics metrics;
    private StreamProcessor streamProcessor;

    private StreamProcessorContext streamProcessorContext;
    private ZeebeDb zeebeDb;
    private BooleanSupplier shouldProcessNext;
    private BooleanSupplier abortCondition;

    public ProcessingStateMachineBuilder setMetrics(StreamProcessorMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public ProcessingStateMachineBuilder setStreamProcessor(StreamProcessor streamProcessor) {
      this.streamProcessor = streamProcessor;
      return this;
    }

    public ProcessingStateMachineBuilder setStreamProcessorContext(StreamProcessorContext context) {
      this.streamProcessorContext = context;
      return this;
    }

    public ProcessingStateMachineBuilder setZeebeDb(ZeebeDb zeebeDb) {
      this.zeebeDb = zeebeDb;
      return this;
    }

    public ProcessingStateMachineBuilder setShouldProcessNext(BooleanSupplier shouldProcessNext) {
      this.shouldProcessNext = shouldProcessNext;
      return this;
    }

    public ProcessingStateMachineBuilder setAbortCondition(BooleanSupplier abortCondition) {
      this.abortCondition = abortCondition;
      return this;
    }

    public ProcessingStateMachine build() {
      Objects.requireNonNull(streamProcessorContext);
      Objects.requireNonNull(metrics);
      Objects.requireNonNull(streamProcessor);
      Objects.requireNonNull(zeebeDb);
      Objects.requireNonNull(shouldProcessNext);
      Objects.requireNonNull(abortCondition);
      return new ProcessingStateMachine(
          streamProcessorContext,
          metrics,
          streamProcessor,
          zeebeDb,
          shouldProcessNext,
          abortCondition);
    }
  }
}
