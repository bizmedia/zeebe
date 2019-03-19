/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow.processor.handlers.activity;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.workflow.model.element.ExecutableActivity;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEvent;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.catchevent.CatchEventSupplierEventOccurredHandler;
import io.zeebe.broker.workflow.state.EventTrigger;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.buffer.BufferUtil;
import java.util.List;

public class ActivityEventOccurredHandler<T extends ExecutableActivity>
    extends CatchEventSupplierEventOccurredHandler<T> {
  public ActivityEventOccurredHandler() {
    this(null);
  }

  public ActivityEventOccurredHandler(WorkflowInstanceIntent nextState) {
    super(nextState);
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    final EventTrigger event = getTriggeredEvent(context, context.getRecord().getKey());
    if (isActivityEventHandler(context, event)) {
      return super.handleState(context);
    }

    final ExecutableBoundaryEvent boundaryEvent = getBoundaryEvent(context, event);
    if (boundaryEvent == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(
          "No boundary event found with ID {} for process {}",
          BufferUtil.bufferAsString(event.getElementId()),
          BufferUtil.bufferAsString(context.getValue().getBpmnProcessId()));
      return false;
    }

    final WorkflowInstanceRecord eventRecord =
        getEventRecord(context, event, boundaryEvent.getElementType());
    if (boundaryEvent.cancelActivity()) {
      transitionTo(context, WorkflowInstanceIntent.ELEMENT_TERMINATING);
      deferEvent(
          context, context.getRecord().getKey(), context.getRecord().getKey(), eventRecord, event);
    } else {
      publishEvent(context, context.getRecord().getKey(), eventRecord, event);
    }

    return true;
  }

  private boolean isActivityEventHandler(BpmnStepContext<T> context, EventTrigger event) {
    return event.getElementId().equals(context.getElement().getId());
  }

  private ExecutableBoundaryEvent getBoundaryEvent(BpmnStepContext<T> context, EventTrigger event) {
    final List<ExecutableBoundaryEvent> boundaryEvents = context.getElement().getBoundaryEvents();
    for (final ExecutableBoundaryEvent boundaryEvent : boundaryEvents) {
      if (event.getElementId().equals(boundaryEvent.getId())) {
        return boundaryEvent;
      }
    }

    return null;
  }
}
