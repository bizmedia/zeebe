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
package io.zeebe.broker.workflow.processor.handlers.element;

import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.state.EventTrigger;
import io.zeebe.protocol.BpmnElementType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * Checks the event trigger, and if it is related to the element itself, will transition to
 * completing.
 */
public abstract class EventOccurredHandler<T extends ExecutableFlowElement>
    extends AbstractHandler<T> {
  private final WorkflowInstanceRecord eventRecord = new WorkflowInstanceRecord();

  /**
   * Returns the latest event trigger but does not consume it from the state. It will be consumed
   * once the caller calls {@link #processEventTrigger(BpmnStepContext, long, long, EventTrigger)}.
   */
  protected EventTrigger getTriggeredEvent(BpmnStepContext<T> context, long scopeKey) {
    return context.getStateDb().getEventScopeInstanceState().peekEventTrigger(scopeKey);
  }

  /**
   * Will trigger the creation of a new instance of the flow with the same elementId as the event,
   * but in an asynchronous fashion in the form of a deferred record.
   *
   * @param context the current BPMN context
   * @param eventScopeKey the event scope key from which the event trigger was initially read
   * @param flowScopeKey the key of the element instance which will publish the deferred record
   * @param record the record to defer
   * @param event the event trigger to handle
   */
  protected long deferEvent(
      BpmnStepContext<T> context,
      long eventScopeKey,
      long flowScopeKey,
      WorkflowInstanceRecord record,
      EventTrigger event) {
    final long eventInstanceKey =
        context
            .getOutput()
            .deferRecord(flowScopeKey, record, WorkflowInstanceIntent.ELEMENT_ACTIVATING);

    processEventTrigger(context, eventScopeKey, eventInstanceKey, event);
    return eventInstanceKey;
  }

  /**
   * Will trigger the creation of a new instance of the flow with the same elementId as the event in
   * a synchronous fashion by publishing its ELEMENT_ACTIVATING event and spawning a new token for
   * it.
   *
   * @param context the current BPMN context
   * @param eventScopeKey the event scope key from which the event trigger was initially read
   * @param record the record to publish
   * @param event the event trigger to handle
   */
  protected long publishEvent(
      BpmnStepContext<T> context,
      long eventScopeKey,
      WorkflowInstanceRecord record,
      EventTrigger event) {
    final long eventInstanceKey =
        context.getOutput().appendNewEvent(WorkflowInstanceIntent.ELEMENT_ACTIVATING, record);
    processEventTrigger(context, eventScopeKey, eventInstanceKey, event);
    context.getFlowScopeInstance().spawnToken();

    return eventInstanceKey;
  }

  /**
   * Applies the event payload to the given variable scope, and removes the event from the state
   * ensuring it cannot be reprocessed.
   *
   * @param context the current BPMN context
   * @param eventScopeKey the event scope key from which the event trigger was initially read
   * @param variableScopeKey the variable scope key on which the event trigger payload will be set
   * @param event the event trigger to handle
   */
  protected void processEventTrigger(
      BpmnStepContext<T> context, long eventScopeKey, long variableScopeKey, EventTrigger event) {
    context
        .getElementInstanceState()
        .getVariablesState()
        .setPayload(variableScopeKey, event.getPayload());

    context
        .getStateDb()
        .getEventScopeInstanceState()
        .deleteTrigger(eventScopeKey, event.getEventKey());
  }

  protected WorkflowInstanceRecord getEventRecord(
      BpmnStepContext<T> context, EventTrigger event, BpmnElementType elementType) {
    eventRecord.reset();
    eventRecord.wrap(context.getValue());
    eventRecord.setElementId(event.getElementId());
    eventRecord.setBpmnElementType(elementType);

    return eventRecord;
  }
}
