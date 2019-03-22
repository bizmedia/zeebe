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
package io.zeebe.broker.workflow.processor.handlers.container;

import io.zeebe.broker.workflow.model.element.ExecutableCatchEventElement;
import io.zeebe.broker.workflow.model.element.ExecutableFlowElementContainer;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.ElementStateHandler;
import io.zeebe.broker.workflow.state.IndexedRecord;
import io.zeebe.broker.workflow.state.StoredRecord.Purpose;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.List;

public class ContainerElementActivatedHandler<T extends ExecutableFlowElementContainer>
    implements ElementStateHandler<T> {

  @Override
  public boolean handleState(BpmnStepContext<T> context) {
    final ExecutableFlowElementContainer element = context.getElement();
    final ExecutableCatchEventElement firstStartEvent = element.getStartEvents().get(0);

    // workflows with none start event only have a single none start event and no other types of
    // start events
    if (firstStartEvent.isNone()) {
      activateNoneStartEvent(context, firstStartEvent);
    } else {
      publishDeferredRecord(context);
    }

    context.getElementInstance().spawnToken();
    return true;
  }

  private void publishDeferredRecord(BpmnStepContext<T> context) {
    final IndexedRecord deferredRecord = getDeferredRecord(context);
    context
        .getOutput()
        .appendFollowUpEvent(
            deferredRecord.getKey(), deferredRecord.getState(), deferredRecord.getValue());
  }

  private void activateNoneStartEvent(
      BpmnStepContext<T> context, ExecutableCatchEventElement firstStartEvent) {
    final WorkflowInstanceRecord value = context.getValue();

    value.setElementId(firstStartEvent.getId());
    value.setBpmnElementType(firstStartEvent.getElementType());
    value.setFlowScopeKey(context.getRecord().getKey());
    context.getOutput().appendNewEvent(WorkflowInstanceIntent.ELEMENT_ACTIVATING, value);
  }

  private IndexedRecord getDeferredRecord(BpmnStepContext<T> context) {
    final long wfInstanceKey = context.getRecord().getValue().getWorkflowInstanceKey();
    final List<IndexedRecord> deferredRecords =
        context.getElementInstanceState().getDeferredRecords(wfInstanceKey);

    if (deferredRecords.isEmpty()) {
      throw new IllegalStateException(
          "Expected process with no none start events to have a deferred record, but nothing was found");
    }

    assert deferredRecords.size() == 1
        : "should only have one deferred start event per workflow instance";

    final IndexedRecord deferredRecord = deferredRecords.get(0);
    context
        .getElementInstanceState()
        .removeStoredRecord(wfInstanceKey, deferredRecord.getKey(), Purpose.DEFERRED);
    return deferredRecord;
  }
}
