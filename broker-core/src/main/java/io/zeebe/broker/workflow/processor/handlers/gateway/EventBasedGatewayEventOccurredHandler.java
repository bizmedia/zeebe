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
package io.zeebe.broker.workflow.processor.handlers.gateway;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.workflow.model.element.ExecutableEventBasedGateway;
import io.zeebe.broker.workflow.model.element.ExecutableSequenceFlow;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.element.EventOccurredHandler;
import io.zeebe.broker.workflow.state.EventTrigger;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.util.buffer.BufferUtil;
import java.util.List;

// todo: this skips the sequence flow taken and just starts the next element
// https://github.com/zeebe-io/zeebe/issues/1979
public class EventBasedGatewayEventOccurredHandler<T extends ExecutableEventBasedGateway>
    extends EventOccurredHandler<T> {

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    final EventTrigger event = getTriggeredEvent(context, context.getRecord().getKey());
    if (event == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.debug(
          "Processing EVENT_OCCURRED but no event trigger found for element {}",
          context.getElementInstance());
      return;
    }

    final ExecutableSequenceFlow flow = getSequenceFlow(context, event);
    if (flow == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(
          "No outgoing flow has a target with ID {} for process {}",
          BufferUtil.bufferAsString(event.getElementId()),
          BufferUtil.bufferAsString(context.getValue().getBpmnProcessId()));
      return;
    }

    final WorkflowInstanceRecord eventRecord =
        getEventRecord(context, event, flow.getTarget().getElementType());
    deferEvent(
        context, context.getRecord().getKey(), context.getRecord().getKey(), eventRecord, event);
  }

  private ExecutableSequenceFlow getSequenceFlow(BpmnStepContext<T> context, EventTrigger event) {
    final List<ExecutableSequenceFlow> outgoing = context.getElement().getOutgoing();

    for (final ExecutableSequenceFlow flow : outgoing) {
      if (flow.getTarget().getId().equals(event.getElementId())) {
        return flow;
      }
    }

    return null;
  }
}
