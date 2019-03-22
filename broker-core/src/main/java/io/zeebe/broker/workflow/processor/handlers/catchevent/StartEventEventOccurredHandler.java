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
package io.zeebe.broker.workflow.processor.handlers.catchevent;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEventElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.element.EventOccurredHandler;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.EventTrigger;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.BpmnElementType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class StartEventEventOccurredHandler<T extends ExecutableCatchEventElement>
    extends EventOccurredHandler<T> {
  private static final String NO_WORKFLOW_FOUND_MESSAGE =
      "Expected to create an instance of workflow with key '%d', but no such workflow was found";
  private static final String NO_TRIGGERED_EVENT_MESSAGE = "No triggered event for workflow '%d'";

  private final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
  private final WorkflowState state;

  public StartEventEventOccurredHandler(WorkflowState state) {
    this.state = state;
  }

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    final WorkflowInstanceRecord event = context.getRecord().getValue();
    final long workflowKey = event.getWorkflowKey();
    final DeployedWorkflow workflow = state.getWorkflowByKey(workflowKey);
    final long workflowInstanceKey =
        context.getOutput().getStreamWriter().getKeyGenerator().nextKey();

    // this should never happen because workflows are never deleted.
    if (workflow == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(
          String.format(NO_WORKFLOW_FOUND_MESSAGE, workflowKey));
      return;
    }

    final EventTrigger triggeredEvent = getTriggeredEvent(context, workflowKey);
    if (triggeredEvent == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(
          String.format(NO_TRIGGERED_EVENT_MESSAGE, workflowKey));
      return;
    }

    createWorkflowInstance(context, workflow, workflowInstanceKey);
    final WorkflowInstanceRecord record =
        getEventRecord(context, triggeredEvent, BpmnElementType.START_EVENT)
            .setWorkflowInstanceKey(workflowInstanceKey)
            .setVersion(workflow.getVersion())
            .setBpmnProcessId(workflow.getBpmnProcessId())
            .setFlowScopeKey(workflowInstanceKey);

    deferEvent(context, workflowKey, workflowInstanceKey, record, triggeredEvent);
  }

  private void createWorkflowInstance(
      BpmnStepContext<T> context, DeployedWorkflow workflow, long workflowInstanceKey) {
    record
        .setBpmnProcessId(workflow.getBpmnProcessId())
        .setWorkflowKey(workflow.getKey())
        .setVersion(workflow.getVersion())
        .setWorkflowInstanceKey(workflowInstanceKey);

    context
        .getOutput()
        .appendFollowUpEvent(
            workflowInstanceKey,
            WorkflowInstanceIntent.ELEMENT_ACTIVATING,
            record,
            workflow.getWorkflow());
  }
}
