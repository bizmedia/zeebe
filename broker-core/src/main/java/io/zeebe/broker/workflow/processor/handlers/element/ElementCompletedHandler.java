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

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.AbstractTerminalStateHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * Delegates completion logic to sub class, and if successful and it is the last active element in
 * its flow scope, will terminate its flow scope.
 *
 * @param <T>
 */
public class ElementCompletedHandler<T extends ExecutableFlowNode>
    extends AbstractTerminalStateHandler<T> {
  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    // todo: move this to some catch event supplier handler
    // https://github.com/zeebe-io/zeebe/issues/1968
    publishDeferredRecords(context);

    if (isLastActiveExecutionPathInScope(context)) {
      completeFlowScope(context);
    }

    return super.handleState(context);
  }

  protected void completeFlowScope(BpmnStepContext<T> context) {
    final ElementInstance flowScopeInstance = context.getFlowScopeInstance();
    final WorkflowInstanceRecord flowScopeInstanceValue = flowScopeInstance.getValue();

    context
        .getOutput()
        .appendFollowUpEvent(
            flowScopeInstance.getKey(),
            WorkflowInstanceIntent.ELEMENT_COMPLETING,
            flowScopeInstanceValue);
  }
}
