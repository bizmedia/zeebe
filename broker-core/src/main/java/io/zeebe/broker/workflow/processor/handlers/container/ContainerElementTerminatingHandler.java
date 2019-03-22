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

import io.zeebe.broker.incident.processor.IncidentState;
import io.zeebe.broker.workflow.model.element.ExecutableFlowElementContainer;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.EventOutput;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.ElementInstanceState;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.List;

public class ContainerElementTerminatingHandler<T extends ExecutableFlowElementContainer>
    extends AbstractHandler<T> {
  private final IncidentState incidentState;

  public ContainerElementTerminatingHandler(IncidentState incidentState) {
    this.incidentState = incidentState;
  }

  @Override
  public void handleRecord(BpmnStepContext<T> context) {
    final ElementInstance elementInstance = context.getElementInstance();
    final EventOutput output = context.getOutput();
    final ElementInstanceState elementInstanceState = context.getElementInstanceState();
    final int childCount = context.getElementInstance().getNumberOfActiveElementInstances();

    if (childCount == 0) {
      elementInstanceState.visitFailedRecords(
          elementInstance.getKey(),
          (token) -> {
            incidentState.forExistingWorkflowIncident(
                token.getKey(),
                (incident, key) -> context.getOutput().appendResolvedIncidentEvent(key, incident));
          });

      transitionTo(context, WorkflowInstanceIntent.ELEMENT_TERMINATED);
    } else {
      terminateChildren(elementInstance, output, elementInstanceState);
    }
  }

  private void terminateChildren(
      ElementInstance elementInstance,
      EventOutput output,
      ElementInstanceState elementInstanceState) {
    final List<ElementInstance> children =
        elementInstanceState.getChildren(elementInstance.getKey());
    for (final ElementInstance child : children) {
      if (child.canTerminate()) {
        terminateChild(output, child);
      }
    }
  }

  private void terminateChild(EventOutput output, ElementInstance child) {
    output.appendFollowUpEvent(
        child.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATING, child.getValue());
  }
}
