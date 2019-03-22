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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.incident.processor.IncidentState;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.ErrorType;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ElementTerminatingHandlerTest extends ElementHandlerTestCase {
  private ElementTerminatingHandler<ExecutableFlowNode> handler;
  private final IncidentState incidentState = zeebeStateRule.getZeebeState().getIncidentState();

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new ElementTerminatingHandler<>(incidentState);
  }

  @Test
  public void shouldNotHandleStateIfNoElementGiven() {
    // given

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldResolvePendingIncidents() {
    // given
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    final long incidentKey = zeebeStateRule.getKeyGenerator().nextKey();
    final IncidentRecord incident =
        new IncidentRecord()
            .setElementInstanceKey(instance.getKey())
            .setVariableScopeKey(instance.getKey())
            .setErrorMessage("message")
            .setErrorType(ErrorType.IO_MAPPING_ERROR)
            .setBpmnProcessId(instance.getValue().getBpmnProcessId())
            .setElementId(instance.getValue().getElementId())
            .setWorkflowInstanceKey(instance.getValue().getWorkflowInstanceKey());
    incidentState.createIncident(incidentKey, incident);

    // when
    final boolean handled = handler.handleState(context);

    // then
    assertThat(handled).isTrue();
    verify(eventOutput, times(1)).appendResolvedIncidentEvent(eq(incidentKey), eq(incident));
  }
}
