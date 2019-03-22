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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.util.MockTypedRecord;
import io.zeebe.broker.util.ZeebeStateRule;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.CatchEventBehavior;
import io.zeebe.broker.workflow.processor.EventOutput;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.ClassRule;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public abstract class ElementHandlerTestCase {
  @ClassRule public static ZeebeStateRule zeebeStateRule = new ZeebeStateRule();

  @Mock public EventOutput eventOutput;
  @Mock public CatchEventBehavior catchEventBehavior;
  @Mock public TypedStreamWriter streamWriter;
  @Captor public ArgumentCaptor<IncidentRecord> incidentCaptor;

  protected BpmnStepContext<ExecutableFlowNode> context;

  @Before
  public void setUp() {
    context =
        new BpmnStepContext<>(
            zeebeStateRule.getZeebeState().getWorkflowState(), eventOutput, catchEventBehavior);
    context.setStreamWriter(streamWriter);
  }

  protected IncidentRecord getRaisedIncident() {
    verifyIncidentRaised();
    return incidentCaptor.getValue();
  }

  protected void verifyIncidentRaised() {
    verify(eventOutput, times(1)).storeFailedRecord(context.getRecord());
    verify(streamWriter, times(1))
        .appendNewCommand(eq(IncidentIntent.CREATE), incidentCaptor.capture());
  }

  protected ElementInstance createAndSetContextElementInstance(WorkflowInstanceIntent state) {
    final ElementInstance instance = newElementInstance(state);
    setContextElementInstance(instance);

    return instance;
  }

  protected ElementInstance createAndSetContextElementInstance(
      WorkflowInstanceIntent state, ElementInstance flowScope) {
    final ElementInstance instance = newElementInstance(state, flowScope);
    setContextElementInstance(instance);

    return instance;
  }

  protected void setContextElementInstance(ElementInstance instance) {
    context.setRecord(newRecordFor(instance));
  }

  protected ElementInstance newElementInstance(WorkflowInstanceIntent state) {
    final long key = zeebeStateRule.getKeyGenerator().nextKey();
    final WorkflowInstanceRecord value = new WorkflowInstanceRecord();

    return zeebeStateRule
        .getZeebeState()
        .getWorkflowState()
        .getElementInstanceState()
        .newInstance(key, value, state);
  }

  protected ElementInstance newElementInstance(
      WorkflowInstanceIntent state, ElementInstance flowScope) {
    final long key = zeebeStateRule.getKeyGenerator().nextKey();
    final WorkflowInstanceRecord value = new WorkflowInstanceRecord();
    value.setFlowScopeKey(flowScope.getKey());
    flowScope.spawnToken();

    return zeebeStateRule
        .getZeebeState()
        .getWorkflowState()
        .getElementInstanceState()
        .newInstance(flowScope, key, value, state);
  }

  protected TypedRecord<WorkflowInstanceRecord> newRecordFor(ElementInstance instance) {
    final RecordMetadata metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.WORKFLOW_INSTANCE)
            .intent(instance.getState());

    return new MockTypedRecord<>(instance.getKey(), metadata, instance.getValue());
  }
}
