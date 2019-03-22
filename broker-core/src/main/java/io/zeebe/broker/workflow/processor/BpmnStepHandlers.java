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
package io.zeebe.broker.workflow.processor;

import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.workflow.model.BpmnStep;
import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import io.zeebe.broker.workflow.processor.handlers.activity.ActivityElementTerminatedHandler;
import io.zeebe.broker.workflow.processor.handlers.activity.ActivityEventOccurredHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.CatchEventEventOccurredHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.CatchEventSupplierElementActivatingHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.CatchEventSupplierElementCompletingHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.CatchEventSupplierElementTerminatingHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.IntermediateCatchEventElementActivatedHandler;
import io.zeebe.broker.workflow.processor.handlers.catchevent.StartEventEventOccurredHandler;
import io.zeebe.broker.workflow.processor.handlers.container.ContainerElementActivatedHandler;
import io.zeebe.broker.workflow.processor.handlers.container.ContainerElementTerminatingHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementActivatedHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementActivatingHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementCompletedHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementCompletingHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementTerminatedHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementTerminatingHandler;
import io.zeebe.broker.workflow.processor.handlers.element.EventOccurredHandler;
import io.zeebe.broker.workflow.processor.handlers.gateway.EventBasedGatewayElementActivatingHandler;
import io.zeebe.broker.workflow.processor.handlers.gateway.EventBasedGatewayElementCompletingHandler;
import io.zeebe.broker.workflow.processor.handlers.gateway.EventBasedGatewayElementTerminatingHandler;
import io.zeebe.broker.workflow.processor.handlers.gateway.EventBasedGatewayEventOccurredHandler;
import io.zeebe.broker.workflow.processor.handlers.gateway.ExclusiveGatewayElementActivatingHandler;
import io.zeebe.broker.workflow.processor.handlers.seqflow.FlowOutElementCompletedHandler;
import io.zeebe.broker.workflow.processor.handlers.seqflow.ParallelMergeSequenceFlowTaken;
import io.zeebe.broker.workflow.processor.handlers.seqflow.SequenceFlowTakenHandler;
import io.zeebe.broker.workflow.processor.handlers.servicetask.ServiceTaskElementActivatedHandler;
import io.zeebe.broker.workflow.processor.handlers.servicetask.ServiceTaskElementTerminatingHandler;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.EnumMap;
import java.util.Map;

public class BpmnStepHandlers {
  private final Map<BpmnStep, BpmnStepHandler<?>> stepHandlers = new EnumMap<>(BpmnStep.class);

  public BpmnStepHandlers(ZeebeState state) {
    stepHandlers.put(BpmnStep.ELEMENT_ACTIVATING, new ElementActivatingHandler<>());
    stepHandlers.put(BpmnStep.ELEMENT_ACTIVATED, new ElementActivatedHandler<>());
    stepHandlers.put(BpmnStep.EVENT_OCCURRED, new EventOccurredHandler<>());
    stepHandlers.put(BpmnStep.ELEMENT_COMPLETING, new ElementCompletingHandler<>());
    stepHandlers.put(BpmnStep.ELEMENT_COMPLETED, new ElementCompletedHandler<>());
    stepHandlers.put(
        BpmnStep.ELEMENT_TERMINATING, new ElementTerminatingHandler<>(state.getIncidentState()));
    stepHandlers.put(BpmnStep.ELEMENT_TERMINATED, new ElementTerminatedHandler<>());
    stepHandlers.put(BpmnStep.FLOWOUT_ELEMENT_COMPLETED, new FlowOutElementCompletedHandler<>());

    stepHandlers.put(
        BpmnStep.ACTIVITY_ELEMENT_ACTIVATING, new CatchEventSupplierElementActivatingHandler<>());
    stepHandlers.put(BpmnStep.ACTIVITY_EVENT_OCCURRED, new ActivityEventOccurredHandler<>());
    stepHandlers.put(
        BpmnStep.ACTIVITY_ELEMENT_COMPLETING, new CatchEventSupplierElementCompletingHandler<>());
    stepHandlers.put(
        BpmnStep.ACTIVITY_ELEMENT_TERMINATING,
        new CatchEventSupplierElementTerminatingHandler<>(state.getIncidentState()));
    stepHandlers.put(
        BpmnStep.ACTIVITY_ELEMENT_TERMINATED, new ActivityElementTerminatedHandler<>());

    stepHandlers.put(
        BpmnStep.CONTAINER_ELEMENT_ACTIVATED,
        new ContainerElementActivatedHandler<>(state.getWorkflowState()));
    stepHandlers.put(
        BpmnStep.CONTAINER_ELEMENT_TERMINATING,
        new ContainerElementTerminatingHandler<>(state.getIncidentState()));

    stepHandlers.put(
        BpmnStep.EVENT_BASED_GATEWAY_ELEMENT_ACTIVATING,
        new EventBasedGatewayElementActivatingHandler<>());
    stepHandlers.put(
        BpmnStep.EVENT_BASED_GATEWAY_ELEMENT_ACTIVATED, new ElementActivatedHandler<>(null));
    stepHandlers.put(
        BpmnStep.EVENT_BASED_GATEWAY_EVENT_OCCURRED, new EventBasedGatewayEventOccurredHandler<>());
    stepHandlers.put(
        BpmnStep.EVENT_BASED_GATEWAY_ELEMENT_COMPLETING,
        new EventBasedGatewayElementCompletingHandler<>());
    stepHandlers.put(
        BpmnStep.EVENT_BASED_GATEWAY_ELEMENT_TERMINATING,
        new EventBasedGatewayElementTerminatingHandler<>(state.getIncidentState()));

    stepHandlers.put(
        BpmnStep.EXCLUSIVE_GATEWAY_ELEMENT_ACTIVATING,
        new ExclusiveGatewayElementActivatingHandler<>());

    stepHandlers.put(
        BpmnStep.INTERMEDIATE_CATCH_EVENT_ELEMENT_ACTIVATING,
        new CatchEventSupplierElementActivatingHandler<>());
    stepHandlers.put(
        BpmnStep.INTERMEDIATE_CATCH_EVENT_ELEMENT_ACTIVATED,
        new IntermediateCatchEventElementActivatedHandler<>());
    stepHandlers.put(
        BpmnStep.INTERMEDIATE_CATCH_EVENT_EVENT_OCCURRED, new CatchEventEventOccurredHandler<>());
    stepHandlers.put(
        BpmnStep.INTERMEDIATE_CATCH_EVENT_ELEMENT_COMPLETING,
        new CatchEventSupplierElementCompletingHandler<>());
    stepHandlers.put(
        BpmnStep.INTERMEDIATE_CATCH_EVENT_ELEMENT_TERMINATING,
        new CatchEventSupplierElementTerminatingHandler<>(state.getIncidentState()));

    stepHandlers.put(BpmnStep.RECEIVE_TASK_ELEMENT_ACTIVATED, new ElementActivatedHandler<>(null));
    stepHandlers.put(
        BpmnStep.RECEIVE_TASK_EVENT_OCCURRED,
        new ActivityEventOccurredHandler<>(WorkflowInstanceIntent.ELEMENT_COMPLETING));

    stepHandlers.put(
        BpmnStep.SERVICE_TASK_ELEMENT_ACTIVATED, new ServiceTaskElementActivatedHandler<>());
    stepHandlers.put(
        BpmnStep.SERVICE_TASK_ELEMENT_TERMINATING,
        new ServiceTaskElementTerminatingHandler<>(state.getIncidentState(), state.getJobState()));

    stepHandlers.put(
        BpmnStep.START_EVENT_EVENT_OCCURRED,
        new StartEventEventOccurredHandler<>(state.getWorkflowState()));

    stepHandlers.put(
        BpmnStep.PARALLEL_MERGE_SEQUENCE_FLOW_TAKEN, new ParallelMergeSequenceFlowTaken<>());
    stepHandlers.put(BpmnStep.SEQUENCE_FLOW_TAKEN, new SequenceFlowTakenHandler<>());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void handle(BpmnStepContext context) {
    final ExecutableFlowElement flowElement = context.getElement();
    final WorkflowInstanceIntent state =
        (WorkflowInstanceIntent) context.getRecord().getMetadata().getIntent();
    final BpmnStep step = flowElement.getStep(state);

    if (step != null) {
      final BpmnStepHandler stepHandler = stepHandlers.get(step);
      assert stepHandler != null : "no step handler configured for step " + step.toString();
      stepHandler.handle(context);
    }
  }
}
