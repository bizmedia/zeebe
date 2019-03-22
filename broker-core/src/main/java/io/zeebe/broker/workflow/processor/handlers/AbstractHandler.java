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
package io.zeebe.broker.workflow.processor.handlers;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.WorkflowInstanceLifecycle;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public abstract class AbstractHandler<T extends ExecutableFlowElement>
    implements BpmnStepHandler<T> {
  /**
   * Delegates handling the state logic to subclasses, and moves to the next state iff there is a
   * next state to move to and it is a synchronous handler.
   *
   * @param context current step context
   */
  @Override
  public void handle(BpmnStepContext<T> context) {
    if (!shouldHandle(context)) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.debug(
          "Skipping record {} due to step guard; in-memory element is {}",
          context.getRecord(),
          context.getElementInstance());
      return;
    }

    handleRecord(context);
  }

  /**
   * Called when the handler is the concrete, specialized handler for the current record. To compose
   * handlers together, {@link AbstractHandler#handleState(BpmnStepContext)} should be called.
   *
   * @param context current step context
   */
  protected abstract void handleRecord(BpmnStepContext<T> context);

  /**
   * To be overridden by subclasses.
   *
   * @param context current step context
   * @return true if ready successful, false otherwise
   */
  protected boolean handleState(BpmnStepContext<T> context) {
    return true;
  }

  /**
   * Describes guard clauses for the current step.
   *
   * @param context current step context
   * @return true if the event should be processed, false otherwise
   */
  protected boolean shouldHandle(BpmnStepContext<T> context) {
    switch (context.getState()) {
      case SEQUENCE_FLOW_TAKEN:
        return isElementActive(context.getFlowScopeInstance());
      case ELEMENT_ACTIVATING:
      case ELEMENT_ACTIVATED:
      case ELEMENT_COMPLETING:
      case ELEMENT_COMPLETED:
        return isStateSameAsElementState(context)
            && (isRootScope(context) || isElementActive(context.getFlowScopeInstance()));
      case ELEMENT_TERMINATING:
      case ELEMENT_TERMINATED:
        return isStateSameAsElementState(context);
      case EVENT_OCCURRED:
        return (context.getRecord().getValue().getWorkflowInstanceKey() == -1
            || isElementActive(context.getElementInstance()));
      default:
        return true;
    }
  }

  private boolean isRootScope(BpmnStepContext<T> context) {
    return context.getRecord().getValue().getFlowScopeKey() == -1;
  }

  /**
   * Returns true if the current record intent is the same as the element's current state. This is
   * primarily to ensure we're not processing concurrent state transitions (e.g. writing
   * ELEMENT_ACTIVATING and ELEMENT_ACTIVATED in the same step will transition the element to
   * ACTIVATED, and we shouldn't process the ELEMENT_ACTIVATING in that case).
   */
  private boolean isStateSameAsElementState(BpmnStepContext<T> context) {
    return context.getElementInstance() != null
        && context.getState() == context.getElementInstance().getState();
  }

  private boolean isElementActive(ElementInstance instance) {
    return instance != null && instance.isActive();
  }

  protected boolean isElementTerminating(ElementInstance instance) {
    return instance != null && instance.isTerminating();
  }

  protected void transitionTo(BpmnStepContext<T> context, WorkflowInstanceIntent nextState) {
    final WorkflowInstanceIntent state = context.getElementInstance().getState();

    assert WorkflowInstanceLifecycle.canTransition(state, nextState)
        : String.format("cannot transition from '%s' to '%s'", state, nextState);

    context.getElementInstance().setState(nextState);
    context
        .getOutput()
        .appendFollowUpEvent(context.getRecord().getKey(), nextState, context.getValue());

    // todo: this is an ugly workaround which should be removed once we have a better workflow
    // instance state abstraction: essentially, whenever transitioning to a terminating state, we
    // want to reject any potential event triggers
    // https://github.com/zeebe-io/zeebe/issues/1980
    if (nextState == WorkflowInstanceIntent.ELEMENT_COMPLETING
        || nextState == WorkflowInstanceIntent.ELEMENT_TERMINATING) {
      context
          .getStateDb()
          .getEventScopeInstanceState()
          .shutdownInstance(context.getRecord().getKey());
    }
  }
}
