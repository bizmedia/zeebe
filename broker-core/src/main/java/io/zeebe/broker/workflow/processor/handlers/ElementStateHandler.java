package io.zeebe.broker.workflow.processor.handlers;

import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;

public interface ElementStateHandler<T extends ExecutableFlowElement> {
  /**
   * @param context current step context
   * @return true if handled successfully, false if an error occurred
   */
  boolean handleState(BpmnStepContext<T> context);
}
