package io.zeebe.broker.workflow.processor.handlers.receivetask;

import io.zeebe.broker.workflow.model.element.ExecutableReceiveTask;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.ElementStateHandler;
import io.zeebe.broker.workflow.processor.handlers.element.EventOccurredHandler;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ReceiveTaskEventOccurredHandler<T extends ExecutableReceiveTask>
    extends EventOccurredHandler<T> {
  private final ElementStateHandler<T> catchEventElementStateHandler;
  private final ElementStateHandler<T> activityElementStateHandler;

  public ReceiveTaskEventOccurredHandler(
      ElementStateHandler<T> catchEventElementStateHandler,
      ElementStateHandler<T> activityElementStateHandler) {
    this.catchEventElementStateHandler = catchEventElementStateHandler;
    this.activityElementStateHandler = activityElementStateHandler;
  }

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    if (catchEventElementStateHandler.handleState(context)) {
      transitionTo(context, WorkflowInstanceIntent.ELEMENT_COMPLETING);
    } else {
      activityElementStateHandler.handleState(context);
    }
  }
}
