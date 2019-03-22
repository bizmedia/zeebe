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

import io.zeebe.broker.workflow.model.element.ExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.processor.handlers.ElementStateHandler;
import io.zeebe.broker.workflow.processor.handlers.element.ElementCompletingHandler;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class CatchEventSupplierElementCompletingHandler<T extends ExecutableCatchEventSupplier>
    extends AbstractHandler<T> {
  private final ElementStateHandler<T> elementCompletingHandler;

  public CatchEventSupplierElementCompletingHandler() {
    this(new ElementCompletingHandler<>());
  }

  public CatchEventSupplierElementCompletingHandler(
      ElementStateHandler<T> elementCompletingHandler) {
    this.elementCompletingHandler = elementCompletingHandler;
  }

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    if (elementCompletingHandler.handleState(context) && handleState(context)) {
      transitionTo(context, WorkflowInstanceIntent.ELEMENT_COMPLETED);
    }
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    context.getCatchEventBehavior().unsubscribeFromEvents(context.getRecord().getKey(), context);
    return true;
  }
}
