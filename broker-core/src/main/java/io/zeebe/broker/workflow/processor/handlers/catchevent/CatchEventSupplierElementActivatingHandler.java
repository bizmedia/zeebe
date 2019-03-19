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
import io.zeebe.broker.workflow.processor.handlers.IOMappingHelper;
import io.zeebe.broker.workflow.processor.handlers.element.ElementActivatingHandler;
import io.zeebe.broker.workflow.processor.message.MessageCorrelationKeyException;
import io.zeebe.protocol.ErrorType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class CatchEventSupplierElementActivatingHandler<T extends ExecutableCatchEventSupplier>
    extends ElementActivatingHandler<T> {
  public CatchEventSupplierElementActivatingHandler() {
    super();
  }

  public CatchEventSupplierElementActivatingHandler(WorkflowInstanceIntent nextState) {
    super(nextState);
  }

  public CatchEventSupplierElementActivatingHandler(
      WorkflowInstanceIntent nextState, IOMappingHelper ioMappingHelper) {
    super(nextState, ioMappingHelper);
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    if (super.handleState(context)) {
      try {
        context.getCatchEventBehavior().subscribeToEvents(context, context.getElement());
        return true;
      } catch (MessageCorrelationKeyException e) {
        context.raiseIncident(
            ErrorType.EXTRACT_VALUE_ERROR, e.getContext().getVariablesScopeKey(), e.getMessage());
      }
    }

    return false;
  }
}
