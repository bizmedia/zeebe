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
package io.zeebe.broker.workflow.processor.handlers.gateway;

import io.zeebe.broker.workflow.model.element.ExecutableEventBasedGateway;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.processor.handlers.ElementStateHandler;

public class EventBasedGatewayElementActivatingHandler<T extends ExecutableEventBasedGateway>
    extends AbstractHandler<T> {
  private final ElementStateHandler<T> elementActivatingHandler;
  private final ElementStateHandler<T> catchEventSupplierActivatingHandler;

  public EventBasedGatewayElementActivatingHandler(
      ElementStateHandler<T> elementActivatingHandler,
      ElementStateHandler<T> catchEventSupplierActivatingHandler) {
    this.elementActivatingHandler = elementActivatingHandler;
    this.catchEventSupplierActivatingHandler = catchEventSupplierActivatingHandler;
  }

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    if (elementActivatingHandler.handleState(context)) {
      catchEventSupplierActivatingHandler.handleState(context);
    }
  }
}
