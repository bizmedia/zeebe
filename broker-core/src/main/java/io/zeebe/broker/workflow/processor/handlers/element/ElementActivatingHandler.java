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

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.processor.handlers.ElementStateHandler;
import io.zeebe.broker.workflow.processor.handlers.IOMappingHelper;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.ErrorType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * Applies input mappings in the scope.
 *
 * @param <T>
 */
public class ElementActivatingHandler<T extends ExecutableFlowNode> extends AbstractHandler<T>
    implements ElementStateHandler<T> {
  private final IOMappingHelper ioMappingHelper;

  public ElementActivatingHandler() {
    this(new IOMappingHelper());
  }

  public ElementActivatingHandler(IOMappingHelper ioMappingHelper) {
    this.ioMappingHelper = ioMappingHelper;
  }

  @Override
  protected void handleRecord(BpmnStepContext<T> context) {
    if (handleState(context)) {
      transitionTo(context, WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    }
  }

  @Override
  public boolean handleState(BpmnStepContext<T> context) {
    try {
      ioMappingHelper.applyInputMappings(context);
      return true;
    } catch (MappingException e) {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, e.getMessage());
    }

    return false;
  }
}
