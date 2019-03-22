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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.workflow.model.element.ExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.model.element.MockExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.processor.handlers.element.ElementHandlerTestCase;
import io.zeebe.broker.workflow.processor.message.MessageCorrelationKeyContext;
import io.zeebe.broker.workflow.processor.message.MessageCorrelationKeyException;
import io.zeebe.protocol.ErrorType;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.buffer.BufferUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class CatchEventSupplierElementActivatingHandlerTest
    extends ElementHandlerTestCase<ExecutableCatchEventSupplier> {
  private CatchEventSupplierElementActivatingHandler<ExecutableCatchEventSupplier> handler;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new CatchEventSupplierElementActivatingHandler<>();
  }

  @Test
  public void shouldSubscribeToEvents() {
    // given
    final MockExecutableCatchEventSupplier element = new MockExecutableCatchEventSupplier();
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATING);
    context.setElement(element);

    // when
    final boolean handled = handler.handleState(context);

    // then
    verify(catchEventBehavior, times(1)).subscribeToEvents(context, element);
    assertThat(handled).isTrue();
  }

  @Test
  public void shouldRaiseIncidentOnCorrelationException() {
    // given
    final long variableScopeKey = 1L;
    final MessageCorrelationKeyContext correlationContext =
        new MessageCorrelationKeyContext(null, variableScopeKey);
    final MessageCorrelationKeyException exception =
        new MessageCorrelationKeyException(correlationContext, "fail");
    final MockExecutableCatchEventSupplier element = new MockExecutableCatchEventSupplier();
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATING);
    context.setElement(element);

    // when
    doThrow(exception).when(catchEventBehavior).subscribeToEvents(context, element);
    final boolean handled = handler.handleState(context);

    // then
    final IncidentRecord raisedIncident = getRaisedIncident();
    assertThat(handled).isFalse();
    assertThat(raisedIncident.getErrorType()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR);
    assertThat(raisedIncident.getVariableScopeKey()).isEqualTo(variableScopeKey);
    assertThat(raisedIncident.getErrorMessage())
        .isEqualTo(BufferUtil.wrapString(exception.getMessage()));
  }
}
