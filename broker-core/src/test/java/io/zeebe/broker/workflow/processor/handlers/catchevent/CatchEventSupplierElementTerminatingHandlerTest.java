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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.workflow.model.element.ExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.model.element.MockExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.processor.handlers.element.ElementHandlerTestCase;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class CatchEventSupplierElementTerminatingHandlerTest
    extends ElementHandlerTestCase<ExecutableCatchEventSupplier> {
  private CatchEventSupplierElementTerminatingHandler<ExecutableCatchEventSupplier> handler;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler =
        new CatchEventSupplierElementTerminatingHandler<>(
            zeebeStateRule.getZeebeState().getIncidentState());
  }

  @Test
  public void shouldUnsubscribeFromEvents() {
    // given
    final MockExecutableCatchEventSupplier element = new MockExecutableCatchEventSupplier();
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    context.setElement(element);

    // when
    final boolean handled = handler.handleState(context);

    // then
    verify(catchEventBehavior, times(1)).unsubscribeFromEvents(instance.getKey(), context);
    assertThat(handled).isTrue();
  }
}
