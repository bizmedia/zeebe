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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.handlers.IOMappingHelper;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ElementCompletingHandlerTest extends ElementHandlerTestCase {
  @Mock public IOMappingHelper ioMappingHelper;

  private ElementCompletingHandler<ExecutableFlowNode> handler;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new ElementCompletingHandler<>(ioMappingHelper);
  }

  @Test
  public void shouldNotHandleStateIfNoElementGiven() {
    // given

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldNotHandleStateIfFlowScopeIsNotActivated() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETING, flowScope);

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldHandleStateIfElementIsRootFlowScope() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETING);

    // when - then
    assertThat(handler.shouldHandleState(context)).isTrue();
  }

  @Test
  public void shouldApplyOutputMappings() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETING);

    // when
    handler.handleState(context);

    // then
    verify(ioMappingHelper, times(1)).applyOutputMappings(context);
  }

  @Test
  public void shouldRaiseIncidentOnMappingException() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETING);
    final MappingException exception = new MappingException("failed");

    // when
    doThrow(exception).when(ioMappingHelper).applyOutputMappings(context);
    final boolean handled = handler.handleState(context);

    // then
    assertThat(handled).isFalse();
    verifyIncidentRaised();
  }

  @Test
  public void shouldHandleState() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETING);

    // when
    final boolean handled = handler.handleState(context);

    // then
    assertThat(handled).isTrue();
  }
}
