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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.StoredRecord.Purpose;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ElementCompletedHandlerTest extends ElementHandlerTestCase<ExecutableFlowNode> {
  private ElementCompletedHandler<ExecutableFlowNode> handler;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new ElementCompletedHandler<>();
  }

  @Test
  public void shouldNotHandleStateIfNoElementGiven() {
    // given
    context.setElementInstance(null);

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldNotHandleStateIfInMemoryStateIsDifferent() {
    // given
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED);
    instance.setState(WorkflowInstanceIntent.ELEMENT_TERMINATED);

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldNotHandleStateIfFlowScopeIsNotActivated() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED, flowScope);

    // when - then
    assertThat(handler.shouldHandleState(context)).isFalse();
  }

  @Test
  public void shouldHandleStateIfElementIsRootFlowScope() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED);

    // when - then
    assertThat(handler.shouldHandleState(context)).isTrue();
  }

  @Test
  public void shouldPublishDeferredRecords() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    final int currentTokens = flowScope.getNumberOfActiveTokens();
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED, flowScope);
    final ElementInstance deferred = newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATING);
    context
        .getElementInstanceState()
        .storeRecord(
            deferred.getKey(),
            instance.getKey(),
            deferred.getValue(),
            deferred.getState(),
            Purpose.DEFERRED);

    // when
    handler.handleState(context);

    // then
    assertThat(flowScope.getNumberOfActiveTokens()).isEqualTo(currentTokens + 1);
    verify(eventOutput, times(1))
        .appendFollowUpEvent(deferred.getKey(), deferred.getState(), deferred.getValue());
  }

  @Test
  public void shouldCompleteFlowScopeIfLastActiveElement() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED, flowScope);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, times(1))
        .appendFollowUpEvent(
            flowScope.getKey(), WorkflowInstanceIntent.ELEMENT_COMPLETING, flowScope.getValue());
  }

  @Test
  public void shouldNotCompleteFlowScopeIfNonePresent() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, never())
        .appendFollowUpEvent(
            anyLong(), any(WorkflowInstanceIntent.class), any(WorkflowInstanceRecord.class));
  }

  @Test
  public void shouldNotCompleteFlowScopeIfMoreActiveElementsPresent() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_COMPLETED, flowScope);
    newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED, flowScope);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, never())
        .appendFollowUpEvent(
            eq(flowScope.getKey()), any(WorkflowInstanceIntent.class), eq(flowScope.getValue()));
  }
}
