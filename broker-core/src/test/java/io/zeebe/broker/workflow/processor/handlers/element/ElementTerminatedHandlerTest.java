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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ElementTerminatedHandlerTest extends ElementHandlerTestCase<ExecutableFlowNode> {
  private ElementTerminatedHandler<ExecutableFlowNode> handler;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new ElementTerminatedHandler<>();
  }

  @Test
  public void shouldNotHandleStateIfNoElementGiven() {
    // given
    context.setElementInstance(null);

    // when - then
    assertThat(handler.shouldHandle(context)).isFalse();
  }

  @Test
  public void shouldNotHandleStateIfInMemoryStateIsDifferent() {
    // given
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED);
    instance.setState(WorkflowInstanceIntent.ELEMENT_COMPLETING);

    // when - then
    assertThat(handler.shouldHandle(context)).isFalse();
  }

  @Test
  public void shouldTerminateScope() {
    // given
    final ElementInstance flowScope =
        newElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, times(1))
        .appendFollowUpEvent(
            flowScope.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope.getValue());
  }

  @Test
  public void shouldNotTerminateScopeIfRootScope() {
    // given
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, never())
        .appendFollowUpEvent(
            anyLong(), any(WorkflowInstanceIntent.class), any(WorkflowInstanceRecord.class));
  }

  @Test
  public void shouldNotTerminateScopeIfScopeActive() {
    // given
    final ElementInstance flowScope = newElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, never())
        .appendFollowUpEvent(
            flowScope.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope.getValue());
  }

  @Test
  public void shouldNotTerminateScopeIfActiveSiblingElements() {
    // given
    final ElementInstance flowScope =
        newElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATING);
    newElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATING, flowScope);
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope);

    // when
    handler.handleState(context);

    // then
    verify(eventOutput, never())
        .appendFollowUpEvent(
            flowScope.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATED, flowScope.getValue());
  }
}
