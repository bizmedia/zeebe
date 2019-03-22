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

import io.zeebe.broker.workflow.model.element.ExecutableCatchEvent;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEventElement;
import io.zeebe.broker.workflow.processor.handlers.element.ElementHandlerTestCase;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.EventScopeInstanceState;
import io.zeebe.broker.workflow.state.EventTrigger;
import io.zeebe.broker.workflow.state.VariablesState;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.Strings;
import java.util.Collections;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class CatchEventEventOccurredHandlerTest
    extends ElementHandlerTestCase<ExecutableCatchEvent> {
  private CatchEventEventOccurredHandler<ExecutableCatchEvent> handler;
  private final EventScopeInstanceState eventState =
      zeebeStateRule.getZeebeState().getWorkflowState().getEventScopeInstanceState();
  private final VariablesState variablesState =
      zeebeStateRule
          .getZeebeState()
          .getWorkflowState()
          .getElementInstanceState()
          .getVariablesState();

  @Override
  @Before
  public void setUp() {
    super.setUp();
    handler = new CatchEventEventOccurredHandler<>();
  }

  @Test
  public void shouldProcessEventTrigger() {
    // given
    final ExecutableCatchEvent element =
        new ExecutableCatchEventElement(Strings.newRandomValidBpmnId());
    final ElementInstance instance =
        createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    final EventTrigger eventTrigger =
        createEventTrigger(instance.getKey(), element.getId(), MsgPackUtil.asMsgPack("foo", "bar"));
    context.setElement(element);
    context.getRecord().getMetadata().intent(WorkflowInstanceIntent.EVENT_OCCURRED);

    // when
    final boolean handled = handler.handleState(context);

    // then
    assertThat(handled).isTrue();
    assertThat(variablesState.getPayload(instance.getKey())).isEqualTo(eventTrigger.getPayload());
    assertThat(eventState.peekEventTrigger(instance.getKey())).isNull();
  }

  @Test
  public void shouldReturnUnhandledWhenNoEventTrigger() {
    // given
    final ExecutableCatchEvent element =
        new ExecutableCatchEventElement(Strings.newRandomValidBpmnId());
    createAndSetContextElementInstance(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    context.setElement(element);
    context.getRecord().getMetadata().intent(WorkflowInstanceIntent.EVENT_OCCURRED);

    // when
    final boolean handled = handler.handleState(context);

    // then
    assertThat(handled).isFalse();
  }

  private EventTrigger createEventTrigger(
      long scopeKey, DirectBuffer elementId, DirectBuffer payload) {
    final long eventKey = zeebeStateRule.getKeyGenerator().nextKey();
    final EventTrigger trigger =
        new EventTrigger().setElementId(elementId).setPayload(payload).setEventKey(eventKey);

    eventState.createIfNotExists(scopeKey, Collections.emptyList());
    eventState.triggerEvent(
        scopeKey, trigger.getEventKey(), trigger.getElementId(), trigger.getPayload());

    return trigger;
  }
}
