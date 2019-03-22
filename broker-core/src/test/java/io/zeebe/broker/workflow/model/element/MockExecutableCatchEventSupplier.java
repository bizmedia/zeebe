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
package io.zeebe.broker.workflow.model.element;

import io.zeebe.test.util.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.agrona.DirectBuffer;

public class MockExecutableCatchEventSupplier extends ExecutableFlowNodeImpl
    implements ExecutableCatchEventSupplier {
  private final List<ExecutableCatchEvent> events;
  private final List<DirectBuffer> interruptingElementIds;

  public MockExecutableCatchEventSupplier() {
    this(Strings.newRandomValidBpmnId(), new ArrayList<>(), new ArrayList<>());
  }

  public MockExecutableCatchEventSupplier(
      String id, List<ExecutableCatchEvent> events, List<DirectBuffer> interruptingElementIds) {
    super(id);
    this.events = events;
    this.interruptingElementIds = interruptingElementIds;
  }

  @Override
  public List<ExecutableCatchEvent> getEvents() {
    return events;
  }

  public void addEvent(ExecutableCatchEvent event) {
    addEvent(event, false);
  }

  public void addEvent(ExecutableCatchEvent event, boolean interrupting) {
    events.add(event);
    if (interrupting) {
      interruptingElementIds.add(event.getId());
    }
  }

  @Override
  public Collection<DirectBuffer> getInterruptingElementIds() {
    return interruptingElementIds;
  }
}
