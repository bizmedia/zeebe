/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.protocol.impl.record.value.error;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.agrona.DirectBuffer;

public class ErrorRecord extends UnpackedObject {
  private final StringProperty exceptionMessageProp = new StringProperty("exceptionMessage");
  private final StringProperty stacktraceProp = new StringProperty("stacktrace", "");
  private final LongProperty errorEventPositionProp = new LongProperty("errorEventPosition");

  private final LongProperty workflowInstanceKeyProp = new LongProperty("workflowInstanceKey", -1L);
  private final LongProperty workflowKeyProp = new LongProperty("workflowKey", -1L);
  private final StringProperty bpmnProcessIdProp = new StringProperty("bpmnProcessId", "");

  private final StringProperty elementIdProp = new StringProperty("elementId", "");
  private final LongProperty elementInstanceKeyProp = new LongProperty("elementInstanceKey", -1L);

  public ErrorRecord() {
    this.declareProperty(exceptionMessageProp)
        .declareProperty(stacktraceProp)
        .declareProperty(errorEventPositionProp)
        .declareProperty(workflowInstanceKeyProp)
        .declareProperty(workflowKeyProp)
        .declareProperty(bpmnProcessIdProp)
        .declareProperty(elementIdProp)
        .declareProperty(elementInstanceKeyProp);
  }

  public void initErrorRecord(Throwable throwable, long position) {
    reset();

    final StringWriter stringWriter = new StringWriter();
    final PrintWriter pw = new PrintWriter(stringWriter);
    throwable.printStackTrace(pw);

    stacktraceProp.setValue(stringWriter.toString());
    exceptionMessageProp.setValue(throwable.getMessage());
    errorEventPositionProp.setValue(position);
  }

  public DirectBuffer getExceptionMessage() {
    return exceptionMessageProp.getValue();
  }

  public DirectBuffer getStacktrace() {
    return stacktraceProp.getValue();
  }

  public long getErrorEventPosition() {
    return errorEventPositionProp.getValue();
  }

  public DirectBuffer getBpmnProcessId() {
    return bpmnProcessIdProp.getValue();
  }

  public ErrorRecord setBpmnProcessId(DirectBuffer directBuffer) {
    bpmnProcessIdProp.setValue(directBuffer, 0, directBuffer.capacity());
    return this;
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKeyProp.getValue();
  }

  public ErrorRecord setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKeyProp.setValue(workflowInstanceKey);
    return this;
  }

  public long getWorkflowKey() {
    return workflowKeyProp.getValue();
  }

  public ErrorRecord setWorkflowKey(long workflowInstanceKey) {
    this.workflowKeyProp.setValue(workflowInstanceKey);
    return this;
  }

  public DirectBuffer getElementId() {
    return elementIdProp.getValue();
  }

  public ErrorRecord setElementId(DirectBuffer elementId) {
    this.elementIdProp.setValue(elementId, 0, elementId.capacity());
    return this;
  }

  public long getElementInstanceKey() {
    return elementInstanceKeyProp.getValue();
  }

  public ErrorRecord setElementInstanceKey(long elementInstanceKey) {
    this.elementInstanceKeyProp.setValue(elementInstanceKey);
    return this;
  }
}
