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
package io.zeebe.db.impl;

import static io.zeebe.db.impl.rocksdb.transaction.ZeebeTransactionDb.ZERO_SIZE_ARRAY;

import io.zeebe.db.DbContext;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.impl.rocksdb.transaction.ZeebeTransaction;
import java.util.ArrayDeque;
import java.util.Queue;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class DefaultDbContext implements DbContext {
  private ZeebeTransaction currentTransaction;

  // we can also simply use one buffer
  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);

  private final Queue<ExpandableArrayBuffer> prefixKeyBuffers;

  public DefaultDbContext() {
    prefixKeyBuffers = new ArrayDeque<>();
    prefixKeyBuffers.add(new ExpandableArrayBuffer());
    prefixKeyBuffers.add(new ExpandableArrayBuffer());
  }

  @Override
  public void writeKey(DbKey key) {
    key.write(keyBuffer, 0);
  }

  @Override
  public void writeValue(DbValue value) {
    value.write(valueBuffer, 0);
  }

  @Override
  public byte[] getKeyBufferArray() {
    return keyBuffer.byteArray();
  }

  @Override
  public byte[] getValueBufferArray() {
    return valueBuffer.byteArray();
  }

  @Override
  public boolean isInCurrentTransaction() {
    return currentTransaction != null;
  }

  @Override
  public ZeebeTransaction getCurrentTransaction() {
    return currentTransaction;
  }

  @Override
  public void setCurrentTransaction(ZeebeTransaction currentTransaction) {
    this.currentTransaction = currentTransaction;
  }

  @Override
  public void closeTransaction() {
    if (currentTransaction != null) {
      currentTransaction.close();
      currentTransaction = null;
    }
  }

  @Override
  public void wrapKeyView(byte[] key) {
    if (key != null) {
      keyViewBuffer.wrap(key);
    } else {
      keyViewBuffer.wrap(ZERO_SIZE_ARRAY);
    }
  }

  @Override
  public DirectBuffer getKeyView() {
    return isKeyViewEmpty() ? null : keyViewBuffer;
  }

  @Override
  public boolean isKeyViewEmpty() {
    return keyViewBuffer.capacity() == ZERO_SIZE_ARRAY.length;
  }

  @Override
  public void wrapValueView(byte[] value) {
    if (value != null) {
      valueViewBuffer.wrap(value);
    } else {
      valueViewBuffer.wrap(ZERO_SIZE_ARRAY);
    }
  }

  @Override
  public DirectBuffer getValueView() {
    return isValueViewEmpty() ? null : valueViewBuffer;
  }

  @Override
  public boolean isValueViewEmpty() {
    return valueViewBuffer.capacity() == ZERO_SIZE_ARRAY.length;
  }

  @Override
  public void ensurePrefixKeyBufferAvailable() {
    if (prefixKeyBuffers.peek() == null) {
      throw new IllegalStateException(
          "Currently nested prefix iterations are not supported! This will cause unexpected behavior.");
    }
  }

  @Override
  public ExpandableArrayBuffer getPrefixKeyBuffer() {
    return prefixKeyBuffers.remove();
  }

  @Override
  public void returnPrefixKeyBuffer(ExpandableArrayBuffer prefixKeyBuffer) {
    prefixKeyBuffers.add(prefixKeyBuffer);
  }
}
