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
package io.zeebe.db;

import io.zeebe.db.impl.rocksdb.transaction.ZeebeTransaction;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

public interface DbContext {
  void writeKey(DbKey key);

  void writeValue(DbValue value);

  byte[] getKeyBufferArray();

  byte[] getValueBufferArray();

  boolean isInCurrentTransaction();

  ZeebeTransaction getCurrentTransaction();

  void setCurrentTransaction(ZeebeTransaction currentTransaction);

  void closeTransaction();

  void wrapKeyView(byte[] key);

  DirectBuffer getKeyView();

  boolean isKeyViewEmpty();

  void wrapValueView(byte[] value);

  DirectBuffer getValueView();

  boolean isValueViewEmpty();

  void ensurePrefixKeyBufferAvailable();

  ExpandableArrayBuffer getPrefixKeyBuffer();

  void returnPrefixKeyBuffer(ExpandableArrayBuffer prefixKeyBuffer);
}
