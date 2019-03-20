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
package io.zeebe.db.impl.rocksdb.transaction;

import static io.zeebe.util.buffer.BufferUtil.startsWith;
import static org.rocksdb.Status.Code.Aborted;
import static org.rocksdb.Status.Code.Busy;
import static org.rocksdb.Status.Code.Expired;
import static org.rocksdb.Status.Code.IOError;
import static org.rocksdb.Status.Code.MergeInProgress;
import static org.rocksdb.Status.Code.Ok;
import static org.rocksdb.Status.Code.TimedOut;
import static org.rocksdb.Status.Code.TryAgain;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.TransactionOperation;
import io.zeebe.db.TransactionRunnable;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbException;
import io.zeebe.db.impl.DefaultDbContext;
import java.io.File;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Status;
import org.rocksdb.Status.Code;
import org.rocksdb.WriteOptions;

public class ZeebeTransactionDb<ColumnFamilyNames extends Enum<ColumnFamilyNames>>
    implements ZeebeDb<ColumnFamilyNames> {

  private static final EnumSet<Code> RECOVERABLE_ERROR_CODES =
      EnumSet.of(Ok, Aborted, Expired, IOError, Busy, TimedOut, TryAgain, MergeInProgress);

  public static final byte[] ZERO_SIZE_ARRAY = new byte[0];

  public static <ColumnFamilyNames extends Enum<ColumnFamilyNames>>
      ZeebeTransactionDb<ColumnFamilyNames> openTransactionalDb(
          final DBOptions options,
          final String path,
          final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
          final List<AutoCloseable> closables,
          Class<ColumnFamilyNames> columnFamilyTypeClass)
          throws RocksDBException {
    final EnumMap<ColumnFamilyNames, Long> columnFamilyMap = new EnumMap<>(columnFamilyTypeClass);

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final OptimisticTransactionDB optimisticTransactionDB =
        OptimisticTransactionDB.open(options, path, columnFamilyDescriptors, handles);

    final ColumnFamilyNames[] enumConstants = columnFamilyTypeClass.getEnumConstants();
    final Long2ObjectHashMap<ColumnFamilyHandle> handleToEnumMap = new Long2ObjectHashMap();
    for (int i = 0; i < handles.size(); i++) {
      columnFamilyMap.put(enumConstants[i], getNativeHandle(handles.get(i)));
      handleToEnumMap.put(getNativeHandle(handles.get(i)), handles.get(i));
    }

    final ZeebeTransactionDb<ColumnFamilyNames> db =
        new ZeebeTransactionDb<>(
            optimisticTransactionDB,
            columnFamilyMap,
            handleToEnumMap,
            closables,
            columnFamilyTypeClass);

    return db;
  }

  private static long getNativeHandle(final RocksObject object) {
    try {
      return RocksDbInternal.nativeHandle.getLong(object);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Unexpected error occurred trying to access private nativeHandle_ field", e);
    }
  }

  private final OptimisticTransactionDB optimisticTransactionDB;
  private final List<AutoCloseable> closables;
  private final Class<ColumnFamilyNames> columnFamilyNamesClass;

  private final EnumMap<ColumnFamilyNames, Long> columnFamilyMap;
  private final Long2ObjectHashMap<ColumnFamilyHandle> handelToEnumMap;

  protected ZeebeTransactionDb(
      OptimisticTransactionDB optimisticTransactionDB,
      EnumMap<ColumnFamilyNames, Long> columnFamilyMap,
      Long2ObjectHashMap<ColumnFamilyHandle> handelToEnumMap,
      List<AutoCloseable> closables,
      Class<ColumnFamilyNames> columnFamilyNamesClass) {
    this.optimisticTransactionDB = optimisticTransactionDB;
    this.columnFamilyMap = columnFamilyMap;
    this.handelToEnumMap = handelToEnumMap;
    this.closables = closables;
    this.columnFamilyNamesClass = columnFamilyNamesClass;
  }

  protected long getColumnFamilyHandle(ColumnFamilyNames columnFamily) {
    return columnFamilyMap.get(columnFamily);
  }

  @Override
  public <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyNames columnFamily,
          DbContext context,
          KeyType keyInstance,
          ValueType valueInstance) {
    return new TransactionalColumnFamily<>(this, columnFamily, context, keyInstance, valueInstance);
  }

  protected void put(long columnFamilyHandle, DbContext context, DbKey key, DbValue value) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          context.writeKey(key);
          context.writeValue(value);

          transaction.put(
              columnFamilyHandle,
              context.getKeyBufferArray(),
              key.getLength(),
              context.getValueBufferArray(),
              value.getLength());
        });
  }

  private void ensureInOpenTransaction(DbContext context, TransactionOperation runnable) {
    transaction(context, runnable);
  }

  @Override
  public void transaction(DbContext context, TransactionRunnable operations) {
    transaction(context, transaction -> operations.run());
  }

  @Override
  public void transaction(DbContext context, TransactionOperation operations) {
    try {
      if (context.isInCurrentTransaction()) {
        operations.run(context.getCurrentTransaction());
      } else {
        runInNewTransaction(context, operations);
      }
    } catch (RocksDBException rdbex) {
      final String errorMessage = "Unexpected error occurred during RocksDB transaction.";
      if (isRocksDbExceptionRecoverable(rdbex)) {
        throw new ZeebeDbException(errorMessage, rdbex);
      } else {
        throw new RuntimeException(errorMessage, rdbex);
      }

    } catch (Exception ex) {
      throw new RuntimeException(
          "Unexpected error occurred during zeebe db transaction operation.", ex);
    }
  }

  private boolean isRocksDbExceptionRecoverable(RocksDBException rdbex) {
    final Status status = rdbex.getStatus();
    return RECOVERABLE_ERROR_CODES.contains(status.getCode());
  }

  private void runInNewTransaction(DbContext context, TransactionOperation operations)
      throws Exception {
    try (WriteOptions options = new WriteOptions()) {
      final ZeebeTransaction transaction =
          new ZeebeTransaction(optimisticTransactionDB.beginTransaction(options));
      context.setCurrentTransaction(transaction);

      operations.run(transaction);

      transaction.commit();
    } finally {
      context.closeTransaction();
    }
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// GET ///////////////////////////////////
  ////////////////////////////////////////////////////////////////////

  protected DirectBuffer get(long columnFamilyHandle, DbContext context, DbKey key) {
    context.writeKey(key);
    final int keyLength = key.getLength();
    return getValue(columnFamilyHandle, context, keyLength);
  }

  private DirectBuffer getValue(long columnFamilyHandle, DbContext context, int keyLength) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          try (ReadOptions readOptions = new ReadOptions()) {
            final byte[] value =
                transaction.get(
                    columnFamilyHandle,
                    getNativeHandle(readOptions),
                    context.getKeyBufferArray(),
                    keyLength);
            context.wrapValueView(value);
          }
        });
    return context.getValueView();
  }

  protected boolean exists(long columnFamilyHandle, DbContext context, DbKey key) {
    context.wrapValueView(new byte[0]);
    ensureInOpenTransaction(
        context,
        transaction -> {
          context.writeKey(key);
          getValue(columnFamilyHandle, context, key.getLength());
        });
    return !context.isValueViewEmpty();
  }

  protected void delete(long columnFamilyHandle, DbContext context, DbKey key) {
    context.writeKey(key);

    ensureInOpenTransaction(
        context,
        transaction ->
            transaction.delete(columnFamilyHandle, context.getKeyBufferArray(), key.getLength()));
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// ITERATION /////////////////////////////
  ////////////////////////////////////////////////////////////////////

  RocksIterator newIterator(long columnFamilyHandle, DbContext context, ReadOptions options) {
    final ColumnFamilyHandle handle = handelToEnumMap.get(columnFamilyHandle);
    return context.getCurrentTransaction().newIterator(options, handle);
  }

  public <ValueType extends DbValue> void foreach(
      long columnFamilyHandle,
      DbContext context,
      ValueType iteratorValue,
      Consumer<ValueType> consumer) {
    foreach(
        columnFamilyHandle,
        context,
        (keyBuffer, valueBuffer) -> {
          iteratorValue.wrap(valueBuffer, 0, valueBuffer.capacity());
          consumer.accept(iteratorValue);
        });
  }

  public <KeyType extends DbKey, ValueType extends DbValue> void foreach(
      long columnFamilyHandle,
      DbContext context,
      KeyType iteratorKey,
      ValueType iteratorValue,
      BiConsumer<KeyType, ValueType> consumer) {
    foreach(
        columnFamilyHandle,
        context,
        (keyBuffer, valueBuffer) -> {
          iteratorKey.wrap(keyBuffer, 0, keyBuffer.capacity());
          iteratorValue.wrap(valueBuffer, 0, valueBuffer.capacity());
          consumer.accept(iteratorKey, iteratorValue);
        });
  }

  private void foreach(
      long columnFamilyHandle,
      DbContext context,
      BiConsumer<DirectBuffer, DirectBuffer> keyValuePairConsumer) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          try (ReadOptions readOptions = new ReadOptions();
              RocksIterator iterator = newIterator(columnFamilyHandle, context, readOptions)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
              context.wrapKeyView(iterator.key());
              context.wrapValueView(iterator.value());
              keyValuePairConsumer.accept(context.getKeyView(), context.getValueView());
            }
          }
        });
  }

  public <KeyType extends DbKey, ValueType extends DbValue> void whileTrue(
      long columnFamilyHandle,
      DbContext context,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(
        context,
        transaction -> {
          try (ReadOptions readOptions = new ReadOptions();
              RocksIterator iterator = newIterator(columnFamilyHandle, context, readOptions)) {
            boolean shouldVisitNext = true;
            for (iterator.seekToFirst(); iterator.isValid() && shouldVisitNext; iterator.next()) {
              shouldVisitNext = visit(context, keyInstance, valueInstance, visitor, iterator);
            }
          }
        });
  }

  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      long columnFamilyHandle,
      DbContext context,
      DbKey prefix,
      KeyType keyInstance,
      ValueType valueInstance,
      BiConsumer<KeyType, ValueType> visitor) {
    whileEqualPrefix(
        columnFamilyHandle,
        context,
        prefix,
        keyInstance,
        valueInstance,
        (k, v) -> {
          visitor.accept(k, v);
          return true;
        });
  }

  /**
   * NOTE: it doesn't seem possible in Java RocksDB to set a flexible prefix extractor on iterators
   * at the moment, so using prefixes seem to be mostly related to skipping files that do not
   * contain keys with the given prefix (which is useful anyway), but it will still iterate over all
   * keys contained in those files, so we still need to make sure the key actually matches the
   * prefix.
   *
   * <p>While iterating over subsequent keys we have to validate it.
   */
  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      long columnFamilyHandle,
      DbContext context,
      DbKey prefix,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> visitor) {
    context.ensurePrefixKeyBufferAvailable();

    ensureInOpenTransaction(
        context,
        transaction -> {
          final ExpandableArrayBuffer prefixKeyBuffer = context.getPrefixKeyBuffer();
          try (ReadOptions options =
                  new ReadOptions().setPrefixSameAsStart(true).setTotalOrderSeek(false);
              RocksIterator iterator = newIterator(columnFamilyHandle, context, options)) {
            prefix.write(prefixKeyBuffer, 0);
            final int prefixLength = prefix.getLength();

            boolean shouldVisitNext = true;

            for (RocksDbInternal.seek(
                    iterator, getNativeHandle(iterator), prefixKeyBuffer.byteArray(), prefixLength);
                iterator.isValid() && shouldVisitNext;
                iterator.next()) {
              final byte[] keyBytes = iterator.key();
              if (!startsWith(
                  prefixKeyBuffer.byteArray(),
                  0,
                  prefix.getLength(),
                  keyBytes,
                  0,
                  keyBytes.length)) {
                break;
              }

              shouldVisitNext = visit(context, keyInstance, valueInstance, visitor, iterator);
            }
          } finally {
            context.returnPrefixKeyBuffer(prefixKeyBuffer);
          }
        });
  }

  private <KeyType extends DbKey, ValueType extends DbValue> boolean visit(
      DbContext context,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> iteratorConsumer,
      RocksIterator iterator) {
    context.wrapKeyView(iterator.key());
    context.wrapValueView(iterator.value());

    final DirectBuffer keyViewBuffer = context.getKeyView();
    keyInstance.wrap(keyViewBuffer, 0, keyViewBuffer.capacity());
    final DirectBuffer valueViewBuffer = context.getValueView();
    valueInstance.wrap(valueViewBuffer, 0, valueViewBuffer.capacity());

    return iteratorConsumer.visit(keyInstance, valueInstance);
  }

  public boolean isEmpty(long columnFamilyHandle, DbContext context) {
    final AtomicBoolean isEmpty = new AtomicBoolean(false);
    ensureInOpenTransaction(
        context,
        transaction -> {
          try (ReadOptions options = new ReadOptions();
              RocksIterator iterator = newIterator(columnFamilyHandle, context, options)) {
            iterator.seekToFirst();
            final boolean hasEntry = iterator.isValid();
            isEmpty.set(!hasEntry);
            return;
          }
        });
    return isEmpty.get();
  }

  @Override
  public void createSnapshot(File snapshotDir) {
    try (Checkpoint checkpoint = Checkpoint.create(optimisticTransactionDB)) {
      try {
        checkpoint.createCheckpoint(snapshotDir.getAbsolutePath());
      } catch (RocksDBException rocksException) {
        throw new ZeebeDbException(rocksException);
      }
    }
  }

  @Override
  public DbContext createContext() {
    return new DefaultDbContext();
  }

  @Override
  public void close() {
    closables.forEach(
        closable -> {
          try {
            closable.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    optimisticTransactionDB.close();
  }
}
