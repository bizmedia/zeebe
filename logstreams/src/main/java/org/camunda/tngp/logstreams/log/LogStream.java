package org.camunda.tngp.logstreams.log;

import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.logstreams.impl.LogController;
import org.camunda.tngp.logstreams.impl.log.index.LogBlockIndex;
import org.camunda.tngp.logstreams.spi.LogStorage;
import org.camunda.tngp.util.agent.AgentRunnerService;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a stream of events from a log storage.
 *
 * Opening the log stream will start the indexing of the current existing log storage.
 * The log storage will separated into blocks, these blocks will be indexed by an LogController.
 * The LogBlockIndex is available and can be accessed via {@link LogStream#getLogBlockIndex()}.
 *
 * The LogStream will append available events to the log storage with the help of an LogController.
 * The events are read from a given Dispatcher, if available. This can be stopped with the
 * {@link LogStream#stopLogStreaming()}  method and re-/started with {@link LogStream#startLogStreaming(AgentRunnerService)}
 * or {@link LogStream#startLogStreaming(AgentRunnerService, int)}.
 *
 * To access the current LogStorage the {@link LogStream#getLogStorage()} can be used. The {@link #close()}
 * method will close all LogController and the log storage.
 */
public interface LogStream extends AutoCloseable
{
    int DEFAULT_WRITE_BUFFER_SIZE = 1024 * 1024 * 16;
    int DEFAULT_MAX_APPEND_BLOCK_SIZE = 1024 * 1024 * 4;

    /**
     * @return the log stream's logId
     */
    int getId();

    /**
     * Returns the name of the log stream.
     *
     * @return the log stream name
     */
    String getLogName();

    /**
     * Opens the log stream synchronously. This blocks until the log stream is
     * opened.
     */
    void open();

    /**
     * Opens the log stream asynchronously.
     */
    CompletableFuture<Void> openAsync();

    /**
     * Closes the log stream synchronously. This blocks until the log stream is
     * closed.
     */
    void close();

    /**
     * Closes the log stream asynchronous.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * @return the current position of the log appender, or a negative value if
     *         the log stream is not open
     */
    long getCurrentAppenderPosition();

    /**
     * Register a failure listener.
     */
    void registerFailureListener(LogStreamFailureListener listener);

    /**
     * Remove a registered failure listener.
     */
    void removeFailureListener(LogStreamFailureListener listener);

    /**
     * Returns the log storage, which is accessed by the LogStream.
     *
     * @return the log storage
     */
    LogStorage getLogStorage();

    /**
     * Returns the LogBlockIndex object, which is used for indexing the LogStorage.
     *
     * @return the log block index
     */
    LogBlockIndex getLogBlockIndex();

    /**
     * Returns the maximum size of a block for which an index is created.
     *
     * @return  the index block size
     */
    int getIndexBlockSize();

    /**
     * Returns the writeBuffer, which is used by the LogStreamController to stream the content into the log storage.
     *
     * @return the writebuffer, which is used by the LogStreamController
     */
    Dispatcher getWriteBuffer();

    /**
     * Returns the log stream controller, which streams the logged events from the write buffer into the log storage.
     *
     * @return the log stream controller
     */
    LogController getLogStreamController();

    /**
     * Returns the log block index controller, which creates periodically the block index for the log storage.
     * @return the log block index controller
     */
    LogController getLogBlockIndexController();

    /**
     * Stops the streaming to the log storage. New events are no longer append to the log storage.
     */
    void stopLogStreaming();

    /**
     * This method delegates to {@link #startLogStreaming(AgentRunnerService, int)}.
     *
     * The {@link #DEFAULT_MAX_APPEND_BLOCK_SIZE} is used as default max append block size.
     *
     * @see {@link #startLogStreaming(AgentRunnerService, int)}
     * @param writeBufferAgentRunnerService the agent runner service which is used for the scheduling
     * @return returns the future for the log stream controller opening
     */
    CompletableFuture<Void> startLogStreaming(AgentRunnerService writeBufferAgentRunnerService);

    /**
     * Starts the log streaming from the write buffer into log storage. The write buffer
     * is internally created .The given agent runner service is used to schedule the writing.
     *
     * The {@link #DEFAULT_MAX_APPEND_BLOCK_SIZE} is used as default max append block size.
     * This method delegates to {@link #startLogStreaming(AgentRunnerService, int)}.
     *
     * @param writeBufferAgentRunnerService the agent runner service which is used for the scheduling
     * @param maxAppendBlockSize the maximum block size which should been appended
     * @return returns the future for the log stream controller opening
     */
    CompletableFuture<Void> startLogStreaming(AgentRunnerService writeBufferAgentRunnerService, int maxAppendBlockSize);

    /**
     * Truncates the log stream from the given position to the end of the stream.
     * This method will truncated the log storage and block index.
     *
     *
     * @param position the position to start the truncation
     * @return the future which is completed if the truncation was successful
     */
    CompletableFuture<Void> truncate(long position);
}
