package com.tripadvisor.reflow;

/**
 * A completion service for workflow nodes, allowing nodes to be submitted
 * for execution and execution results to be retrieved. Similar to
 * {@link java.util.concurrent.CompletionService} but makes it easy to
 * retrieve a node reference on failure.
 */
public interface WorkflowCompletionService<T extends Task>
{
    /**
     * Removes and returns the execution results of a node previously
     * submitted via {@link #submit(TaskNode)}. If there are no
     * results ready, waits for a node to finish executing.
     *
     * This method may wait indefinitely if called when no
     * nodes are executing.
     *
     * @return the execution results of a completed node
     * @throws InterruptedException if interrupted while waiting
     */
    TaskResult<T> take() throws InterruptedException;

    /**
     * Submits a node for execution.
     *
     * @param node the node to submit
     */
    void submit(TaskNode<T> node);

}
