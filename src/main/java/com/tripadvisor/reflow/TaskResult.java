package com.tripadvisor.reflow;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * The result of executing a {@link Task} (success or failure).
 * In the failure case, includes the exception thrown.
 */
public class TaskResult<T extends Task>
{
    private final TaskNode<T> m_node;
    private final Throwable m_failureCause;

    private TaskResult(TaskNode<T> node, Throwable failureCause)
    {
        m_node = node;
        m_failureCause = failureCause;
    }

    /**
     * Creates a successful task result.
     */
    public static <U extends Task> TaskResult<U> success(TaskNode<U> node)
    {
        return new TaskResult<>(node, null);
    }

    /**
     * Creates a failed task result.
     */
    public static <U extends Task> TaskResult<U> failure(TaskNode<U> node, Throwable cause)
    {
        return new TaskResult<>(node, Preconditions.checkNotNull(cause));
    }

    /**
     * Returns the node with which the task is associated.
     */
    public TaskNode<T> getNode()
    {
        return m_node;
    }

    /**
     * Returns the exception thrown by the task,
     * or null if the task completed successfully.
     */
    @Nullable
    public Throwable getFailureCause()
    {
        return m_failureCause;
    }

    /**
     * Indicates whether the task completed successfully
     * (without throwing an exception).
     */
    public boolean isSuccessful()
    {
        return m_failureCause == null;
    }
}
