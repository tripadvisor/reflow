package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * A node with an associated task.
 */
public class TaskNode<T extends Task> extends WorkflowNode<T>
{
    private static final long serialVersionUID = 0L;

    private final T m_task;

    private TaskNode(String key, T task)
    {
        super(key);
        m_task = task;
        validateState();
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        validateState();
    }

    private void readObjectNoData() throws ObjectStreamException
    {
        throw new InvalidObjectException("No object data");
    }

    private void validateState()
    {
        Preconditions.checkNotNull(m_task, "Null task");
    }

    /**
     * Returns {@code true}.
     */
    @Override
    public boolean hasTask()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getTask()
    {
        return m_task;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", getClass().getSimpleName(), getKey(), m_task);
    }

    public static final class Builder<U extends Task> extends WorkflowNode.Builder<U>
    {
        private U m_task;

        /**
         * {@inheritDoc}
         */
        @CanIgnoreReturnValue
        @Override
        public Builder<U> setKey(@Nullable String key)
        {
            return (Builder<U>) super.setKey(key);
        }

        /**
         * {@inheritDoc}
         */
        @CanIgnoreReturnValue
        @Override
        public Builder<U> setDependencies(Set<WorkflowNode.Builder<U>> dependencies)
        {
            return (Builder<U>) super.setDependencies(dependencies);
        }

        /**
         * Gets the task that will be used when building nodes, or
         * {@code null} if no task has been set.
         */
        @Nullable
        public U getTask()
        {
            return m_task;
        }

        /**
         * Sets the task that will be used when building nodes.
         * The task must be set before constructing a workflow.
         */
        @CanIgnoreReturnValue
        public Builder<U> setTask(U task)
        {
            m_task = task;
            return this;
        }

        @Override
        TaskNode<U> build(String key)
        {
            return new TaskNode<>(key, m_task);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return String.format("%s(%s, %s)", getClass().getSimpleName(), getKey(), m_task);
        }
    }

    /**
     * Returns a new builder.
     */
    public static <U extends Task> TaskNode.Builder<U> builder()
    {
        return new TaskNode.Builder<>();
    }

    /**
     * Returns a new builder associated with the given task.
     */
    public static <U extends Task> TaskNode.Builder<U> builder(U task)
    {
        return new TaskNode.Builder<U>().setTask(task);
    }

    /**
     * Returns a new builder associated with the given key and task.
     */
    public static <U extends Task> TaskNode.Builder<U> builder(@Nullable String key, U task)
    {
        return new TaskNode.Builder<U>().setKey(key).setTask(task);
    }
}
