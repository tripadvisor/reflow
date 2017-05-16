package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * A node with an associated task.
 */
public class TaskNode<T extends Task> extends WorkflowNode<T>
{
    private static final long serialVersionUID = 8887648155684502573L;

    private final T m_task;

    private TaskNode(T task)
    {
        m_task = Preconditions.checkNotNull(task);
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
        return String.format("%s(%s)", getClass().getSimpleName(), m_task);
    }

    public static final class Builder<K, U extends Task> extends WorkflowNode.Builder<K, U>
    {
        private U m_task;

        @Override
        public Builder<K, U> setKey(@Nullable K key)
        {
            return (Builder<K, U>) super.setKey(key);
        }

        @Override
        public Builder<K, U> setDependencies(Set<WorkflowNode.Builder<K, U>> dependencies)
        {
            return (Builder<K, U>) super.setDependencies(dependencies);
        }

        @Nullable
        public U getTask()
        {
            return m_task;
        }

        public Builder<K, U> setTask(U task)
        {
            m_task = task;
            return this;
        }

        @Override
        TaskNode<U> build()
        {
            return new TaskNode<>(m_task);
        }

        @Override
        public String toString()
        {
            return m_task == null ?
                    super.toString() :
                    String.format("%s(%s, %s)", getClass().getSimpleName(), getKey(), m_task);
        }
    }

    /**
     * Returns a new builder.
     */
    public static <K, U extends Task> TaskNode.Builder<K, U> builder()
    {
        return new TaskNode.Builder<>();
    }

    /**
     * Returns a new builder associated with the given task.
     */
    public static <K, U extends Task> TaskNode.Builder<K, U> builder(U userObject)
    {
        return new TaskNode.Builder<K, U>().setTask(userObject);
    }

    /**
     * Returns a new builder associated with the given key and task.
     */
    public static <K, U extends Task> TaskNode.Builder<K, U> builder(K key, U userObject)
    {
        return new TaskNode.Builder<K, U>().setKey(key).setTask(userObject);
    }
}
