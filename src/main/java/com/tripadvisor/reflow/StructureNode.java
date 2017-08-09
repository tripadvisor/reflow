package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A node that has no associated task and serves only to link other nodes.
 */
public class StructureNode<T extends Task> extends WorkflowNode<T>
{
    private static final long serialVersionUID = 0L;

    private StructureNode(String key)
    {
        super(key);
    }

    /**
     * Returns {@code false}.
     */
    @Override
    public boolean hasTask()
    {
        return false;
    }

    /**
     * Immediately throws an exception.
     *
     * @deprecated
     * There's no task to get.
     *
     * @throws NoSuchElementException always
     */
    @Override
    @Deprecated
    public T getTask()
    {
        throw new NoSuchElementException();
    }

    public static final class Builder<U extends Task> extends WorkflowNode.Builder<U>
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Builder<U> setKey(@Nullable String key)
        {
            return (Builder<U>) super.setKey(key);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder<U> setDependencies(Set<WorkflowNode.Builder<U>> dependencies)
        {
            return (Builder<U>) super.setDependencies(dependencies);
        }

        @Override
        StructureNode<U> build(String key)
        {
            return new StructureNode<>(key);
        }
    }

    /**
     * Returns a new builder.
     */
    public static <U extends Task> StructureNode.Builder<U> builder()
    {
        return new StructureNode.Builder<>();
    }

    /**
     * Returns a new builder associated with the given key.
     */
    public static <U extends Task> StructureNode.Builder<U> builder(@Nullable String key)
    {
        return new StructureNode.Builder<U>().setKey(key);
    }
}
