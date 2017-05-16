package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A node that has no associated task and serves only to link other nodes.
 */
public class StructureNode<T extends Task> extends WorkflowNode<T>
{
    private static final long serialVersionUID = -4588265518951283418L;

    private StructureNode()
    {}

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

    public static final class Builder<K, U extends Task> extends WorkflowNode.Builder<K, U>
    {
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

        @Override
        StructureNode<U> build()
        {
            return new StructureNode<>();
        }
    }

    /**
     * Returns a new builder.
     */
    public static <K, U extends Task> StructureNode.Builder<K, U> builder()
    {
        return new StructureNode.Builder<>();
    }

    /**
     * Returns a new builder associated with the given key.
     */
    public static <K, U extends Task> StructureNode.Builder<K, U> builder(K key)
    {
        return new StructureNode.Builder<K, U>().setKey(key);
    }
}
