package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * A node in a workflow, possibly referencing a task.
 *
 * <p>Note that the object graph in which a particular node resides
 * is not guaranteed to be acyclic unless the node is referenced by a
 * {@link Workflow} object.</p>
 */
public abstract class WorkflowNode<T extends Task> implements Serializable
{
    private static final long serialVersionUID = 6044975822389478028L;

    private ImmutableSet<WorkflowNode<T>> m_dependencies;
    private ImmutableSet<WorkflowNode<T>> m_dependents;

    WorkflowNode()
    {}

    /**
     * Returns the set of nodes this node depends on.
     */
    public Set<WorkflowNode<T>> getDependencies()
    {
        return m_dependencies;
    }

    /**
     * Returns the set of nodes that depend on this node.
     */
    public Set<WorkflowNode<T>> getDependents()
    {
        return m_dependents;
    }

    void setDependencies(Set<WorkflowNode<T>> dependencies)
    {
        m_dependencies = ImmutableSet.copyOf(dependencies);
    }

    void setDependents(Set<WorkflowNode<T>> dependents)
    {
        m_dependents = ImmutableSet.copyOf(dependents);
    }

    /**
     * Indicates whether this node has an associated task.
     */
    public abstract boolean hasTask();

    /**
     * Returns the task associated with this node.
     *
     * @throws NoSuchElementException if this node has no associated task
     */
    public abstract T getTask();

    /**
     * Returns a string representation of this node.
     */
    @Override
    public String toString()
    {
        return String.format("%s@%s", getClass().getSimpleName(), Integer.toHexString(hashCode()));
    }

    /**
     * A node builder for use in creating immutable execution graphs.
     */
    public static abstract class Builder<K, U extends Task>
    {
        private K m_key;
        private Set<Builder<K, U>> m_dependencies;

        Builder()
        {}

        @Nullable
        public K getKey()
        {
            return m_key;
        }

        public Builder<K, U> setKey(@Nullable K key)
        {
            m_key = key;
            return this;
        }

        public Set<Builder<K, U>> getDependencies()
        {
            if (m_dependencies == null)
            {
                m_dependencies = new HashSet<>();
            }
            return m_dependencies;
        }

        @Nullable
        Set<Builder<K, U>> getDependenciesNullable()
        {
            return m_dependencies;
        }

        public Builder<K, U> setDependencies(Set<Builder<K, U>> dependencies)
        {
            m_dependencies = dependencies;
            return this;
        }

        @SafeVarargs
        public final Builder<K, U> addDependencies(Builder<K, U> dependency, Builder<K, U>... moreDependencies)
        {
            Set<Builder<K, U>> dependencies = getDependencies();
            dependencies.add(dependency);
            Collections.addAll(dependencies, moreDependencies);
            return this;
        }

        abstract WorkflowNode<U> build();

        @Override
        public String toString()
        {
            String name = getClass().getSimpleName();
            return m_key == null ?
                    String.format("%s@%s", name, Integer.toHexString(hashCode())) :
                    String.format("%s(%s)", name, m_key);
        }
    }
}
