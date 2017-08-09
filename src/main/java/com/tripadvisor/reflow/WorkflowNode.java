package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
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
    private static final long serialVersionUID = 0L;

    private static final Pattern KEY_REGEX = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9_-]{0,254}[a-zA-Z0-9])?");

    private final String m_key;

    private ImmutableSet<WorkflowNode<T>> m_dependencies;
    private transient ImmutableSet<WorkflowNode<T>> m_dependents;

    WorkflowNode(String key)
    {
        m_key = key;
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
        Preconditions.checkNotNull(m_key, "Null key");
        Preconditions.checkArgument(KEY_REGEX.matcher(m_key).matches(), "Key must match pattern %s", KEY_REGEX.pattern());
    }

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
     * Returns the key with which this node is associated.
     */
    public String getKey()
    {
        return m_key;
    }

    /**
     * Returns a string representation of this node.
     */
    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), m_key);
    }

    /**
     * A builder class for {@link WorkflowNode}s.
     */
    public static abstract class Builder<U extends Task>
    {
        @Nullable
        private String m_key;

        private Set<Builder<U>> m_dependencies;

        Builder()
        {}

        /**
         * Returns the key that will be used when building nodes, or
         * {@code null} if no key has been set.
         */
        @Nullable
        public String getKey()
        {
            return m_key;
        }

        /**
         * Sets the key that will be used when building nodes. The value
         * {@code null} will result in the generation of a key during workflow
         * construction.
         *
         * <p>Keys must be between 1 and 256 characters in length and may
         * include the characters {@code 'a'} through {@code 'z'}, {@code 'A'}
         * through {@code 'Z'}, {@code '0'} through {@code '9'}, and the hyphen
         * {@code '-'} and underscore {@code '_'}. Keys may not start or end
         * with a hyphen or underscore.</p>
         */
        public Builder<U> setKey(@Nullable String key)
        {
            m_key = key;
            return this;
        }

        /**
         * Returns the dependencies that will be used when building nodes.
         */
        public Set<Builder<U>> getDependencies()
        {
            if (m_dependencies == null)
            {
                m_dependencies = new HashSet<>();
            }
            return m_dependencies;
        }

        /**
         * Returns the dependencies that will be used when building nodes.
         * If the dependency set is not initialized, returns {@code null}.
         */
        @Nullable
        Set<Builder<U>> getDependenciesNullable()
        {
            return m_dependencies;
        }

        /**
         * Sets the dependencies that will be used when building nodes.
         * The value {@code null} will be translated to an empty set.
         */
        public Builder<U> setDependencies(Set<Builder<U>> dependencies)
        {
            m_dependencies = dependencies;
            return this;
        }

        /**
         * Adds dependencies that will be used when building nodes.
         */
        @SafeVarargs
        public final Builder<U> addDependencies(Builder<U> dependency, Builder<U>... moreDependencies)
        {
            Set<Builder<U>> dependencies = getDependencies();
            dependencies.add(dependency);
            Collections.addAll(dependencies, moreDependencies);
            return this;
        }

        abstract WorkflowNode<U> build(String key);

        /**
         * Returns a string representation of this builder.
         */
        @Override
        public String toString()
        {
            return String.format("%s(%s)", getClass().getSimpleName(), m_key);
        }
    }
}
