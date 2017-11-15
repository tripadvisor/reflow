package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * A set of tasks arranged in an immutable directed acyclic graph.
 *
 * <p>Workflows are composed of node objects, each holding references to a set of
 * dependency nodes and a set of dependent nodes. Nodes may optionally reference
 * a task object.</p>
 *
 * <p>Tasks, in turn, define some output (for example, in terms of file paths).
 * This enables the intelligent execution of workflows, skipping tasks that
 * have already created their output.</p>
 */
public class Workflow<T extends Task> extends Target<T> implements Serializable
{
    private final ImmutableBiMap<String, WorkflowNode<T>> m_nodes;

    private Workflow(Collection<WorkflowNode<T>> nodes)
    {
        Optional<List<WorkflowNode<T>>> sortedNodes = TraversalUtils.topologicalSort(nodes);
        Preconditions.checkArgument(sortedNodes.isPresent(), "Input graph contains a cycle");
        m_nodes = sortedNodes.get().stream().collect(toImmutableBiMap(WorkflowNode::getKey, Function.identity()));
    }

    private static class SerializedForm<U extends Task> implements Serializable
    {
        private static final long serialVersionUID = 0L;

        private final ImmutableSet<WorkflowNode<U>> m_nodes;

        public SerializedForm(ImmutableSet<WorkflowNode<U>> nodes)
        {
            m_nodes = nodes;
        }

        private Object readResolve()
        {
            return Workflow.of(m_nodes);
        }
    }

    /**
     * Constructs a graph from a non-empty collection of node builder objects.
     *
     * <p>The graph represented by the builders must be acyclic. The
     * dependencies of every builder in the given collection must also be in
     * the collection, and the collection must not contain any repeated
     * elements.</p>
     *
     * <p>Builders with null keys will yield nodes with generated keys.
     * Builders with null dependency sets will yield nodes with no
     * dependencies.</p>
     *
     * @param builders a collection of builder objects representing a graph
     * @return a graph corresponding to the input collection
     * @throws IllegalArgumentException if {@code builders} is empty, contains
     * repeated elements, contains builders with repeated keys, contains
     * builders that reference builders outside the collection, or contains
     * builders arranged in a cyclical graph
     */
    public static <U extends Task> Workflow<U> create(Collection<? extends WorkflowNode.Builder<U>> builders)
    {
        // Take a snapshot of the provided keys, then instantiate a node for each builder
        ImmutableBiMap<WorkflowNode.Builder<U>, String> keysByBuilder = builders.stream()
                .filter(b -> b.getKey() != null)
                .collect(toImmutableBiMap(Function.identity(), WorkflowNode.Builder::getKey));
        Map<WorkflowNode.Builder<U>, WorkflowNode<U>> nodesByBuilder = Maps.newHashMapWithExpectedSize(builders.size());
        int nextKey = 0;

        for (WorkflowNode.Builder<U> builder : builders)
        {
            String key = keysByBuilder.get(builder);
            if (key == null)
            {
                do
                {
                    key = Strings.padStart(Integer.toHexString(nextKey++), Integer.BYTES * 2, '0');
                }
                while (keysByBuilder.containsValue(key));
            }

            WorkflowNode<U> node = builder.build(key);

            if (nodesByBuilder.put(builder, node) != null)
            {
                throw new IllegalArgumentException("Input collection contains repeated elements");
            }
        }

        if (nodesByBuilder.isEmpty())
        {
            throw new IllegalArgumentException("Input collection is empty");
        }

        // Copy over dependencies from the template
        Function<WorkflowNode.Builder<U>, WorkflowNode<U>> getNode = (builder) ->
        {
            WorkflowNode<U> node = nodesByBuilder.get(builder);
            if (node == null)
            {
                throw new IllegalArgumentException("Input collection is incomplete: missing builder " + builder);
            }
            return node;
        };

        nodesByBuilder.forEach((builder, node) -> node.setDependencies(
                Optional.ofNullable(builder.getDependenciesNullable())
                        .orElse(ImmutableSet.of())
                        .stream()
                        .map(getNode)
                        .collect(toImmutableSet())));

        return calculateDependents(nodesByBuilder.values());
    }

    private static <U extends Task> Workflow<U> of(ImmutableSet<WorkflowNode<U>> nodes)
    {
        Preconditions.checkArgument(
                nodes.stream().map(WorkflowNode::getKey).distinct().count() == nodes.size(),
                "Input collection contains repeated keys"
        );

        Preconditions.checkArgument(!nodes.isEmpty(), "Input collection is empty");

        Preconditions.checkArgument(
                nodes.stream().flatMap(node -> node.getDependencies().stream()).allMatch(nodes::contains),
                "Input collection is incomplete"
        );

        return calculateDependents(nodes);
    }

    private static <U extends Task> Workflow<U> calculateDependents(Collection<WorkflowNode<U>> nodes)
    {
        Map<WorkflowNode, Set<WorkflowNode<U>>> dependentsMap = Maps.newHashMapWithExpectedSize(nodes.size());

        for (WorkflowNode<U> node : nodes)
        {
            for (WorkflowNode<U> dependency : node.getDependencies())
            {
                dependentsMap.computeIfAbsent(dependency, key -> new HashSet<>()).add(node);
            }
        }

        for (WorkflowNode<U> node : nodes)
        {
            node.setDependents(dependentsMap.getOrDefault(node, ImmutableSet.of()));
        }

        return new Workflow<>(nodes);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new InvalidObjectException("Use SerializedForm");
    }

    private Object writeReplace()
    {
        return new SerializedForm<>(ImmutableSet.copyOf(m_nodes.values()));
    }

    /**
     * Returns this workflow.
     *
     * @deprecated
     * No need to call this.
     */
    @Override
    @Deprecated
    Workflow<T> getWorkflow()
    {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, WorkflowNode<T>> getNodes()
    {
        return m_nodes;
    }

    Set<WorkflowNode<T>> getNodeSet()
    {
        return m_nodes.values();
    }

    @Override
    boolean containsNode(WorkflowNode<T> node)
    {
        return m_nodes.containsValue(node);
    }
}
