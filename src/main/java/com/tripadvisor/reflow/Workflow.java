package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.tripadvisor.reflow.WorkflowNode.Builder;

import static java.util.stream.Collectors.toSet;

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
public class Workflow<K, T extends Task> extends Target<T> implements Serializable
{
    private final ImmutableBiMap<K, WorkflowNode<T>> m_keyedNodes;
    private final ImmutableSet<WorkflowNode<T>> m_allNodes;

    private Workflow(ImmutableBiMap<K, WorkflowNode<T>> keyedNodes, ImmutableSet<WorkflowNode<T>> allNodes)
    {
        Optional<List<WorkflowNode<T>>> sortedNodes = TraversalUtils.topologicalSort(allNodes);
        Preconditions.checkState(sortedNodes.isPresent(), "Input graph contains a cycle");
        Preconditions.checkState(allNodes.containsAll(keyedNodes.values()), "Keyed nodes not a subset of all nodes");

        m_keyedNodes = keyedNodes;
        m_allNodes = ImmutableSet.copyOf(sortedNodes.get());
    }

    private static class SerializedForm<KK, TT extends Task> implements Serializable
    {
        private static final long serialVersionUID = -3862828678509294316L;

        private ImmutableBiMap<KK, WorkflowNode<TT>> m_keyedNodes;
        private ImmutableSet<WorkflowNode<TT>> m_allNodes;

        public SerializedForm(ImmutableBiMap<KK, WorkflowNode<TT>> keyedNodes, ImmutableSet<WorkflowNode<TT>> allNodes)
        {
            m_keyedNodes = keyedNodes;
            m_allNodes = allNodes;
        }

        private Object readResolve()
        {
            return Workflow.of(m_keyedNodes, m_allNodes);
        }
    }

    /**
     * Constructs a graph from a collection of node builder objects.
     *
     * The graph represented by the builders must be acyclic. The
     * dependencies of every builder in the given collection must also be in
     * the collection, and the collection must not contain any repeated
     * elements.
     *
     * Builders with null dependency sets are translated to nodes with no
     * dependencies.
     *
     * @param builders a collection of builder objects representing a graph
     * @return a graph corresponding to the input collection
     */
    public static <KK, TT extends Task> Workflow<KK, TT> create(Collection<? extends WorkflowNode.Builder<KK, TT>> builders)
    {
        ImmutableBiMap<Builder<KK, TT>, WorkflowNode<TT>> nodesByBuilder;
        ImmutableBiMap<KK, WorkflowNode<TT>> nodesByKey;

        // Populate maps, creating a new node for each builder in the template
        ImmutableBiMap.Builder<WorkflowNode.Builder<KK, TT>, WorkflowNode<TT>> nodesByBuilderMutable = ImmutableBiMap.builder();
        ImmutableBiMap.Builder<KK, WorkflowNode<TT>> nodesByKeyMutable = ImmutableBiMap.builder();
        for (WorkflowNode.Builder<KK, TT> builder : builders)
        {
            WorkflowNode<TT> node = builder.build();
            KK key = builder.getKey();

            nodesByBuilderMutable.put(builder, node);
            if (key != null)
            {
                nodesByKeyMutable.put(key, node);
            }
        }

        try
        {
            nodesByBuilder = nodesByBuilderMutable.build();
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException("Input collection contains repeated elements", e);
        }

        try
        {
            nodesByKey = nodesByKeyMutable.build();
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException("Input collection contains repeated keys", e);
        }

        if (nodesByBuilder.isEmpty())
        {
            throw new IllegalStateException("Input collection is empty");
        }

        // Copy over dependencies from the template
        Function<WorkflowNode.Builder<KK, TT>, WorkflowNode<TT>> getNode = (builder) ->
        {
            WorkflowNode<TT> node = nodesByBuilder.get(builder);
            if (node == null)
            {
                throw new IllegalStateException("Input collection is incomplete: missing builder " + builder);
            }
            return node;
        };

        nodesByBuilder.forEach((builder, node) -> node.setDependencies(
                Optional.ofNullable(builder.getDependenciesNullable())
                        .orElse(Collections.emptySet())
                        .stream()
                        .map(getNode)
                        .collect(toSet())));

        // Calculate dependents
        Map<WorkflowNode, Set<WorkflowNode<TT>>> dependentsMap = new HashMap<>(nodesByBuilder.size());
        for (WorkflowNode<TT> node : nodesByBuilder.values())
        {
            for (WorkflowNode<TT> dependency : node.getDependencies())
            {
                dependentsMap.computeIfAbsent(dependency, key -> new HashSet<>()).add(node);
            }
        }

        for (WorkflowNode<TT> node : nodesByBuilder.values())
        {
            node.setDependents(dependentsMap.getOrDefault(node, Collections.emptySet()));
        }

        // Finally, construct a new graph, copying the values out of
        // nodesByBuilder to avoid holding a reference to the builders
        return new Workflow<>(nodesByKey, ImmutableSet.copyOf(nodesByBuilder.values()));
    }

    private static <KK, TT extends Task> Workflow<KK, TT> of(ImmutableBiMap<KK, WorkflowNode<TT>> keyedNodes,
                                                             ImmutableSet<WorkflowNode<TT>> allNodes)
    {
        Preconditions.checkState(
                TraversalUtils.collectNodes(
                        allNodes.iterator(),
                        node -> Stream.concat(node.getDependencies().stream(), node.getDependents().stream()).iterator()
                ).equals(allNodes),
                "Input collection is incomplete"
        );
        return new Workflow<>(keyedNodes, allNodes);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new InvalidObjectException("Use SerializedForm");
    }

    private Object writeReplace()
    {
        return new SerializedForm<>(m_keyedNodes, m_allNodes);
    }

    /**
     * Returns a target containing only the given nodes
     * and their dependencies.
     */
    @SafeVarargs
    public final Target<T> stoppingAfter(WorkflowNode<T> node, WorkflowNode<T>... moreNodes)
    {
        return stoppingAfter(Lists.asList(node, moreNodes));
    }

    /**
     * Returns a target containing only the given nodes
     * and their dependencies.
     */
    public Target<T> stoppingAfter(Collection<WorkflowNode<T>> nodes)
    {
        return WorkflowSubset.subsetEndingAt(this, nodes);
    }

    /**
     * Returns a target containing only the nodes corresponding
     * to the given keys and their dependencies.
     */
    @SafeVarargs
    public final Target<T> stoppingAfterKeys(K key, K... moreKeys)
    {
        return stoppingAfterKeys(Lists.asList(key, moreKeys));
    }

    /**
     * Returns a target containing only the nodes corresponding
     * to the given keys and their dependencies.
     */
    public Target<T> stoppingAfterKeys(Collection<? extends K> keys)
    {
        return stoppingAfter(Collections2.transform(keys, m_keyedNodes::get));
    }

    /**
     * Returns this workflow.
     *
     * @deprecated
     * No need to call this.
     */
    @Override
    @Deprecated
    Workflow<?, T> getWorkflow()
    {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<WorkflowNode<T>> getNodes()
    {
        return m_allNodes;
    }

    /**
     * Returns a map of the nodes with an associated key in this workflow.
     */
    public Map<K, WorkflowNode<T>> keyedNodes()
    {
        return m_keyedNodes;
    }
}
