package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * A non-empty subset of the nodes in a workflow.
 */
class WorkflowSubset<T extends Task> extends Target<T> implements Serializable
{
    private final Workflow<T> m_workflow;
    private final ImmutableMap<String, WorkflowNode<T>> m_nodes;

    private WorkflowSubset(Workflow<T> workflow, Set<WorkflowNode<T>> nodes)
    {
        m_workflow = Preconditions.checkNotNull(workflow);
        Optional<List<WorkflowNode<T>>> sortedNodes = TraversalUtils.topologicalSort(nodes);
        assert sortedNodes.isPresent() : "Unexpected graph cycle";
        m_nodes = Maps.uniqueIndex(sortedNodes.get(), WorkflowNode::getKey);
    }

    /**
     * Returns a target for the given nodes plus dependents. All of the given
     * nodes must be included in the given target. Dependents are defined over
     * the subgraph containing all the nodes in the given target and all edges
     * connecting those nodes.
     *
     * @param universe A target defining the subgraph over which dependents
     *                 will be calculated
     * @param nodes Nodes defining the boundary of the returned target
     * @return A target for the given nodes plus dependents in the given target
     */
    public static <U extends Task> Target<U> subsetBeginningAt(Target<U> universe, Collection<WorkflowNode<U>> nodes)
    {
        return subsetOfCollectedNodes(universe, nodes, WorkflowNode::getDependents);
    }

    /**
     * Returns a target for the given nodes plus dependencies. All of the given
     * nodes must be included in the given target. Dependencies are defined over
     * the subgraph containing all the nodes in the given target and all edges
     * connecting those nodes.
     *
     * @param universe A target defining the subgraph over which dependencies
     *                 will be calculated
     * @param nodes Nodes defining the boundary of the returned target
     * @return A target for the given nodes
     * plus dependencies in the given target
     */
    public static <U extends Task> Target<U> subsetEndingAt(Target<U> universe, Collection<WorkflowNode<U>> nodes)
    {
        return subsetOfCollectedNodes(universe, nodes, WorkflowNode::getDependencies);
    }

    private static <U extends Task> Target<U> subsetOfCollectedNodes(
            Target<U> universe, Collection<WorkflowNode<U>> startNodes,
            Function<WorkflowNode<U>, Set<WorkflowNode<U>>> neighborsFunc)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = validateSubset(universe, startNodes);
        return nodesCopy.size() == universe.getNodes().size() ?
                universe :
                new WorkflowSubset<>(
                        universe.getWorkflow(),
                        TraversalUtils.collectNodes(
                                nodesCopy.iterator(),
                                node -> neighborsFunc.apply(node).stream()
                                        .filter(universe::containsNode)
                                        .iterator()
                        )
                );
    }

    private static <U extends Task> WorkflowSubset<U> of(Workflow<U> workflow, ImmutableSet<WorkflowNode<U>> nodes)
    {
        return new WorkflowSubset<>(workflow, validateSubset(workflow, nodes));
    }

    private static <U extends Task> ImmutableSet<WorkflowNode<U>> validateSubset(Target<U> target,
                                                                                 Collection<WorkflowNode<U>> nodes)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = ImmutableSet.copyOf(nodes);

        Preconditions.checkArgument(!nodesCopy.isEmpty(), "Target must contain at least one node");
        Preconditions.checkArgument(nodesCopy.stream().allMatch(target::containsNode),
                                    "Target nodes must belong to the given workflow");

        return nodesCopy;
    }

    // TODO: for large subsets, we could store the absent nodes instead
    private static class SerializedForm<U extends Task> implements Serializable
    {
        private static final long serialVersionUID = 0L;

        private final Workflow<U> m_workflow;
        private final ImmutableSet<WorkflowNode<U>> m_nodes;

        public SerializedForm(Workflow<U> workflow, ImmutableSet<WorkflowNode<U>> nodes)
        {
            m_workflow = workflow;
            m_nodes = nodes;
        }

        private Object readResolve()
        {
            return WorkflowSubset.of(m_workflow, m_nodes);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new InvalidObjectException("Use SerializedForm");
    }

    private Object writeReplace()
    {
        return new SerializedForm<>(m_workflow, ImmutableSet.copyOf(m_nodes.values()));
    }

    @Override
    Workflow<T> getWorkflow()
    {
        return m_workflow;
    }

    @Override
    public Map<String, WorkflowNode<T>> getNodes()
    {
        return m_nodes;
    }

    @Override
    boolean containsNode(WorkflowNode<T> node)
    {
        return m_workflow.containsNode(node) && m_nodes.containsKey(node.getKey());
    }
}
