package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Targets some, but not necessarily all, of a workflow.
 */
class WorkflowSubset<T extends Task> extends Target<T> implements Serializable
{
    private final Workflow<T> m_workflow;
    private final ImmutableMap<String, WorkflowNode<T>> m_nodes;

    private WorkflowSubset(Workflow<T> workflow, ImmutableSet<WorkflowNode<T>> tailNodes)
    {
        m_workflow = Preconditions.checkNotNull(workflow);

        Optional<List<WorkflowNode<T>>> sortedNodes = TraversalUtils.topologicalSort(
                TraversalUtils.collectNodes(tailNodes, WorkflowNode::getDependencies)
        );

        assert sortedNodes.isPresent() : "Unexpected graph cycle";
        m_nodes = Maps.uniqueIndex(sortedNodes.get(), WorkflowNode::getKey);
    }

    /**
     * Returns a subset of a workflow containing the given nodes
     * and their dependencies.
     */
    public static <U extends Task> Target<U> subsetEndingAt(Workflow<U> workflow, Collection<WorkflowNode<U>> nodes)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = validateSubset(workflow, nodes);
        return nodesCopy.size() == workflow.getNodes().size() ?
                workflow :
                new WorkflowSubset<>(workflow, nodesCopy);
    }

    private static <U extends Task> WorkflowSubset<U> of(Workflow<U> workflow, Collection<WorkflowNode<U>> nodes)
    {
        return new WorkflowSubset<>(workflow, validateSubset(workflow, nodes));
    }

    private static <U extends Task> ImmutableSet<WorkflowNode<U>> validateSubset(Workflow<U> workflow,
                                                                                 Collection<WorkflowNode<U>> nodes)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = ImmutableSet.copyOf(nodes);

        Preconditions.checkArgument(!nodesCopy.isEmpty(), "Target must contain at least one node");
        Preconditions.checkArgument(workflow.getNodeSet().containsAll(nodesCopy),
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
    public String toString()
    {
        return String.format("Target(%s)", m_nodes.toString());
    }
}
