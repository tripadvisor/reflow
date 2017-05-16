package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Targets some, but not necessarily all, of a workflow.
 */
class WorkflowSubset<T extends Task> extends Target<T> implements Serializable
{
    private final Workflow<?, T> m_workflow;
    private final ImmutableSet<WorkflowNode<T>> m_nodes;

    private WorkflowSubset(Workflow<?, T> workflow, ImmutableSet<WorkflowNode<T>> tailNodes)
    {
        m_workflow = Preconditions.checkNotNull(workflow);

        Optional<List<WorkflowNode<T>>> sortedNodes = TraversalUtils.topologicalSort(
                TraversalUtils.collectNodes(tailNodes, WorkflowNode::getDependencies)
        );

        assert sortedNodes.isPresent() : "Unexpected graph cycle";
        m_nodes = ImmutableSet.copyOf(sortedNodes.get());
    }

    /**
     * Returns a subset of a workflow containing the given nodes
     * and their dependencies.
     */
    public static <U extends Task> Target<U> subsetEndingAt(Workflow<?, U> workflow, Collection<WorkflowNode<U>> nodes)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = validateSubset(workflow, nodes);
        return nodesCopy.size() == workflow.getNodes().size() ?
                workflow :
                new WorkflowSubset<>(workflow, nodesCopy);
    }

    private static <U extends Task> WorkflowSubset<U> of(Workflow<?, U> workflow, Collection<WorkflowNode<U>> nodes)
    {
        return new WorkflowSubset<>(workflow, validateSubset(workflow, nodes));
    }

    private static <U extends Task> ImmutableSet<WorkflowNode<U>> validateSubset(Workflow<?, U> workflow,
                                                                                 Collection<WorkflowNode<U>> nodes)
    {
        ImmutableSet<WorkflowNode<U>> nodesCopy = ImmutableSet.copyOf(nodes);

        Preconditions.checkArgument(!nodesCopy.isEmpty(), "Target must contain at least one node");
        Preconditions.checkArgument(workflow.getNodes().containsAll(nodesCopy),
                                    "Target nodes must belong to the given workflow");

        return nodesCopy;
    }

    private static class SerializedForm<U extends Task> implements Serializable
    {
        private static final long serialVersionUID = 6677016071263085269L;

        private Workflow<?, U> m_workflow;
        private ImmutableSet<WorkflowNode<U>> m_nodes;

        public SerializedForm(Workflow<?, U> workflow, ImmutableSet<WorkflowNode<U>> nodes)
        {
            m_workflow = workflow;
            m_nodes = nodes;
        }

        private Object readResolve()
        {
            return of(m_workflow, m_nodes);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new InvalidObjectException("Use SerializedForm");
    }

    private Object writeReplace()
    {
        return new SerializedForm<>(m_workflow, m_nodes);
    }

    @Override
    Workflow<?, T> getWorkflow()
    {
        return m_workflow;
    }

    @Override
    public Set<WorkflowNode<T>> getNodes()
    {
        return m_nodes;
    }

    @Override
    public String toString()
    {
        return String.format("Target(%s)", m_nodes.toString());
    }
}
