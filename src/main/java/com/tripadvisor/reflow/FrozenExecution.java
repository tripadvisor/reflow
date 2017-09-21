package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * An immutable, serializable snapshot of an {@link Execution}.
 */
public class FrozenExecution<T extends Task> implements Serializable
{
    private final Workflow<T> m_workflow;
    private final ImmutableMap<WorkflowNode<T>, NodeStatus> m_nodeStates;

    private FrozenExecution(Workflow<T> workflow, ImmutableMap<WorkflowNode<T>, NodeStatus> nodeStates)
    {
        m_workflow = workflow;
        m_nodeStates = nodeStates;
    }

    private static class SerializedForm<U extends Task> implements Serializable
    {
        private static final long serialVersionUID = 0L;

        private final Workflow<U> m_workflow;
        private final ImmutableMap<WorkflowNode<U>, NodeStatus> m_nodeStates;

        public SerializedForm(Workflow<U> workflow, ImmutableMap<WorkflowNode<U>, NodeStatus> nodeStates)
        {
            m_workflow = workflow;
            m_nodeStates = nodeStates;
        }

        private Object readResolve()
        {
            return FrozenExecution.of(m_workflow, m_nodeStates);
        }
    }

    static <U extends Task> FrozenExecution<U> of(Workflow<U> workflow, Map<WorkflowNode<U>, NodeStatus> nodeStates)
    {
        ImmutableMap<WorkflowNode<U>, NodeStatus> nodeStatesCopy = ImmutableMap.copyOf(nodeStates);
        
        Preconditions.checkArgument(workflow.getNodeSet().equals(nodeStatesCopy.keySet()));
        for (Entry<WorkflowNode<U>, NodeStatus> e : nodeStatesCopy.entrySet())
        {
            if (e.getKey().hasTask() && e.getValue().getState().equals(NodeState.SCHEDULED))
            {
                Preconditions.checkArgument(e.getValue().getToken().isPresent(), "Missing token");
            }
        }

        return new FrozenExecution<>(workflow, nodeStatesCopy);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        throw new InvalidObjectException("Use SerializedForm");
    }

    private Object writeReplace()
    {
        return new SerializedForm<>(m_workflow, m_nodeStates);
    }

    Workflow<T> getWorkflow()
    {
        return m_workflow;
    }

    /**
     * Returns an immutable map of node statuses.
     */
    public Map<WorkflowNode<T>, NodeStatus> getNodeStatuses()
    {
        return m_nodeStates;
    }
}
