package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * An immutable, serializable snapshot of an {@link Execution}.
 *
 * @see Execution#freeze()
 * @see WorkflowExecutor#thaw(FrozenExecution)
 */
public class FrozenExecution<T extends Task> implements Serializable
{
    private final Workflow<T> m_workflow;
    private final ImmutableMap<WorkflowNode<T>, NodeState> m_nodeStates;

    private FrozenExecution(Workflow<T> workflow, ImmutableMap<WorkflowNode<T>, NodeState> nodeStates)
    {
        m_workflow = workflow;
        m_nodeStates = nodeStates;
    }

    private static class SerializedForm<U extends Task> implements Serializable
    {
        private static final long serialVersionUID = 0L;

        private final Workflow<U> m_workflow;
        private final ImmutableMap<WorkflowNode<U>, NodeState> m_nodeStates;

        public SerializedForm(Workflow<U> workflow, ImmutableMap<WorkflowNode<U>, NodeState> nodeStates)
        {
            m_workflow = workflow;
            m_nodeStates = nodeStates;
        }

        private Object readResolve()
        {
            return FrozenExecution.of(m_workflow, m_nodeStates);
        }
    }

    static <U extends Task> FrozenExecution<U> of(Workflow<U> workflow, Map<WorkflowNode<U>, NodeState> nodeStates)
    {
        ImmutableMap<WorkflowNode<U>, NodeState> nodeStatesCopy = ImmutableMap.copyOf(nodeStates);
        Preconditions.checkArgument(workflow.getNodeSet().equals(nodeStatesCopy.keySet()));
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
     * Returns an immutable map of node states.
     */
    public Map<WorkflowNode<T>, NodeState> getNodeStates()
    {
        return m_nodeStates;
    }
}
