/*
 * Copyright (C) 2017 TripAdvisor LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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
        ImmutableMap<WorkflowNode<U>, NodeStatus> nodeStatesCopy = ImmutableMap.copyOf(Maps.transformEntries(
                nodeStates,
                (node, status) -> node.hasTask()
                        && status.getState().equals(NodeState.SCHEDULED)
                        && !status.getToken().isPresent() ? NodeStatus.withoutToken(NodeState.READY) : status
        ));
        
        Preconditions.checkArgument(workflow.getNodeSet().equals(nodeStatesCopy.keySet()),
                                    "Node set does not match workflow");

        Preconditions.checkArgument(nodeStatesCopy.entrySet().stream()
                                            .filter(e -> !e.getKey().hasTask())
                                            .noneMatch(e -> e.getValue().getState().equals(NodeState.SCHEDULED)),
                                    "Structure nodes cannot be scheduled");

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
