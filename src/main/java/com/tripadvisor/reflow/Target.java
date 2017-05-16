package com.tripadvisor.reflow;

import java.io.Serializable;
import java.util.Set;

/**
 * A non-empty subset of the nodes in a workflow, with the property
 * that every dependency of a node in the target is also in the target.
 * Designates a particular point to which the flow can be run.
 */
public abstract class Target<T extends Task> implements Serializable
{
    Target()
    {}

    /**
     * Returns the workflow associated with this target.
     */
    abstract Workflow<?, T> getWorkflow();

    /**
     * Returns the nodes in this target.
     */
    public abstract Set<WorkflowNode<T>> getNodes();
}
