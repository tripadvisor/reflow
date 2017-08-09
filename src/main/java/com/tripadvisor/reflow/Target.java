package com.tripadvisor.reflow;

import java.io.Serializable;
import java.util.Map;

/**
 * A non-empty subset of the nodes in a workflow, with the property
 * that every dependency of a node in the target is also in the target.
 * Designates a particular point to which the flow can be run.
 */
public abstract class Target<T extends Task> implements Serializable
{
    private static final long serialVersionUID = 0L;

    Target()
    {}

    /**
     * Returns the workflow associated with this target.
     */
    abstract Workflow<T> getWorkflow();

    /**
     * Returns a map of the nodes in this target by key.
     */
    public abstract Map<String, WorkflowNode<T>> getNodes();
}
