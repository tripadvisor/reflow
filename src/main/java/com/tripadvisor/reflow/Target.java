package com.tripadvisor.reflow;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

/**
 * A non-empty subset of the nodes in a workflow.
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

    /**
     * Returns (in constant time) whether this target contains a particular node.
     */
    abstract boolean containsNode(WorkflowNode<T> node);

    /**
     * Returns a target for the given nodes plus dependents. All of the given
     * nodes must be included in this target. Dependents are defined over the
     * subgraph containing all the nodes in this target and all edges
     * connecting those nodes.
     *
     * <p>For example, if {@code A} is a node passed to this method and
     * {@code B} is a dependent of {@code A}, {@code B} will be included in
     * the returned target if and only if it is included in this target.</p>
     *
     * @return A target for the given nodes plus dependents in this target
     */
    @SafeVarargs
    public final Target<T> startingFrom(WorkflowNode<T> node, WorkflowNode<T>... moreNodes)
    {
        return startingFrom(Lists.asList(node, moreNodes));
    }

    /**
     * Returns a target for the given nodes plus dependents. All of the given
     * nodes must be included in this target. Dependents are defined over the
     * subgraph containing all the nodes in this target and all edges
     * connecting those nodes.
     *
     * <p>For example, if {@code A} is a node passed to this method and
     * {@code B} is a dependent of {@code A}, {@code B} will be included in
     * the returned target if and only if it is included in this target.</p>
     *
     * @return A target for the given nodes plus dependents in this target
     */
    public Target<T> startingFrom(Collection<WorkflowNode<T>> nodes)
    {
        return WorkflowSubset.subsetBeginningAt(this, nodes);
    }


    /**
     * Returns a target for the nodes with the given keys plus dependent
     * nodes. All of the given keys must correspond to nodes in this target.
     * Dependents are defined over the subgraph containing all the nodes in
     * this target and all edges connecting those nodes.
     *
     * @return A target for the nodes with given keys
     * plus dependent nodes in this target
     * @see #startingFrom(WorkflowNode, WorkflowNode[])
     */
    public Target<T> startingFromKeys(String key, String... moreKeys)
    {
        return startingFromKeys(Lists.asList(key, moreKeys));
    }

    /**
     * Returns a target for the nodes with the given keys plus dependent
     * nodes. All of the given keys must correspond to nodes in this target.
     * Dependents are defined over the subgraph containing all the nodes in
     * this target and all edges connecting those nodes.
     *
     * @return A target for the nodes with given keys
     * plus dependent nodes in this target
     * @see #startingFrom(Collection)
     */
    public Target<T> startingFromKeys(Collection<String> keys)
    {
        return startingFrom(Collections2.transform(keys, getNodes()::get));
    }

    /**
     * Returns a target for the given nodes plus dependencies. All of the given
     * nodes must be included in this target. Dependencies are defined over the
     * subgraph containing all the nodes in this target and all edges
     * connecting those nodes.
     *
     * <p>For example, if {@code A} is a node passed to this method and
     * {@code B} is a dependency of {@code A}, {@code B} will be included in
     * the returned target if and only if it is included in this target.</p>
     *
     * @return A target for the given nodes plus dependencies in this target
     */
    @SafeVarargs
    public final Target<T> stoppingAfter(WorkflowNode<T> node, WorkflowNode<T>... moreNodes)
    {
        return stoppingAfter(Lists.asList(node, moreNodes));
    }

    /**
     * Returns a target for the given nodes plus dependencies. All of the given
     * nodes must be included in this target. Dependencies are defined over the
     * subgraph containing all the nodes in this target and all edges
     * connecting those nodes.
     *
     * <p>For example, if {@code A} is a node passed to this method and
     * {@code B} is a dependency of {@code A}, {@code B} will be included in
     * the returned target if and only if it is included in this target.</p>
     *
     * @return A target for the given nodes plus dependencies in this target
     */
    public Target<T> stoppingAfter(Collection<WorkflowNode<T>> nodes)
    {
        return WorkflowSubset.subsetEndingAt(this, nodes);
    }

    /**
     * Returns a target for the nodes with the given keys plus dependency
     * nodes. All of the given keys must correspond to nodes in this target.
     * Dependencies are defined over the subgraph containing all the nodes in
     * this target and all edges connecting those nodes.
     *
     * @return A target for the nodes with given keys
     * plus dependency nodes in this target
     * @see #stoppingAfter(WorkflowNode, WorkflowNode[])
     */
    public Target<T> stoppingAfterKeys(String key, String... moreKeys)
    {
        return stoppingAfterKeys(Lists.asList(key, moreKeys));
    }

    /**
     * Returns a target for the nodes with the given keys plus dependency
     * nodes. All of the given keys must correspond to nodes in this target.
     * Dependencies are defined over the subgraph containing all the nodes in
     * this target and all edges connecting those nodes.
     *
     * @return A target for the nodes with given keys
     * plus dependency nodes in this target
     * @see #stoppingAfter(Collection)
     */
    public Target<T> stoppingAfterKeys(Collection<String> keys)
    {
        return stoppingAfter(Collections2.transform(keys, getNodes()::get));
    }
}
