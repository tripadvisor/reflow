package com.tripadvisor.reflow;

/**
 * The state of a node within a particular execution.
 *
 * <p>Possible transitions between states:
 *
 * <p>{@link #NOT_READY} -&gt; {@link #READY}
 * <br>{@link #READY} -&gt; {@link #SCHEDULED}
 * <br>{@link #READY} -&gt; {@link #SUCCEEDED}
 * <br>{@link #SCHEDULED} -&gt; {@link #SUCCEEDED}
 * <br>{@link #SCHEDULED} -&gt; {@link #FAILED}
 */
public enum NodeState
{
    /**
     * The node is not part of the execution plan.
     */
    IRRELEVANT(true),

    /**
     * The node is not ready for execution because one or more dependencies
     * have not finished executing.
     */
    NOT_READY(false),

    /**
     * The node is ready for execution.
     */
    READY(false),

    /**
     * The node's associated task has been scheduled for execution.
     */
    SCHEDULED(false),

    /**
     * If the node has an associated task, it executed successfully.
     * Execution has progressed past the node.
     */
    SUCCEEDED(true),

    /**
     * The node's associated task failed to execute.
     */
    FAILED(false),
    ;

    private final boolean m_satisfiesDependency;

    NodeState(boolean satisfiesDependency)
    {
        m_satisfiesDependency = satisfiesDependency;
    }

    /**
     * Indicates whether a node in this state represents a satisfied dependency
     * (that is, whether downstream nodes should be allowed to run).
     */
    boolean satisfiesDependency()
    {
        return m_satisfiesDependency;
    }
}
