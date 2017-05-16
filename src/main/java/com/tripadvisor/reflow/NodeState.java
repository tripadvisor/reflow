package com.tripadvisor.reflow;

/**
 * The state of a node with regards to execution. Even if a node has no
 * associated task, it will transition from {@link #NOT_READY} to
 * {@link #READY} to {@link #SUBMITTED} to {@link #SUCCEEDED} for
 * bookkeeping purposes.
 *
 * <p>Possible transitions between states:
 *
 * <p>{@link #NOT_READY} -&gt; {@link #READY}
 * <br>{@link #READY} -&gt; {@link #SUBMITTED}
 * <br>{@link #SUBMITTED} -&gt; {@link #SUCCEEDED}
 * <br>{@link #SUBMITTED} -&gt; {@link #FAILED}
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
     * The node has been submitted for execution.
     */
    SUBMITTED(false),

    /**
     * The node's associated task (if any) executed successfully.
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
