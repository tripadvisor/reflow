package com.tripadvisor.reflow;

/**
 * The state of an execution.
 *
 * <p>Possible transitions between states:
 *
 * <p>{@link #IDLE} -&gt; {@link #RUNNING}
 * <br>{@link #RUNNING} -&gt; {@link #HALTING}
 * <br>{@link #HALTING} -&gt; {@link #IDLE}
 */
public enum ExecutionState
{
    /**
     * Execution is ready to start.
     */
    IDLE,

    /**
     * Execution is ongoing.
     */
    RUNNING,

    /**
     * Execution is ongoing, but no new tasks will be scheduled.
     * Execution will halt once submitted tasks have finished.
     */
    HALTING,
}
