package com.tripadvisor.reflow;

/**
 * The state of an execution.
 *
 * <p>Possible transitions between states:
 *
 * <p>{@link #IDLE} -&gt; {@link #RUNNING}
 * <br>{@link #RUNNING} -&gt; {@link #IDLE}
 * <br>{@link #RUNNING} -&gt; {@link #SHUTDOWN}
 * <br>{@link #SHUTDOWN} -&gt; {@link #IDLE}
 */
public enum ExecutionState
{
    /**
     * Execution is ready to start.
     *
     * <p>Note that this state refers only to the idleness of the scheduling
     * loop, and tasks that have been scheduled but not completed may continue
     * to run.</p>
     */
    IDLE,

    /**
     * Execution is ongoing.
     */
    RUNNING,

    /**
     * Execution is ongoing, but no new tasks will be scheduled.
     * Execution will stop once all scheduled tasks have finished.
     */
    SHUTDOWN,
}
