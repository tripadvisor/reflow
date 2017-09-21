package com.tripadvisor.reflow;

/**
 * A collection of callback methods to be invoked
 * when a scheduled task is completed.
 */
public interface TaskCompletionCallback
{
    /**
     * Reports the success of a task.
     */
    void reportSuccess();

    /**
     * Reports the failure of a task.
     */
    void reportFailure();

    /**
     * Reports the failure of a task with an explanatory message.
     */
    void reportFailure(String message);

    /**
     * Reports the failure of a task, caused by a non-null throwable,
     * with an explanatory message.
     */
    void reportFailure(String message, Throwable cause);

    /**
     * Reports the failure of a task, caused by a non-null throwable.
     */
    void reportFailure(Throwable cause);
}
