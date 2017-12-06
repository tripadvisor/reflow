package com.tripadvisor.reflow;

import javax.annotation.Nullable;

/**
 * An object that manages the actual execution of tasks,
 * scheduling new tasks and signaling the completion of tasks via callback.
 *
 * <p>Scheduling a task yields a token object that represents the newly
 * scheduled task instance. Tokens can be used to register additional
 * callbacks at a later time.</p>
 *
 * <p>Schedulers may accept or reject tokens as they see fit, which
 * includes accepting tokens created by other scheduler instances.</p>
 */
public interface TaskScheduler<T>
{
    /**
     * Schedules a task and registers a callback object that will be used to
     * signal the completion of the task, successful or not. The callback may
     * be invoked directly by this method or in another thread after this
     * method returns.
     *
     * <p>Returns a token representing the scheduled task instance. The token
     * must be non-null unless the callback has already been invoked.</p>
     */
    @Nullable
    ScheduledTaskToken submit(T task, TaskCompletionCallback callback);

    /**
     * Given a token representing a scheduled task instance, registers a
     * callback object that will be used to signal the completion of the task,
     * successful or not. If the task has already been completed, immediately
     * invokes a callback method.
     *
     * @throws InvalidTokenException if {@code token} does not correspond to a
     * scheduled task instance
     */
    void registerCallback(ScheduledTaskToken token, TaskCompletionCallback callback) throws InvalidTokenException;
}
