package com.tripadvisor.reflow;

import java.util.Collection;

/**
 * An abstract unit of work. When run, a task should create all
 * of the outputs returned by its {@link #getOutputs()} method.
 */
public interface Task
{
    /**
     * Indicates what output data this task will create when run.
     * Any two calls to this method on this task should return equivalent
     * collections, in the sense that they represent the same output data.
     */
    Collection<Output> getOutputs();
}
