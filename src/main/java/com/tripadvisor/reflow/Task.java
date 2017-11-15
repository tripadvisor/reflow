package com.tripadvisor.reflow;

import java.util.Collection;

/**
 * An abstract task. When run, the task should create any associated output.
 */
public interface Task
{
    Collection<Output> getOutputs();
}
