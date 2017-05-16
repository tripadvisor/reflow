package com.tripadvisor.reflow;

import java.util.Set;

/**
 * An abstract task. When run, the task should create any associated output.
 */
public interface Task
{
    Set<Output> getOutputs();
}
