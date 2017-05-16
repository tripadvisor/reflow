package com.tripadvisor.reflow;

class DefaultExecutionStrategy<T extends Task> implements ExecutionStrategy<T>
{
    @Override
    public void beforeNode(WorkflowNode<T> node)
    {}

    @Override
    public TaskCompletionBehavior afterTask(TaskResult<T> result)
    {
        return TaskCompletionBehavior.DEFAULT;
    }

    @Override
    public boolean beforeTaskOutputRemoval(TaskNode<T> node, OutputRemovalReason reason)
    {
        return true;
    }

    @Override
    public boolean beforeSingleOutputRemoval(TaskNode<T> node, Output output, OutputRemovalReason reason)
    {
        return true;
    }
}
