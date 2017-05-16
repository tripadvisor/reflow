package com.tripadvisor.reflow;

/**
 * A set of custom behaviors for a {@link WorkflowExecutor}.
 */
public interface ExecutionStrategy<T extends Task>
{
    /**
     * Possible reasons for output removal.
     */
    enum OutputRemovalReason
    {
        /**
         * Outputs are being removed because execution of the
         * corresponding node failed.
         */
        EXECUTION_FAILED,

        /**
         * Outputs are being removed because a rerun of the
         * corresponding node was requested.
         */
        RERUN_REQUESTED,

        /**
         * Outputs are being removed by request.
         */
        REMOVAL_REQUESTED,

        /**
         * Outputs are being invalidated by request, and there is newer output
         * associated with a dependency of the corresponding node.
         */
        PREDATES_DEPENDENCY,
    }

    /**
     * Options for handling nodes that have finished running.
     */
    enum TaskCompletionBehavior
    {
        /**
         * Continue running tasks if and only if the current task succeeded.
         */
        DEFAULT,

        /**
         * Behave as if the current task succeeded.
         * Dependent tasks will run even if the current task failed.
         */
        FORCE_SUCCESS,

        /**
         * Behave as if the current task failed.
         */
        FORCE_FAILURE,

        /**
         * Continue running tasks, even if the current task failed.
         * Dependent tasks will not run if the current task failed.
         */
        CONTINUE,

        /**
         * Stop running tasks, even if the current task succeeded.
         */
        HALT,

        /**
         * Run the current task again, discarding the current result.
         */
        RERUN,
    }

    /**
     * A hook for custom behavior, called when the dependencies of a node
     * have all been satisfied. This method is called before running the task
     * associated with a {@link TaskNode} and for the
     * {@link StructureNode}s in between tasks.
     *
     * @param node a node whose dependencies are satisfied
     */
    void beforeNode(WorkflowNode<T> node);

    /**
     * A hook for custom behavior, called when a task has finished running.
     * This method is called whether the task succeeded or failed. The returned
     * value indicates how graph execution should proceed.
     *
     * @param result details of the completed task
     * @return how to proceed
     */
    TaskCompletionBehavior afterTask(TaskResult<T> result);

    /**
     * A hook for custom behavior, called before a task's output is removed.
     * Output is only removed if this method returns true.
     *
     * @param node a node with task output that's about to be removed
     * @param reason the reason for output removal
     * @return whether to remove the output
     */
    boolean beforeTaskOutputRemoval(TaskNode<T> node, OutputRemovalReason reason);

    /**
     * A hook for custom behavior, called before an individual output is
     * removed. Output is only removed if this method returns true.
     *
     * @param node a node with task output that's about to be removed
     * @param output the specific output that's about to be removed
     * @param reason the reason for output removal
     * @return whether to remove the output
     */
    boolean beforeSingleOutputRemoval(TaskNode<T> node, Output output, OutputRemovalReason reason);
}
