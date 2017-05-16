package com.tripadvisor.reflow;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

/**
 * Job node completion service backed by an {@link Executor}.
 */
public class ExecutorWorkflowCompletionService<T extends Task> implements WorkflowCompletionService<T>
{
    private final Executor m_executor;
    private final Consumer<? super T> m_consumer;

    private final BlockingQueue<TaskResult<T>> m_resultsQueue = new LinkedBlockingQueue<>();

    private ExecutorWorkflowCompletionService(Executor executor, Consumer<? super T> consumer)
    {
        m_executor = Preconditions.checkNotNull(executor);
        m_consumer = Preconditions.checkNotNull(consumer);
    }

    /**
     * Returns a workflow completion service that implements task execution
     * using the given consumer. Execution takes place on the given executor.
     * The consumer must block until execution has finished, and must be
     * thread safe if the executor will execute more than one task at a time.
     */
    public static <U extends Task> ExecutorWorkflowCompletionService<U> from(Executor executor, Consumer<? super U> consumer)
    {
        return new ExecutorWorkflowCompletionService<>(executor, consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void submit(TaskNode<T> node)
    {
        m_executor.execute(new QueueingFuture(() -> m_consumer.accept(node.getTask()), node));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TaskResult<T> take() throws InterruptedException
    {
        return m_resultsQueue.take();
    }

    private class QueueingFuture extends FutureTask<Void>
    {
        private final TaskNode<T> m_node;

        QueueingFuture(Runnable runnable, TaskNode<T> node)
        {
            super(runnable, null);
            m_node = Preconditions.checkNotNull(node);
        }

        @Override
        protected void set(Void v)
        {
            super.set(v);
            m_resultsQueue.add(TaskResult.success(m_node));
        }

        @Override
        protected void setException(Throwable t)
        {
            super.setException(t);
            m_resultsQueue.add(TaskResult.failure(m_node, t));
        }
    }
}
