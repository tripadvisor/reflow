package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toConcurrentMap;

import static com.google.common.collect.Maps.toImmutableEnumMap;

/**
 * A single execution of a workflow, tracking which tasks have been completed
 * and scheduling new tasks as their dependencies are satisfied. Executions can
 * only move forward: To rerun a task that has already been completed, create
 * a new execution.
 */
public class Execution<T extends Task>
{
    // Use of the GuardedBy annotation in this class is not exhaustive
    // because accessing guarded members in a stream expression trips up
    // Error Prone (see https://github.com/google/error-prone/issues/536)

    private final Workflow<T> m_workflow;

    private final TaskScheduler<? super T> m_scheduler;
    private final OutputHandler m_outputHandler;

    private final Lock m_lock = new ReentrantLock();

    @GuardedBy("m_lock")
    private final Condition m_taskNodeAvailable = m_lock.newCondition();

    private final Map<WorkflowNode<T>, NodeStatus> m_nodeStatuses;
    private final ImmutableMap<NodeState, Set<WorkflowNode<T>>> m_nodesByState;

    @GuardedBy("m_lock")
    private final Queue<StructureNode<T>> m_structureNodeQueue = new ArrayDeque<>();

    @GuardedBy("m_lock")
    private final Queue<TaskNodeCompletion<T>> m_taskNodeQueue = new ArrayDeque<>();

    @GuardedBy("m_lock")
    private final List<Exception> m_exceptions = new ArrayList<>();

    private volatile ExecutionState m_state = ExecutionState.IDLE;

    private volatile boolean m_shutdownOnFailure = true;

    @GuardedBy("m_lock")
    private Thread m_driverThread;

    private Execution(Workflow<T> workflow, TaskScheduler<? super T> scheduler, OutputHandler outputHandler,
                      Map<WorkflowNode<T>, NodeStatus> nodeStatuses)
    {
        m_workflow = Preconditions.checkNotNull(workflow);
        m_scheduler = Preconditions.checkNotNull(scheduler);
        m_outputHandler = Preconditions.checkNotNull(outputHandler);

        m_nodeStatuses = nodeStatuses;
        m_nodesByState = Arrays.stream(NodeState.values()).collect(toImmutableEnumMap(
                Function.identity(),
                state -> m_nodeStatuses.entrySet().stream()
                        .filter(e -> e.getValue().getState().equals(state))
                        .map(Entry::getKey)
                        .collect(toCollection(ConcurrentHashMap::newKeySet))
        ));
    }

    /**
     * Returns a new execution over the given nodes,
     * backed by the given task scheduler and output handler.
     */
    private static <U extends Task> Execution<U> newExecutionFromNodesToRun(Workflow<U> workflow,
                                                                            TaskScheduler<? super U> scheduler,
                                                                            OutputHandler outputHandler,
                                                                            Collection<WorkflowNode<U>> nodesToRun)
    {
        Set<WorkflowNode<U>> nodesToRunSet = nodesToRun instanceof Set ?
                (Set<WorkflowNode<U>>) nodesToRun : ImmutableSet.copyOf(nodesToRun);

        Map<WorkflowNode<U>, NodeStatus> nodeStates = workflow.getNodes().values().stream()
                .collect(toConcurrentMap(
                        Function.identity(),
                        node -> !nodesToRunSet.contains(node) ? NodeStatus.withoutToken(NodeState.IRRELEVANT) :
                                node.getDependencies().stream().anyMatch(nodesToRunSet::contains) ?
                                        NodeStatus.withoutToken(NodeState.NOT_READY) :
                                        NodeStatus.withoutToken(NodeState.READY)
                ));

        return new Execution<>(workflow, scheduler, outputHandler, nodeStates);
    }

    /**
     * Returns a new execution over the given target,
     * backed by the given task scheduler and a default output handler.
     *
     * <p>Equivalent to calling
     * {@link #newExecution(Target, TaskScheduler, OutputHandler)},
     * supplying {@link OutputHandler#create()} as the third parameter.</p>
     */
    public static <U extends Task> Execution<U> newExecution(Target<U> target, TaskScheduler<? super U> scheduler)
    {
        return newExecution(target, scheduler, OutputHandler.create());
    }

    /**
     * Returns a new execution over the given target,
     * backed by the given task scheduler and output handler.
     */
    public static <U extends Task> Execution<U> newExecution(Target<U> target,
                                                             TaskScheduler<? super U> scheduler,
                                                             OutputHandler outputHandler)
    {
        return newExecutionFromNodesToRun(target.getWorkflow(), scheduler, outputHandler, target.getNodes().values());
    }

    /**
     * Returns a new execution over the given target, backed by the given task
     * scheduler and a default output handler. Instead of including the entire
     * target, the execution will skip nodes with existing, up-to-date output.
     *
     * <p>Equivalent to calling
     * {@link #newExecutionFromExistingOutput(Target, TaskScheduler, OutputHandler)},
     * supplying {@link OutputHandler#create()} as the third parameter.</p>
     *
     * @throws IOException if an I/O error occurs during output validation
     */
    public static <U extends Task> Execution<U> newExecutionFromExistingOutput(Target<U> target,
                                                                               TaskScheduler<? super U> scheduler)
            throws IOException
    {
        return newExecutionFromExistingOutput(target, scheduler, OutputHandler.create());
    }

    /**
     * Returns a new execution over the given target, backed by the given task
     * scheduler and a default output handler. Instead of including the entire
     * target, the execution will skip tasks with existing, up-to-date output.
     *
     * <p>Initially, the set of nodes to run contains all target nodes without
     * dependents that are also in the target. Next, it is limited to nodes
     * without existing output (specifically, nodes that have no associated
     * task, nodes with an associated task that creates no output, and nodes
     * with an output-creating task where the output has not been created or
     * is out of-date). Finally, the set is repeatedly expanded to include
     * direct dependencies without existing output, stopping when no such
     * dependencies exist.</p>
     *
     * @throws IOException if an I/O error occurs during output validation
     */
    public static <U extends Task> Execution<U> newExecutionFromExistingOutput(Target<U> target,
                                                                               TaskScheduler<? super U> scheduler,
                                                                               OutputHandler outputHandler)
            throws IOException
    {
        Collection<WorkflowNode<U>> targetNodes = target.getNodes().values();
        Map<Output, Instant> timestamps = outputHandler.invalidateOutput(targetNodes).getValidatedTimestamps();

        Predicate<WorkflowNode<U>> isTailNode = node -> node.getDependents().stream().noneMatch(targetNodes::contains);

        Predicate<WorkflowNode<U>> noOutputOrOutputMissing = node ->
        {
            if (!node.hasTask())
            {
                return true;
            }
            Collection<Output> outputs = node.getTask().getOutputs();
            return outputs.isEmpty() || outputs.stream()
                    .map(timestamps::get)
                    .anyMatch(Predicate.isEqual(Instant.MAX));
        };

        Set<WorkflowNode<U>> nodesToRun = TraversalUtils.collectNodes(
                targetNodes.stream()
                        .filter(isTailNode)
                        .filter(noOutputOrOutputMissing)
                        .iterator(),
                node -> node.getDependencies().stream()
                        .filter(noOutputOrOutputMissing)
                        .iterator()
        );

        return newExecutionFromNodesToRun(target.getWorkflow(), scheduler, outputHandler, nodesToRun);
    }

    /**
     * Un-freezes an execution, returning a new execution backed by the given
     * task scheduler and a default output handler.
     *
     * <p>Equivalent to calling
     * {@link #thaw(FrozenExecution, TaskScheduler, OutputHandler)},
     * supplying {@link OutputHandler#create()} as the third parameter.</p>
     *
     * @throws InvalidTokenException if the frozen execution includes scheduled
     * task tokens that are rejected by the given scheduler
     */
    public static <U extends Task> Execution<U> thaw(FrozenExecution<U> frozen, TaskScheduler<? super U> scheduler)
            throws InvalidTokenException
    {
        return thaw(frozen, scheduler, OutputHandler.create());
    }

    /**
     * Un-freezes an execution, returning a new execution backed by the given
     * task scheduler and output handler.
     *
     * @throws InvalidTokenException if the frozen execution includes scheduled
     * task tokens that are rejected by the given scheduler
     */
    public static <U extends Task> Execution<U> thaw(FrozenExecution<U> frozen,
                                                     TaskScheduler<? super U> scheduler,
                                                     OutputHandler outputHandler) throws InvalidTokenException
    {
        Execution<U> thawed = new Execution<>(frozen.getWorkflow(), scheduler, outputHandler,
                                              new ConcurrentHashMap<>(frozen.getNodeStatuses()));
        thawed.updateReadiness();
        thawed.registerCallbacks();
        return thawed;
    }

    /**
     * Returns the current state of the execution.
     */
    public ExecutionState getState()
    {
        return m_state;
    }

    /**
     * Returns a live, read-only view of node statuses.
     */
    public Map<WorkflowNode<T>, NodeStatus> getNodeStatuses()
    {
        return Collections.unmodifiableMap(m_nodeStatuses);
    }

    /**
     * Returns whether this execution will shut down
     * when any task fails to execute.
     */
    public boolean isShutdownOnFailure()
    {
        return m_shutdownOnFailure;
    }

    /**
     * Sets whether this execution will shut down
     * when any task fails to execute.
     */
    public void setShutdownOnFailure(boolean shutdownOnFailure)
    {
        m_shutdownOnFailure = shutdownOnFailure;
    }

    /**
     * Returns a snapshot of this execution.
     *
     * <p>The returned snapshot is guaranteed to be consistent. However, it can
     * quickly become out-of-date if the execution is running, or if any tasks
     * have been scheduled but not completed.</p>
     *
     * <p>If any tasks are in the process of being scheduled (the scheduler's
     * {@link TaskScheduler#submit(Object, TaskCompletionCallback) submit}
     * method has been called but has not yet returned a token), the associated
     * nodes will be marked {@link NodeState#READY READY}.</p>
     */
    public FrozenExecution<T> freeze()
    {
        m_lock.lock();
        try
        {
            return FrozenExecution.of(m_workflow, m_nodeStatuses);
        }
        finally
        {
            m_lock.unlock();
        }
    }

    /**
     * Schedules tasks in a loop and waits for them to finish. Scheduling
     * continues until every task with satisfied dependencies has run or the
     * execution is interrupted. If a scheduled task fails, its output is
     * removed.
     *
     * @throws IllegalStateException if this execution is already running
     * @throws ExecutionException if a task fails
     * @throws InterruptedException if interrupted while waiting
     */
    public void run() throws ExecutionException, InterruptedException
    {
        m_lock.lock();
        try
        {
            if (m_driverThread != null)
            {
                throw new IllegalStateException("Execution is already running");
            }
            m_driverThread = Thread.currentThread();
            m_state = ExecutionState.RUNNING;

            try
            {
                submitReadyNodes();

                while (!m_nodesByState.get(NodeState.SCHEDULED).isEmpty()
                        || !m_structureNodeQueue.isEmpty()
                        || !m_taskNodeQueue.isEmpty())
                {
                    // Check for a queued structure node
                    WorkflowNode<T> node = m_structureNodeQueue.poll();
                    if (node != null)
                    {
                        updateDependentReadiness(node);
                        submitReadyNodes();
                        continue;  // In case this was the last node
                    }

                    // Wait for a queued task node
                    TaskNodeCompletion<T> completion;
                    while ((completion = m_taskNodeQueue.poll()) == null)
                    {
                        try
                        {
                            m_taskNodeAvailable.await();
                        }
                        catch (InterruptedException e)
                        {
                            m_exceptions.add(e);
                            throwStoredExceptions();
                            throw e;
                        }
                    }

                    node = completion.getNode();
                    if (m_nodeStatuses.get(node).getState().equals(NodeState.SUCCEEDED))
                    {
                        updateDependentReadiness(node);
                        submitReadyNodes();
                    }
                    else
                    {
                        if (m_shutdownOnFailure)
                        {
                            m_state = ExecutionState.SHUTDOWN;
                        }

                        m_exceptions.add(completion.newExecutionException());

                        try
                        {
                            m_outputHandler.removeOutput(ImmutableSet.of(node),
                                                         OutputRemovalReason.EXECUTION_FAILED);
                        }
                        catch (IOException e)
                        {
                            m_exceptions.add(e);
                        }
                    }
                }

                throwStoredExceptions();
            }
            catch (Exception e)
            {
                m_exceptions.add(e);
                throwStoredExceptions();
                throw e;
            }
            finally
            {
                m_state = ExecutionState.IDLE;
                m_driverThread = null;
            }
        }
        finally
        {
            m_lock.unlock();
        }
    }

    /**
     * Shuts down this execution and returns immediately. Scheduled tasks will
     * continue to run, no new tasks will be scheduled. The current call to
     * {@link #run()} will return when all scheduled tasks have completed.
     *
     * <p>If it does not coincide with a call to {@link #run()}, this method
     * has no effect.</p>
     */
    public void shutdown()
    {
        m_lock.lock();
        try
        {
            if (m_state.equals(ExecutionState.RUNNING))
            {
                m_state = ExecutionState.SHUTDOWN;
            }
        }
        finally
        {
            m_lock.unlock();
        }
    }

    /**
     * Interrupts this execution and returns immediately. The current call to
     * {@link #run()} should return shortly afterwards, possibly with an
     * {@link InterruptedException}. Scheduled tasks may continue to run.
     *
     * <p>If it does not coincide with a call to {@link #run()}, this method
     * has no effect.</p>
     */
    public void interrupt()
    {
        m_lock.lock();
        try
        {
            if (m_driverThread != null)
            {
                m_driverThread.interrupt();
            }
        }
        finally
        {
            m_lock.unlock();
        }
    }

    @GuardedBy("m_lock")
    private void registerCallbacks() throws InvalidTokenException
    {
        for (WorkflowNode<T> node : m_nodesByState.get(NodeState.SCHEDULED))
        {
            Optional<ScheduledTaskToken> token = m_nodeStatuses.get(node).getToken();
            assert node.hasTask() : "Scheduled structure node";
            assert token.isPresent() : "Missing token";
            m_scheduler.registerCallback(token.get(), new WeakCallback(new QueueingCallback((TaskNode<T>) node)));
        }
    }

    @GuardedBy("m_lock")
    private void updateReadiness()
    {
        updateReadiness(m_nodesByState.get(NodeState.NOT_READY).stream());
    }

    @GuardedBy("m_lock")
    private void updateDependentReadiness(WorkflowNode<T> node)
    {
        updateReadiness(
                node.getDependents().stream()
                        .filter(dependent -> m_nodeStatuses.get(dependent).getState().equals(NodeState.NOT_READY))
        );
    }

    @GuardedBy("m_lock")
    private void updateReadiness(Stream<WorkflowNode<T>> potentiallyReadyNodes)
    {
        potentiallyReadyNodes
                .filter(dependent -> dependent.getDependencies().stream()
                        .allMatch(dependency -> m_nodeStatuses.get(dependency).getState().satisfiesDependency()))
                .forEach(dependent -> updateStatus(dependent, NodeState.READY));
    }

    @GuardedBy("m_lock")
    private void submitReadyNodes()
    {
        Set<WorkflowNode<T>> readyNodes = m_nodesByState.get(NodeState.READY);
        while (m_state == ExecutionState.RUNNING && !readyNodes.isEmpty())
        {
            Iterator<WorkflowNode<T>> iter = readyNodes.iterator();
            WorkflowNode<T> node = iter.next();
            iter.remove();

            if (node.hasTask())
            {
                // We want to store the token from the m_taskExecutor.submit() call in the node's
                // state object. However, in the case of a direct executor, submit() will do the
                // actual task execution and invoke a completion callback before we get a token.
                // To begin with, set the state to SCHEDULED with no token.
                updateStatus(node, NodeState.SCHEDULED);

                // Submit the task, temporarily releasing the lock in case submit() blocks
                ScheduledTaskToken token;
                m_lock.unlock();
                try
                {
                    token = m_scheduler.submit(node.getTask(),
                                               new WeakCallback(new QueueingCallback((TaskNode<T>) node)));
                }
                finally
                {
                    m_lock.lock();
                }

                // Only update state if submit() didn't do it for us
                if (m_nodeStatuses.get(node).getState().equals(NodeState.SCHEDULED))
                {
                    updateStatus(node, NodeStatus.scheduledWithToken(token));
                }
            }
            else
            {
                updateStatus(node, NodeState.SUCCEEDED);
                m_structureNodeQueue.add((StructureNode<T>) node);
            }
        }
    }

    // @GuardedBy("m_lock")
    private void updateStatus(WorkflowNode<T> node, NodeState state)
    {
        updateStatus(node, NodeStatus.withoutToken(state));
    }

    // @GuardedBy("m_lock")
    private void updateStatus(WorkflowNode<T> node, NodeStatus status)
    {
        m_nodeStatuses.put(node, status);

        for (Entry<NodeState, Set<WorkflowNode<T>>> e : m_nodesByState.entrySet())
        {
            if (e.getKey().equals(status.getState()))
            {
                e.getValue().add(node);
            }
            else
            {
                e.getValue().remove(node);
            }
        }
    }

    /**
     * If the list of stored exceptions contains any exceptions, throws the
     * most important one, with the rest attached as suppressed exceptions.
     * Otherwise, returns normally. Clears the stored exception list.
     *
     * <p>Unchecked exceptions and checked exceptions of an unexpected type are
     * considered most important, followed by instances of ExecutionException
     * and InterruptedException. Unexpected checked exceptions are wrapped in
     * an AssertionError.</p>
     *
     * <p>It's possible for the list to contain an IOException, but it should
     * always be accompanied by an ExecutionException, so the top-level
     * exception can't be an IOException.</p>
     */
    @GuardedBy("m_lock")
    private void throwStoredExceptions() throws ExecutionException, InterruptedException
    {
        m_exceptions.sort(Comparator.comparing(InterruptedException.class::isInstance)
                                  .thenComparing(IOException.class::isInstance)
                                  .thenComparing(ExecutionException.class::isInstance));

        Iterator<Exception> iter = m_exceptions.iterator();
        if (iter.hasNext())
        {
            Exception toThrow = iter.next();
            iter.forEachRemaining(toThrow::addSuppressed);
            m_exceptions.clear();

            Throwables.throwIfInstanceOf(toThrow, InterruptedException.class);
            Throwables.throwIfInstanceOf(toThrow, ExecutionException.class);
            Throwables.throwIfUnchecked(toThrow);
            throw new AssertionError("Unexpected checked exception", toThrow);
        }
    }

    /**
     * Record of a task execution finishing (successfully or otherwise).
     * References the corresponding node and an optional failure cause.
     * The lack of a failure cause does not indicate success.
     */
    private static class TaskNodeCompletion<U extends Task>
    {
        private final TaskNode<U> m_node;

        @Nullable
        private final String m_message;

        @Nullable
        private final Throwable m_failureCause;

        public TaskNodeCompletion(TaskNode<U> node, @Nullable String message, @Nullable Throwable failureCause)
        {
            m_node = node;
            m_message = message;
            m_failureCause = failureCause;
        }

        public TaskNode<U> getNode()
        {
            return m_node;
        }

        public ExecutionException newExecutionException()
        {
            StringBuilder sb = new StringBuilder("Task for node ").append(m_node.getKey()).append(" failed");

            if (m_message != null)
            {
                sb.append(": ").append(m_message);
            }

            return new ExecutionException(sb.toString(), m_failureCause);
        }
    }

    /**
     * Task completion callback that updates the state of the
     * corresponding node and wakes up the thread driving execution.
     */
    private class QueueingCallback implements TaskCompletionCallback
    {
        private final TaskNode<T> m_node;

        public QueueingCallback(TaskNode<T> node)
        {
            m_node = node;
        }

        @Override
        public void reportSuccess()
        {
            queueResult(new TaskNodeCompletion<>(m_node, null, null), NodeState.SUCCEEDED);
        }

        @Override
        public void reportFailure()
        {
            queueResult(new TaskNodeCompletion<>(m_node, null, null), NodeState.FAILED);
        }

        @Override
        public void reportFailure(String message)
        {
            queueResult(new TaskNodeCompletion<>(m_node, Preconditions.checkNotNull(message), null), NodeState.FAILED);
        }

        @Override
        public void reportFailure(String message, Throwable cause)
        {
            queueResult(new TaskNodeCompletion<>(m_node,
                                                 Preconditions.checkNotNull(message),
                                                 Preconditions.checkNotNull(cause)),
                        NodeState.FAILED);
        }

        @Override
        public void reportFailure(Throwable cause)
        {
            queueResult(new TaskNodeCompletion<>(m_node, null, Preconditions.checkNotNull(cause)), NodeState.FAILED);
        }

        private void queueResult(TaskNodeCompletion<T> result, NodeState state)
        {
            m_lock.lock();
            try
            {
                if (m_nodeStatuses.get(m_node).getState().equals(NodeState.SCHEDULED))
                {
                    updateStatus(m_node, state);
                    m_taskNodeQueue.add(result);
                    m_taskNodeAvailable.signal();
                }
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }
}
