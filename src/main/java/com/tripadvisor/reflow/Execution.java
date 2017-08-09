package com.tripadvisor.reflow;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.tripadvisor.reflow.ExecutionStrategy.OutputRemovalReason;
import com.tripadvisor.reflow.ExecutionStrategy.TaskCompletionBehavior;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toConcurrentMap;

import static com.google.common.collect.Maps.toImmutableEnumMap;

/**
 * A single execution of a workflow, tracking which tasks have been completed
 * and scheduling new tasks as their dependencies are satisfied.
 */
public class Execution<T extends Task>
{
    private final WorkflowCompletionService<T> m_completionService;
    private final ExecutionStrategy<T> m_strategy;
    private final Workflow<T> m_workflow;

    private final Lock m_lock = new ReentrantLock();

    private final Map<WorkflowNode<T>, NodeState> m_nodeStates;
    private final ImmutableMap<NodeState, Set<WorkflowNode<T>>> m_nodesByState;

    private final Queue<WorkflowNode<T>> m_structureNodeQueue = new ArrayDeque<>();
    private final List<Exception> m_exceptions = new ArrayList<>(4);

    private volatile ExecutionState m_state = ExecutionState.IDLE;

    private Execution(WorkflowCompletionService<T> completionService, ExecutionStrategy<T> strategy,
                      Workflow<T> workflow, Map<WorkflowNode<T>, NodeState> nodeStates)
    {
        m_completionService = completionService;
        m_strategy = strategy;
        m_workflow = workflow;
        m_nodeStates = nodeStates;
        m_nodesByState = EnumSet.allOf(NodeState.class).stream().collect(toImmutableEnumMap(
                Function.identity(),
                nodeState -> m_nodeStates.entrySet().stream()
                        .filter(e -> e.getValue() == nodeState)
                        .map(Entry::getKey)
                        .collect(toCollection(ConcurrentHashMap::newKeySet))
        ));
    }

    static <U extends Task> Execution<U> create(WorkflowCompletionService<U> completionService,
                                                ExecutionStrategy<U> strategy,
                                                Workflow<U> workflow, Collection<WorkflowNode<U>> nodesToRun)
    {
        Preconditions.checkNotNull(completionService);
        Preconditions.checkNotNull(strategy);
        Preconditions.checkNotNull(workflow);
        ImmutableSet<WorkflowNode<U>> nodesToRunCopy = ImmutableSet.copyOf(nodesToRun);

        Map<WorkflowNode<U>, NodeState> nodeStates = workflow.getNodes().values().stream().collect(toConcurrentMap(
                Function.identity(),
                node -> !nodesToRunCopy.contains(node) ? NodeState.IRRELEVANT :
                        node.getDependencies().stream().anyMatch(nodesToRunCopy::contains) ? NodeState.NOT_READY :
                                NodeState.READY
        ));

        return new Execution<>(completionService, strategy, workflow, nodeStates);
    }

    static <U extends Task> Execution<U> thaw(WorkflowCompletionService<U> completionService,
                                              ExecutionStrategy<U> strategy, FrozenExecution<U> execution)
    {
        return new Execution<>(Preconditions.checkNotNull(completionService), Preconditions.checkNotNull(strategy),
                               execution.getWorkflow(), new ConcurrentHashMap<>(execution.getNodeStates()));
    }

    /**
     * Returns the current state of the execution.
     */
    public ExecutionState getState()
    {
        return m_state;
    }

    /**
     * Returns a read-only view of node states.
     * State transitions are reflected in the returned map as they occur.
     */
    public Map<WorkflowNode<T>, NodeState> getNodeStates()
    {
        return Collections.unmodifiableMap(m_nodeStates);
    }

    /**
     * Returns a snapshot of this execution.
     *
     * @throws IllegalStateException if this execution is currently running
     */
    public FrozenExecution<T> freeze()
    {
        if (!m_lock.tryLock())
        {
            throw new IllegalStateException("Can't freeze an Execution that's running");
        }
        try
        {
            return FrozenExecution.of(m_workflow, m_nodeStates);
        }
        finally
        {
            m_lock.unlock();
        }
    }

    /**
     * Schedules tasks in a loop and waits for them to finish. Scheduling
     * continues until every task with satisfied dependencies has run, or
     * until the associated {@link ExecutionStrategy} indicates it should stop.
     * The default execution strategy stops scheduling tasks if any task fails.
     *
     * @throws IllegalStateException if this execution is currently running
     * @throws IOException if output cannot be deleted after a failure
     * @throws InterruptedException if interrupted while waiting
     * @throws ExecutionException if a task fails
     */
    public void run() throws IOException, InterruptedException, ExecutionException
    {
        if (!m_lock.tryLock())
        {
            throw new IllegalStateException("Execution is already running");
        }
        try
        {
            m_exceptions.clear();
            m_state = ExecutionState.RUNNING;
            _submitReadyNodes();

            while (m_nodesByState.get(NodeState.SUBMITTED).size() > 0)
            {
                if (!m_structureNodeQueue.isEmpty())
                {
                    _completeNode(m_structureNodeQueue.remove());
                }
                else
                {
                    TaskResult<T> result;
                    try
                    {
                        result = m_completionService.take();
                    }
                    catch (InterruptedException e)
                    {
                        m_exceptions.add(e);
                        _maybeThrow();
                        throw e;
                    }
                    _handleResult(result);
                }
            }

            m_state = ExecutionState.IDLE;
            _maybeThrow();
        }
        finally
        {
            m_lock.unlock();
        }
    }

    private void _handleResult(TaskResult<T> result)
    {
        TaskNode<T> node = result.getNode();
        TaskCompletionBehavior behavior = m_strategy.afterTask(result);
        switch (behavior)
        {
            case FORCE_SUCCESS:
                _completeNode(node);
                break;

            case FORCE_FAILURE:
                m_state = ExecutionState.HALTING;
                _failNode(node, new ExecutionException("Forced failure for node " + node, result.getFailureCause()));
                break;

            case HALT:
                m_state = ExecutionState.HALTING;
                // Fall through
            case DEFAULT:
                if (!result.isSuccessful())
                {
                    m_state = ExecutionState.HALTING;
                }
                // Fall through
            case CONTINUE:
                if (result.isSuccessful())
                {
                    _completeNode(node);
                }
                else
                {
                    _failNode(node, new ExecutionException(result.getFailureCause()));
                }
                break;

            case RERUN:
                _removeOutputs(node, OutputRemovalReason.RERUN_REQUESTED);
                _submitNode(node);
                break;

            default:
                throw new RuntimeException("Unhandled TaskCompletionBehavior " + behavior);
        }
    }

    private void _completeNode(WorkflowNode<T> node)
    {
        _updateState(node, NodeState.SUCCEEDED);

        node.getDependents().stream()
                .filter(dependent -> m_nodeStates.get(dependent) == NodeState.NOT_READY)
                .filter(dependent -> dependent.getDependencies().stream()
                        .allMatch(dependency -> m_nodeStates.get(dependency).satisfiesDependency()))
                .forEach(dependent -> _updateState(dependent, NodeState.READY));

        _submitReadyNodes();
    }

    private void _failNode(TaskNode<T> node, ExecutionException failureCause)
    {
        _updateState(node, NodeState.FAILED);

        m_exceptions.add(failureCause);
        _removeOutputs(node, OutputRemovalReason.EXECUTION_FAILED);
    }

    private void _submitNode(WorkflowNode<T> node)
    {
        _updateState(node, NodeState.SUBMITTED);
        m_strategy.beforeNode(node);
        if (node.hasTask())
        {
            m_completionService.submit((TaskNode<T>) node);
        }
        else
        {
            m_structureNodeQueue.add(node);
        }
    }

    private void _submitReadyNodes()
    {
        Set<WorkflowNode<T>> readyNodes = m_nodesByState.get(NodeState.READY);
        while (m_state == ExecutionState.RUNNING && !readyNodes.isEmpty())
        {
            Iterator<WorkflowNode<T>> iter = readyNodes.iterator();
            WorkflowNode<T> next = iter.next();
            iter.remove();
            _submitNode(next);
        }
    }

    private void _removeOutputs(TaskNode<T> node, OutputRemovalReason reason)
    {
        if (m_strategy.beforeTaskOutputRemoval(node, reason))
        {
            for (Output output : node.getTask().getOutputs())
            {
                if (m_strategy.beforeSingleOutputRemoval(node, output, reason))
                {
                    try
                    {
                        output.delete();
                    }
                    catch (IOException e)
                    {
                        m_exceptions.add(e);
                    }
                }
            }
        }
    }

    private void _updateState(WorkflowNode<T> node, NodeState state)
    {
        m_nodeStates.put(node, state);
        m_nodesByState.entrySet().stream()
                .<Consumer<WorkflowNode<T>>>map(e -> e.getKey() == state ? e.getValue()::add : e.getValue()::remove)
                .forEach(c -> c.accept(node));
    }

    /**
     * Throws the most important exception seen so far, if any.
     */
    private void _maybeThrow() throws IOException, InterruptedException, ExecutionException
    {
        // Throw ExecutionExceptions, followed by IOExceptions, followed by InterruptedExceptions
        Iterator<Exception> iter = m_exceptions.stream()
                .sorted(Comparator.comparing(InterruptedException.class::isInstance)
                                .thenComparing(IOException.class::isInstance)
                                .thenComparing(ExecutionException.class::isInstance))
                .iterator();

        if (iter.hasNext())
        {
            Exception toThrow = iter.next();
            iter.forEachRemaining(toThrow::addSuppressed);
            Throwables.throwIfInstanceOf(toThrow, IOException.class);
            Throwables.throwIfInstanceOf(toThrow, InterruptedException.class);
            Throwables.throwIfInstanceOf(toThrow, ExecutionException.class);
            throw new AssertionError("Unexpected exception type", toThrow);
        }
    }
}
