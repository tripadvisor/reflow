package com.tripadvisor.reflow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.tripadvisor.reflow.ExecutionStrategy.OutputRemovalReason;

/**
 * Manages workflow execution and output removal.
 *
 * @see Execution
 */
public class WorkflowExecutor<T extends Task>
{
    private final WorkflowCompletionService<T> m_completionService;
    private final ExecutionStrategy<T> m_strategy;

    /**
     * Cache of output instances associated with nodes. This is necessary when
     * using output instances as map keys: since the {@link Output} interface
     * doesn't specify equality semantics, we must assume reference equality.
     *
     * Nodes with no associated task are mapped to an empty set of outputs.
     */
    private final LoadingCache<WorkflowNode<T>, Set<Output>> m_outputCache = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<WorkflowNode<T>, Set<Output>>()
            {
                @Override
                public Set<Output> load(@Nonnull WorkflowNode<T> node)
                {
                    return node.hasTask() ? ImmutableSet.copyOf(node.getTask().getOutputs()) : ImmutableSet.of();
                }
            });

    private WorkflowExecutor(WorkflowCompletionService<T> completionService, ExecutionStrategy<T> strategy)
    {
        m_completionService = Preconditions.checkNotNull(completionService);
        m_strategy = Preconditions.checkNotNull(strategy);
    }

    /**
     * Creates a new executor using the given completion service and a default
     * execution strategy. The default strategy always removes output and
     * stops scheduling nodes after a failure.
     */
    public static <U extends Task> WorkflowExecutor<U> create(WorkflowCompletionService<U> completionService)
    {
        return new WorkflowExecutor<>(completionService, new DefaultExecutionStrategy<>());
    }

    /**
     * Creates a new executor using the given completion service and strategy.
     */
    public static <U extends Task> WorkflowExecutor<U> create(WorkflowCompletionService<U> completionService,
                                                              ExecutionStrategy<U> strategy)
    {
        return new WorkflowExecutor<>(completionService, strategy);
    }

    /**
     * Removes the output of all tasks in a target.
     */
    public void clearOutput(Target<T> target) throws IOException
    {
        for (WorkflowNode<T> node : target.getNodes().values())
        {
            if (node.hasTask())
            {
                TaskNode<T> taskNode = (TaskNode<T>) node;
                if (m_strategy.beforeTaskOutputRemoval(taskNode, OutputRemovalReason.REMOVAL_REQUESTED))
                {
                    for (Output output : taskNode.getTask().getOutputs())
                    {
                        if (m_strategy.beforeSingleOutputRemoval(taskNode, output,
                                                                 OutputRemovalReason.REMOVAL_REQUESTED))
                        {
                            output.delete();
                        }
                    }
                }
            }
        }
    }

    /**
     * Removes potentially out-of-date output of all tasks in a target.
     * Output is removed if the output of a direct or indirect dependency
     * is more recent.
     */
    public void invalidateOutput(Target<T> target) throws IOException
    {
        _invalidateOutput(target.getNodes().values(), true);
    }

    private Map<Output, Instant> _invalidateOutput(Collection<WorkflowNode<T>> targetNodes, boolean clearIfInvalid)
            throws IOException
    {
        // Cache output timestamps
        // Replace nulls with Instant.MAX (treat outputs that haven't been created yet as newer than anything else)
        Map<Output, Instant> timestamps = Maps.newHashMapWithExpectedSize(
                targetNodes.stream().mapToInt(node -> m_outputCache.getUnchecked(node).size()).sum());
        for (WorkflowNode<T> node : targetNodes)
        {
            for (Output output : m_outputCache.getUnchecked(node))
            {
                timestamps.put(output, output.getTimestamp().orElse(Instant.MAX));
            }
        }

        Map<WorkflowNode<T>, Instant> maxDependencyTimestamps =
                Maps.newHashMapWithExpectedSize(targetNodes.size());

        for (WorkflowNode<T> node : targetNodes)
        {
            // Calculate the most recent timestamp associated with the dependencies (direct/indirect) of this node
            Instant maxDependencyTimestamp = node.getDependencies().stream()
                    .flatMap(dependency -> Stream.concat(
                            // Consider the timestamps of the output of each direct dependency...
                            m_outputCache.getUnchecked(dependency).stream().map(timestamps::get),
                            // ...and indirect dependency
                            Stream.of(maxDependencyTimestamps.get(dependency))
                    ))
                    .max(Comparator.naturalOrder())
                    .orElse(Instant.MIN);

            maxDependencyTimestamps.put(node, maxDependencyTimestamp);

            // Calculate the least recent timestamp associated with the output of this node
            Instant minOutputTimestamp = m_outputCache.getUnchecked(node).stream()
                    .map(timestamps::get)
                    .min(Comparator.naturalOrder())
                    .orElse(Instant.MAX);

            // If a dependency has a more recent timestamp, the output of this node is invalid
            // Clear associated timestamps (and maybe actually delete the output)
            if (node.hasTask() && maxDependencyTimestamp.compareTo(minOutputTimestamp) > 0)
            {
                TaskNode<T> taskNode = (TaskNode<T>) node;
                boolean removeOutput = clearIfInvalid && m_strategy.beforeTaskOutputRemoval(
                        taskNode, OutputRemovalReason.PREDATES_DEPENDENCY);

                for (Output output : m_outputCache.getUnchecked(node))
                {
                    if (removeOutput && m_strategy.beforeSingleOutputRemoval(
                            taskNode, output, OutputRemovalReason.PREDATES_DEPENDENCY))
                    {
                        output.delete();
                    }

                    timestamps.put(output, Instant.MAX);
                }
            }
        }

        return timestamps;
    }

    /**
     * Returns an execution for all tasks in a target.
     */
    public Execution<T> execute(Target<T> target)
    {
        return Execution.create(m_completionService, m_strategy, target.getWorkflow(), target.getNodes().values());
    }

    /**
     * Returns an execution for some tasks in a target. Execution picks up
     * where it last ended based on the presence of task output.
     *
     * More specifically, nodes are marked for execution starting with the
     * nodes that have no dependents and examining dependencies recursively.
     * A task with output that is already present will not be rerun.
     */
    public Execution<T> executeFromExistingOutput(Target<T> target) throws IOException
    {
        Collection<WorkflowNode<T>> targetNodes = target.getNodes().values();

        Map<Output, Instant> timestamps = _invalidateOutput(targetNodes, false);

        Predicate<WorkflowNode<T>> isTailNode = node -> node.getDependents().stream()
                .noneMatch(targetNodes::contains);

        Predicate<WorkflowNode<T>> noOutputOrOutputMissing = node ->
        {
            Set<Output> outputs = m_outputCache.getUnchecked(node);
            return outputs.isEmpty() || outputs.stream()
                    .map(timestamps::get)
                    .anyMatch(Predicate.isEqual(Instant.MAX));
        };

        Set<WorkflowNode<T>> nodesToRun = TraversalUtils.collectNodes(
                targetNodes.stream()
                        .filter(isTailNode)
                        .filter(noOutputOrOutputMissing)
                        .iterator(),
                node -> node.getDependencies().stream()
                        .filter(noOutputOrOutputMissing)
                        .iterator()
        );

        return Execution.create(m_completionService, m_strategy, target.getWorkflow(), nodesToRun);
    }

    /**
     * Un-freezes an execution. The returned execution will use the resources
     * (completion service, etc.) associated with this executor.
     */
    public Execution<T> thaw(FrozenExecution<T> execution)
    {
        return Execution.thaw(m_completionService, m_strategy, execution);
    }
}
