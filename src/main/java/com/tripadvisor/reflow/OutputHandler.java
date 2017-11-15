package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import static java.util.stream.Collectors.toMap;

/**
 * Contains logic for removing output and for determining whether output
 * timestamps are consistent with task dependency relationships.
 *
 * <p>By default, output is removed when removal is explicitly requested
 * (by calling a method on an instance of this class) or when the associated
 * task fails to execute. If this is undesirable, output can be preserved
 * by passing an appropriate {@link OutputRemovalFilter} to the static factory
 * method.</p>
 *
 * <p>If the removal of a particular piece of output results in an exception,
 * any other pieces of output that were slated for removal may persist.</p>
 */
public class OutputHandler
{
    @Nullable
    private final OutputRemovalFilter m_outputRemovalFilter;

    /**
     * Cache of output instances associated with nodes. This is necessary when
     * using output instances as map keys: since the {@link Output} interface
     * doesn't specify equality semantics, we must assume reference equality.
     *
     * Nodes with no associated task are mapped to an empty set of outputs.
     */
    private final LoadingCache<WorkflowNode<?>, Collection<Output>> m_outputCache = CacheBuilder.newBuilder()
            .weakKeys()
            .build(new CacheLoader<WorkflowNode<?>, Collection<Output>>()
            {
                @Override
                public Collection<Output> load(WorkflowNode<?> node)
                {
                    return node.hasTask() ? ImmutableList.copyOf(node.getTask().getOutputs()) : ImmutableList.of();
                }
            });

    private OutputHandler(@Nullable OutputRemovalFilter outputRemovalFilter)
    {
        m_outputRemovalFilter = outputRemovalFilter;
    }

    /**
     * Creates a new instance. Output removal is not filtered;
     * if an output is deemed invalid, it will be removed unconditionally.
     */
    public static OutputHandler create()
    {
        return new OutputHandler(null);
    }

    /**
     * Creates a new instance with the given removal filter.
     */
    public static OutputHandler create(OutputRemovalFilter outputRemovalFilter)
    {
        return new OutputHandler(Preconditions.checkNotNull(outputRemovalFilter));
    }

    /**
     * Removes the output of all tasks in a target.
     *
     * @throws IOException if an I/O error occurs
     */
    public void removeOutput(Target<?> target) throws IOException
    {
        removeOutput(target.getNodes().values(), OutputRemovalReason.REMOVAL_REQUESTED);
    }

    /**
     * Removes the output of the given nodes.
     *
     * @param nodes the nodes for which to remove output
     * @param reason the reason for removing output
     * @throws IOException if an I/O error occurs
     */
    void removeOutput(Collection<? extends WorkflowNode<?>> nodes, OutputRemovalReason reason) throws IOException
    {
        if (nodes.isEmpty())
        {
            return;
        }

        if (m_outputRemovalFilter == null)
        {
            for (WorkflowNode<?> node : nodes)
            {
                if (node.hasTask())
                {
                    for (Output output : node.getTask().getOutputs())
                    {
                        output.delete();
                    }
                }
            }
        }
        else
        {
            Map<WorkflowNode<?>, Set<Output>> outputMap = nodes.stream()
                    .filter(WorkflowNode::hasTask)
                    .collect(toMap(Function.identity(), node -> new HashSet<>(node.getTask().getOutputs())));

            if (!outputMap.isEmpty())
            {
                m_outputRemovalFilter.filterRemovals(outputMap, reason);

                for (Set<Output> outputs : outputMap.values())
                {
                    for (Output output : outputs)
                    {
                        output.delete();
                    }
                }
            }
        }
    }

    /**
     * Removes potentially out-of-date output of all tasks in a target.
     * Output is removed if the output of a direct or indirect dependency
     * is more recent.
     *
     * @throws IOException if an I/O error occurs during validation or removal
     */
    public void removeInvalidOutput(Target<?> target) throws IOException
    {
        InvalidationResult<?> result = invalidateOutput(target.getNodes().values());
        removeOutput(result.getInvalidNodes(), OutputRemovalReason.PREDATES_DEPENDENCY);
    }

    /**
     * Checks the given nodes for potentially out-of-date output. Output is
     * considered out-of-date if the output of a direct or indirect dependency
     * is more recent.
     *
     * <p>Returns a map of output to validated timestamp (where out-of-date or
     * missing outputs are indicated by {@link Instant#MAX}) and a list of
     * nodes that had out-of-date output.</p>
     *
     * @throws IOException if an I/O error occurs
     */
    <T extends Task> InvalidationResult<T> invalidateOutput(Collection<WorkflowNode<T>> targetNodes) throws IOException
    {
        // Cache output timestamps
        // Replace nulls with Instant.MAX (treat outputs that haven't been created yet as newer than anything else)
        Map<Output, Instant> timestamps = new HashMap<>();
        for (WorkflowNode<T> node : targetNodes)
        {
            for (Output output : m_outputCache.getUnchecked(node))
            {
                timestamps.put(output, output.getTimestamp().orElse(Instant.MAX));
            }
        }

        Map<WorkflowNode<T>, Instant> maxDependencyTimestamps =
                Maps.newHashMapWithExpectedSize(targetNodes.size());

        Collection<WorkflowNode<T>> invalidNodes = new ArrayList<>();
        for (WorkflowNode<T> node : targetNodes)
        {
            // Calculate the most recent timestamp associated with the dependencies (direct/indirect) of this node
            Instant maxDependencyTimestamp = node.getDependencies().stream()
                    .filter(targetNodes::contains)
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
                invalidNodes.add(node);
                for (Output output : m_outputCache.getUnchecked(node))
                {
                    timestamps.put(output, Instant.MAX);
                }
            }
        }

        return new InvalidationResult<>(timestamps, invalidNodes);
    }

    static class InvalidationResult<T extends Task>
    {
        private final Map<Output, Instant> m_validatedTimestamps;
        private final Collection<WorkflowNode<T>> m_invalidNodes;

        private InvalidationResult(Map<Output, Instant> validatedTimestamps, Collection<WorkflowNode<T>> invalidNodes)
        {
            m_validatedTimestamps = validatedTimestamps;
            m_invalidNodes = invalidNodes;
        }

        public Map<Output, Instant> getValidatedTimestamps()
        {
            return m_validatedTimestamps;
        }

        public Collection<WorkflowNode<T>> getInvalidNodes()
        {
            return m_invalidNodes;
        }
    }
}
