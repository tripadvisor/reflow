package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * A helper class for building workflows in a fluent style.
 *
 * <p>Chains consist of one or more "segments", which are collections of node
 * builders (or sub-chains) to run in parallel. Each segment includes a "head",
 * which is a {@link StructureNode.Builder} added automatically as a dependency
 * of the segment's contents, and a "tail", another builder that depends on the
 * segment's contents. Adjacent segments share builders: the tail of the first
 * segment is the same object as the head of the second and so on.</p>
 *
 * <p>The head of the first segment is called the head of the overall chain,
 * and every other builder in the chain depends on it (directly or indirectly).
 * The tail of the last segment is the tail of the overall chain, dependent
 * on every other builder.</p>
 *
 * <p>Chains maintain a set of all contained builders, accessible via the
 * {@link #getContents()} method. Additional builders can be added to this
 * set if desired. For example, one might add dependencies by hand to the tail
 * of a partially-constructed chain, then add those dependencies to the chain
 * contents. This allows the expression of graphs cannot be completely
 * constructed in a fluent style.</p>
 *
 * <p>Instances are not thread safe.</p>
 *
 * <h2>Examples</h2>
 *
 * <p>In the ASCII illustrations that follow, nodes on the right depend on
 * those on the left. The 'o' characters indicate automatically-created
 * {@link StructureNode} instances.</p>
 *
 * <p><strong>Code:</strong></p>
 * <code>BuilderChain.of(a)</code>
 * <p><strong>Result:</strong></p>
 * <pre>
 * o - (a) - o
 * </pre>
 *
 * <p><strong>Code:</strong></p>
 * <code>BuilderChain.of(a).andThen(b)</code>
 * <p><strong>Result:</strong></p>
 * <pre>
 * o - (a) - o - (b) - o
 * </pre>
 *
 * <p><strong>Code:</strong></p>
 * <code>BuilderChain.of(a, b)</code>
 * <p><strong>Result:</strong></p>
 * <pre>
 *   (a)
 *  /   \
 * o     o
 *  \   /
 *   (b)
 * </pre>
 *
 * <p><strong>Code:</strong></p>
 * <code>BuilderChain.ofChains(BuilderChain.of(a), BuilderChain.of(b))</code>
 * <p><strong>Result:</strong></p>
 * <pre>
 *   o - (a) - o
 *  /           \
 * o             o
 *  \           /
 *   o - (b) - o
 * </pre>
 */
public class BuilderChain<K, T extends Task>
{
    private WorkflowNode.Builder<K, T> m_head = StructureNode.builder();
    private WorkflowNode.Builder<K, T> m_tail = StructureNode.builder();

    private Set<WorkflowNode.Builder<K, T>> m_contents = new HashSet<>();

    private BuilderChain()
    {}

    @SafeVarargs
    private static <U> Stream<U> _stream(U first, U... rest)
    {
        return Lists.asList(first, rest).stream();
    }

    /**
     * Returns a single-segment chain containing a builder
     * for each of the given tasks in parallel.
     */
    @SafeVarargs
    public static <KK, TT extends Task> BuilderChain<KK, TT> ofTasks(TT task, TT... moreTasks)
    {
        return _of(_stream(task, moreTasks).map(TaskNode::builder));
    }

    /**
     * Returns a single-segment chain containing a builder
     * for each of the given tasks in parallel.
     */
    public static <KK, TT extends Task> BuilderChain<KK, TT> ofTasks(Collection<? extends TT> tasks)
    {
        return tasks.isEmpty() ? _ofEmptySegment() : _of(tasks.stream().map(TaskNode::builder));
    }

    /**
     * Returns a single-segment chain containing each
     * of the given builders in parallel.
     */
    @SafeVarargs
    public static <KK, TT extends Task> BuilderChain<KK, TT> of(WorkflowNode.Builder<KK, TT> builder,
                                                                WorkflowNode.Builder<KK, TT>... moreBuilders)
    {
        return _of(_stream(builder, moreBuilders));
    }

    /**
     * Returns a single-segment chain containing each
     * of the given builders in parallel.
     */
    public static <KK, TT extends Task> BuilderChain<KK, TT> of(Collection<WorkflowNode.Builder<KK, TT>> builders)
    {
        return builders.isEmpty() ? _ofEmptySegment() : _of(builders.stream());
    }

    private static <KK, TT extends Task> BuilderChain<KK, TT> _of(Stream<WorkflowNode.Builder<KK, TT>> builders)
    {
        BuilderChain<KK, TT> self = new BuilderChain<>();

        builders.peek(builder -> builder.addDependencies(self.m_head))
                .peek(builder -> self.m_tail.addDependencies(builder))
                .forEach(self.m_contents::add);

        return self;
    }

    /**
     * Returns a single-segment chain containing
     * the given sub-chains in parallel.
     */
    @SafeVarargs
    public static <KK, TT extends Task> BuilderChain<KK, TT> ofChains(BuilderChain<KK, TT> chain,
                                                                      BuilderChain<KK, TT>... moreChains)
    {
        return _ofChains(_stream(chain, moreChains));
    }

    /**
     * Returns a single-segment chain containing
     * the given sub-chains in parallel.
     */
    public static <KK, TT extends Task> BuilderChain<KK, TT> ofChains(Collection<BuilderChain<KK, TT>> chains)
    {
        return chains.isEmpty() ? _ofEmptySegment() : _ofChains(chains.stream());
    }

    private static <KK, TT extends Task> BuilderChain<KK, TT> _ofChains(Stream<BuilderChain<KK, TT>> chains)
    {
        BuilderChain<KK, TT> self = new BuilderChain<>();

        chains.peek(chain -> chain.m_head.addDependencies(self.m_head))
                .peek(chain -> self.m_tail.addDependencies(chain.m_tail))
                .peek(chain -> self.m_contents.add(chain.m_head))
                .peek(chain -> self.m_contents.addAll(chain.m_contents))
                .forEach(chain -> self.m_contents.add(chain.m_tail));

        return self;
    }

    private static <KK, TT extends Task> BuilderChain<KK, TT> _ofEmptySegment()
    {
        BuilderChain<KK, TT> self = new BuilderChain<>();
        self.m_tail.addDependencies(self.m_head);
        return self;
    }

    /**
     * Adds a segment containing a builder for each of the given tasks
     * in parallel. The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    @SafeVarargs
    public final BuilderChain<K, T> andThenTasks(T task, T... moreTasks)
    {
        return _andThen(_stream(task, moreTasks).map(TaskNode::builder));
    }

    /**
     * Adds a segment containing a builder for each of the given tasks
     * in parallel. The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    public BuilderChain<K, T> andThenTasks(Collection<? extends T> tasks)
    {
        return tasks.isEmpty() ? _andThenEmptySegment() : _andThen(tasks.stream().map(TaskNode::builder));
    }

    /**
     * Adds a segment containing each of the given builders in parallel.
     * The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    @SafeVarargs
    public final BuilderChain<K, T> andThen(WorkflowNode.Builder<K, T> builder,
                                            WorkflowNode.Builder<K, T>... moreBuilders)
    {
        return _andThen(_stream(builder, moreBuilders));
    }

    /**
     * Adds a segment containing each of the given builders in parallel.
     * The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    public BuilderChain<K, T> andThen(Collection<WorkflowNode.Builder<K, T>> builders)
    {
        return builders.isEmpty() ? _andThenEmptySegment() : _andThen(builders.stream());
    }

    private BuilderChain<K, T> _andThen(Stream<WorkflowNode.Builder<K, T>> builders)
    {
        WorkflowNode.Builder<K, T> newTail = StructureNode.builder();

        builders.peek(builder -> builder.addDependencies(m_tail))
                .peek(builder -> newTail.addDependencies(builder))
                .forEach(m_contents::add);

        m_contents.add(m_tail);
        m_tail = newTail;
        return this;
    }

    /**
     * Adds a segment containing the given sub-chains in parallel.
     * The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    @SafeVarargs
    public final BuilderChain<K, T> andThenChains(BuilderChain<K, T> chain, BuilderChain<K, T>... moreChains)
    {
        return _andThenChains(_stream(chain, moreChains));
    }

    /**
     * Adds a segment containing the given sub-chains in parallel.
     * The new segment is placed at the end of this chain.
     *
     * @return this chain, mutated
     */
    public BuilderChain<K, T> andThenChains(Collection<BuilderChain<K, T>> chains)
    {
        return chains.isEmpty() ? _andThenEmptySegment() : _andThenChains(chains.stream());
    }

    private BuilderChain<K, T> _andThenChains(Stream<BuilderChain<K, T>> chains)
    {
        WorkflowNode.Builder<K, T> newTail = StructureNode.builder();

        chains.peek(chain -> chain.m_head.addDependencies(m_tail))
                .peek(chain -> newTail.addDependencies(chain.m_tail))
                .peek(chain -> m_contents.add(chain.m_head))
                .peek(chain -> m_contents.addAll(chain.m_contents))
                .forEach(chain -> m_contents.add(chain.m_tail));

        m_contents.add(m_tail);
        m_tail = newTail;
        return this;
    }

    private BuilderChain<K, T> _andThenEmptySegment()
    {
        WorkflowNode.Builder<K, T> newTail = StructureNode.builder();
        newTail.addDependencies(m_tail);
        m_contents.add(m_tail);
        m_tail = newTail;
        return this;
    }

    /**
     * Sets the key associated with the head of this chain.
     *
     * @return this chain, mutated
     */
    public BuilderChain<K, T> setHeadKey(@Nullable K key)
    {
        m_head.setKey(key);
        return this;
    }

    /**
     * Sets the key associated with the current tail of this chain.
     *
     * @return this chain, mutated
     */
    public BuilderChain<K, T> setTailKey(@Nullable K key)
    {
        m_tail.setKey(key);
        return this;
    }

    /**
     * Returns the head of this chain.
     */
    public WorkflowNode.Builder<K, T> getHead()
    {
        return m_head;
    }

    /**
     * Returns the current tail of this chain.
     */
    public WorkflowNode.Builder<K, T> getTail()
    {
        return m_tail;
    }

    /**
     * Returns the set of builders in this chain.
     * Elements may be added to, but not removed from, the returned set.
     */
    public Set<WorkflowNode.Builder<K, T>> getContents()
    {
        return new AbstractSet<WorkflowNode.Builder<K, T>>()
        {
            @Override
            public Iterator<WorkflowNode.Builder<K, T>> iterator()
            {
                List<WorkflowNode.Builder<K, T>> contents = new ArrayList<>(size());
                contents.add(m_head);
                contents.add(m_tail);
                contents.addAll(m_contents);
                return Iterators.unmodifiableIterator(contents.iterator());
            }

            @Override
            public int size()
            {
                return m_contents.size() + 2;
            }

            @Override
            public boolean contains(Object o)
            {
                return m_head.equals(o) || m_tail.equals(o) || m_contents.contains(o);
            }

            @Override
            public boolean add(WorkflowNode.Builder<K, T> builder)
            {
                return !m_head.equals(builder) && !m_tail.equals(builder) && m_contents.add(builder);
            }

            @Override
            public boolean removeAll(Collection<?> c)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeIf(Predicate<? super WorkflowNode.Builder<K, T>> filter)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
