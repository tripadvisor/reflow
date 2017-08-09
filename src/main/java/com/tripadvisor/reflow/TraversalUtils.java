package com.tripadvisor.reflow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * Contains utility methods related to graph traversal.
 */
class TraversalUtils
{
    private TraversalUtils()
    {}

    /**
     * Explores a graph in a depth-first search starting from the given nodes,
     * using the given function to determine the neighbors of each node. When
     * no new nodes are reachable, returns the set of explored nodes.
     *
     * @param startNodes the nodes from which to start exploring
     * @param neighborsFunc a function defining the neighbors of a node
     * @return the set of nodes reachable from the start nodes using the
     * given neighbor function
     */
    public static <T extends Task> Set<WorkflowNode<T>> collectNodes(
            Iterable<WorkflowNode<T>> startNodes,
            Function<? super WorkflowNode<T>, ? extends Iterable<WorkflowNode<T>>> neighborsFunc)
    {
        return collectNodes(startNodes.iterator(), neighborsFunc.andThen(Iterable::iterator));
    }

    /**
     * Explores a graph in a depth-first search starting from the given nodes,
     * using the given function to determine the neighbors of each node. When
     * no new nodes are reachable, returns the set of explored nodes.
     *
     * @param startNodes iterator yielding nodes from which to start exploring
     * @param neighborsFunc a function defining the neighbors of a node
     * @return the set of nodes reachable from the start nodes using the
     * given neighbor function
     */
    public static <T extends Task> Set<WorkflowNode<T>> collectNodes(
            Iterator<WorkflowNode<T>> startNodes,
            Function<? super WorkflowNode<T>, ? extends Iterator<WorkflowNode<T>>> neighborsFunc)
    {
        Traversal<T> traversal = new Traversal<>(startNodes, neighborsFunc);
        while (traversal.hasNext())
        {
            traversal.next();
        }
        return traversal.getTraversedNodes();
    }

    /**
     * Explores a graph in a depth-first search starting from the given nodes,
     * using the given function to determine the neighbors of each node. The
     * returned iterator will yield a particular node at most once. When no new
     * nodes are reachable, the iterator is exhausted.
     *
     * <p>Iteration results are evaluated lazily, but depending on graph
     * structure, yielding only a handful of nodes may require significant
     * resources.</p>
     *
     * @param startNodes iterator yielding nodes from which to start exploring
     * @param neighborsFunc a function defining the neighbors of a node
     * @return an iterator yielding nodes reachable from the start nodes using
     * the given neighbor function
     */
    public static <T extends Task> Iterator<WorkflowNode<T>> traverseNodes(
            Iterator<WorkflowNode<T>> startNodes,
            Function<? super WorkflowNode<T>, ? extends Iterator<WorkflowNode<T>>> neighborsFunc)
    {
        return new Traversal<>(startNodes, neighborsFunc);
    }

    /**
     * Explores a graph in a depth-first search starting from the given nodes,
     * using the given function to determine the neighbors of each node.
     *
     * Not thread safe.
     */
    private static final class Traversal<T extends Task> extends AbstractIterator<WorkflowNode<T>>
    {
        private final Deque<WorkflowNode<T>> m_stack = new ArrayDeque<>();
        private final Set<WorkflowNode<T>> m_seen = new HashSet<>();
        private final Function<? super WorkflowNode<T>, ? extends Iterator<WorkflowNode<T>>> m_neighborsFunc;

        public Traversal(Iterator<WorkflowNode<T>> startNodes,
                         Function<? super WorkflowNode<T>, ? extends Iterator<WorkflowNode<T>>> neighborsFunc)
        {
            m_neighborsFunc = Preconditions.checkNotNull(neighborsFunc);
            while (startNodes.hasNext())
            {
                m_stack.addLast(startNodes.next());
            }
        }

        @Override
        protected WorkflowNode<T> computeNext()
        {
            while (!m_stack.isEmpty())
            {
                WorkflowNode<T> node = m_stack.removeLast();
                if (m_seen.add(node))
                {
                    Iterator<WorkflowNode<T>> iter = m_neighborsFunc.apply(node);
                    while (iter.hasNext())
                    {
                        m_stack.addLast(iter.next());
                    }
                    return node;
                }
            }
            return endOfData();
        }

        /**
         * Returns the set of nodes that have been returned
         * by a call to {@link #next()}.
         */
        public Set<WorkflowNode<T>> getTraversedNodes()
        {
            return Collections.unmodifiableSet(m_seen);
        }
    }

    /**
     * Given a graph represented as a set of nodes, returns the nodes sorted
     * topologically (dependency-free nodes first). If the input graph is not
     * acyclic, returns an empty optional. Dependencies and dependents absent
     * from the input set are ignored.
     *
     * @param nodes a set of nodes representing a graph
     * @return a topological sort of the nodes if the graph is acyclic;
     * otherwise, an empty optional
     */
    public static <T extends Task> Optional<List<WorkflowNode<T>>> topologicalSort(Collection<WorkflowNode<T>> nodes)
    {
        // This is an iterative version of Tarjan's algorithm.
        //
        // We initially mark all nodes as unseen, and while there are unseen
        // nodes remaining, we pick one at random and begin a depth first
        // search. A stack is maintained as in classic DFS, but we also keep
        // track of the nodes on the path between the start node and the node
        // currently being examined.
        //
        // When the current path is unwound (as we back up to try a path we
        // discovered earlier), its nodes are added to the list of results in
        // reverse order. This ensures that the dependencies of a node are
        // added to the list before the node itself.

        Set<WorkflowNode<T>> unseen = new HashSet<>(nodes);
        Set<WorkflowNode<T>> currentPath = new HashSet<>();
        Deque<WorkflowNode<T>> stack = new ArrayDeque<>();
        List<WorkflowNode<T>> results = new ArrayList<>(nodes.size());

        while (!unseen.isEmpty())
        {
            stack.addLast(unseen.iterator().next());

            while (!stack.isEmpty())
            {
                WorkflowNode<T> node = stack.getLast();

                // If this node is on the current path, we've already explored
                // it and its dependencies, so pop it off the stack and add it
                // to the list of results
                if (currentPath.contains(node))
                {
                    stack.removeLast();
                    currentPath.remove(node);
                    results.add(node);
                    continue;
                }

                // Otherwise, we haven't explored it yet
                // Mark it as seen and add it to the current path
                unseen.remove(node);
                currentPath.add(node);

                // Push the unseen dependencies of this node on the stack for
                // exploration. If any dependencies are already on the current
                // path, we've found a cycle
                for (WorkflowNode<T> dependency : node.getDependencies())
                {
                    if (unseen.contains(dependency))
                    {
                        stack.addLast(dependency);
                    }
                    else if (currentPath.contains(dependency))
                    {
                        return Optional.empty();
                    }
                }
            }
        }

        return Optional.of(results);
    }
}
