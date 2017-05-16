package com.tripadvisor.reflow;

import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.testng.Assert.fail;

import static com.tripadvisor.reflow.WorkflowNodeSubject.assertThat;

public final class BuilderChainTest
{
    private static Task _task(int id)
    {
        return new NoOpTask(id);
    }

    private static BuilderChain<Integer, Task> _chainOf(Task task, Task... tasks)
    {
        return BuilderChain.ofTasks(task, tasks);
    }

    @Test
    public void testBuild()
    {
        Workflow<Integer, Task> graph;

        // Two segments, two nodes per segment, no keys
        graph = Workflow.create(
                _chainOf(_task(1), _task(2))
                        .andThenTasks(_task(3), _task(4))
                        .getContents()
        );
        assertThat(graph.keyedNodes()).isEmpty();

        // Two segments, two nodes per segment, head key only
        graph = Workflow.create(
                _chainOf(_task(1), _task(2))
                        .andThenTasks(_task(3), _task(4))
                        .setHeadKey(5)
                        .getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(5);

        assertThat(graph.keyedNodes().get(5)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependentCount(6);

        // Two segments, two nodes per segment, tail key only
        graph = Workflow.create(
                _chainOf(_task(1), _task(2))
                        .andThenTasks(_task(3), _task(4))
                        .setTailKey(5)
                        .getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(5);

        assertThat(graph.keyedNodes().get(5)).hasEventualDependencyCount(6);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependentCount(0);

        // Two segments, two nodes per segment, keys between segments
        graph = Workflow.create(
                _chainOf(_task(1), _task(2))
                        .setTailKey(3)
                        .andThenTasks(_task(4), _task(5))
                        .setTailKey(6)
                        .setHeadKey(7)
                        .getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(3, 6, 7);

        assertThat(graph.keyedNodes().get(7)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependentCount(6);
        assertThat(graph.keyedNodes().get(3)).hasEventualDependencyCount(3);
        assertThat(graph.keyedNodes().get(3)).hasEventualDependentCount(3);
        assertThat(graph.keyedNodes().get(6)).hasEventualDependencyCount(6);
        assertThat(graph.keyedNodes().get(6)).hasEventualDependentCount(0);

        assertThat(graph.keyedNodes().get(3)).hasEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(6)).hasEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(6)).hasEventualDependency(graph.keyedNodes().get(3));

        // Nested chains, no keys
        graph = Workflow.create(
                BuilderChain.ofChains(
                        _chainOf(_task(1)).andThenTasks(_task(2)),
                        _chainOf(_task(3)).andThenTasks(_task(4))
                ).getContents()
        );
        assertThat(graph.keyedNodes()).isEmpty();

        // Nested chains, head/tail keys for inner chains
        graph = Workflow.create(
                BuilderChain.ofChains(
                        _chainOf(_task(1))
                                .andThenTasks(_task(2))
                                .setHeadKey(3)
                                .setTailKey(4),
                        _chainOf(_task(5))
                                .andThenTasks(_task(6))
                                .setHeadKey(7)
                                .setTailKey(8)
                ).getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(3, 4, 7, 8);

        assertThat(graph.keyedNodes().get(3)).hasEventualDependencyCount(1);
        assertThat(graph.keyedNodes().get(3)).hasEventualDependentCount(5);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependencyCount(5);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependentCount(1);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependencyCount(1);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependentCount(5);
        assertThat(graph.keyedNodes().get(8)).hasEventualDependencyCount(5);
        assertThat(graph.keyedNodes().get(8)).hasEventualDependentCount(1);

        assertThat(graph.keyedNodes().get(4)).hasEventualDependency(graph.keyedNodes().get(3));
        assertThat(graph.keyedNodes().get(8)).hasEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(4)).doesNotHaveEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(8)).doesNotHaveEventualDependency(graph.keyedNodes().get(3));

        // Nested chains, head/tail key for outer chain
        graph = Workflow.create(
                BuilderChain.ofChains(
                        _chainOf(_task(1)).andThenTasks(_task(2)),
                        _chainOf(_task(3)).andThenTasks(_task(4))
                ).setHeadKey(5).setTailKey(6).getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(5, 6);

        assertThat(graph.keyedNodes().get(5)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependentCount(11);
        assertThat(graph.keyedNodes().get(6)).hasEventualDependencyCount(11);
        assertThat(graph.keyedNodes().get(6)).hasEventualDependentCount(0);

        assertThat(graph.keyedNodes().get(6)).hasEventualDependency(graph.keyedNodes().get(5));

        // Nested chains, head/tail key for all chains
        graph = Workflow.create(
                BuilderChain.ofChains(
                        _chainOf(_task(1))
                                .andThenTasks(_task(2))
                                .setHeadKey(3)
                                .setTailKey(4),
                        _chainOf(_task(5))
                                .andThenTasks(_task(6))
                                .setHeadKey(7)
                                .setTailKey(8)
                ).setHeadKey(9).setTailKey(0).getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(3, 4, 7, 8, 9, 0);

        assertThat(graph.keyedNodes().get(3)).hasEventualDependencyCount(1);
        assertThat(graph.keyedNodes().get(3)).hasEventualDependentCount(5);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependencyCount(5);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependentCount(1);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependencyCount(1);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependentCount(5);
        assertThat(graph.keyedNodes().get(8)).hasEventualDependencyCount(5);
        assertThat(graph.keyedNodes().get(8)).hasEventualDependentCount(1);
        assertThat(graph.keyedNodes().get(9)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(9)).hasEventualDependentCount(11);
        assertThat(graph.keyedNodes().get(0)).hasEventualDependencyCount(11);
        assertThat(graph.keyedNodes().get(0)).hasEventualDependentCount(0);

        assertThat(graph.keyedNodes().get(4)).hasEventualDependency(graph.keyedNodes().get(3));
        assertThat(graph.keyedNodes().get(4)).hasEventualDependency(graph.keyedNodes().get(9));
        assertThat(graph.keyedNodes().get(0)).hasEventualDependency(graph.keyedNodes().get(4));
        assertThat(graph.keyedNodes().get(8)).hasEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(8)).hasEventualDependency(graph.keyedNodes().get(9));
        assertThat(graph.keyedNodes().get(0)).hasEventualDependency(graph.keyedNodes().get(8));
        assertThat(graph.keyedNodes().get(0)).hasEventualDependency(graph.keyedNodes().get(9));
        assertThat(graph.keyedNodes().get(4)).doesNotHaveEventualDependency(graph.keyedNodes().get(7));
        assertThat(graph.keyedNodes().get(8)).doesNotHaveEventualDependency(graph.keyedNodes().get(3));

        // Keys associated with non-structural nodes
        graph = Workflow.create(
                BuilderChain.ofChains(
                        BuilderChain.of(TaskNode.builder(1, _task(1))),
                        BuilderChain.of(TaskNode.builder(2, _task(2)))
                                .andThenTasks(_task(3))
                                .andThen(TaskNode.builder(4, _task(4)))
                ).setTailKey(5).getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(1, 2, 4, 5);

        assertThat(graph.keyedNodes().get(1)).hasEventualDependencyCount(2);
        assertThat(graph.keyedNodes().get(1)).hasEventualDependentCount(2);
        assertThat(graph.keyedNodes().get(2)).hasEventualDependencyCount(2);
        assertThat(graph.keyedNodes().get(2)).hasEventualDependentCount(6);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependencyCount(6);
        assertThat(graph.keyedNodes().get(4)).hasEventualDependentCount(2);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependencyCount(11);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependentCount(0);

        assertThat(graph.keyedNodes().get(4)).hasEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(5)).hasEventualDependency(graph.keyedNodes().get(1));
        assertThat(graph.keyedNodes().get(5)).hasEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(5)).hasEventualDependency(graph.keyedNodes().get(4));
        assertThat(graph.keyedNodes().get(2)).doesNotHaveEventualDependency(graph.keyedNodes().get(1));
        assertThat(graph.keyedNodes().get(1)).doesNotHaveEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(4)).doesNotHaveEventualDependency(graph.keyedNodes().get(1));
        assertThat(graph.keyedNodes().get(1)).doesNotHaveEventualDependency(graph.keyedNodes().get(4));

        // Hanging chains off the side of a main chain
        BuilderChain<Integer, Task> main = _chainOf(_task(1))
                .setHeadKey(2)
                .andThenTasks(_task(3));
        BuilderChain<Integer, Task> sub2 = _chainOf(_task(4)).setHeadKey(5);
        BuilderChain<Integer, Task> sub3 = _chainOf(_task(6)).setTailKey(7);

        main.getTail().addDependencies(sub2.getTail());
        main.getContents().addAll(sub2.getContents());

        sub3.getHead().addDependencies(main.getTail());
        main.getContents().addAll(sub3.getContents());

        graph = Workflow.create(
                main.andThenTasks(_task(8))
                        .setTailKey(9)
                        .getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(2, 5, 7, 9);

        assertThat(graph.keyedNodes().get(2)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(2)).hasEventualDependentCount(9);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependencyCount(0);
        assertThat(graph.keyedNodes().get(5)).hasEventualDependentCount(8);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependencyCount(10);
        assertThat(graph.keyedNodes().get(7)).hasEventualDependentCount(0);
        assertThat(graph.keyedNodes().get(9)).hasEventualDependencyCount(9);
        assertThat(graph.keyedNodes().get(9)).hasEventualDependentCount(0);

        assertThat(graph.keyedNodes().get(9)).hasEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(9)).hasEventualDependency(graph.keyedNodes().get(5));
        assertThat(graph.keyedNodes().get(7)).hasEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(7)).hasEventualDependency(graph.keyedNodes().get(5));
        assertThat(graph.keyedNodes().get(2)).doesNotHaveEventualDependency(graph.keyedNodes().get(5));
        assertThat(graph.keyedNodes().get(5)).doesNotHaveEventualDependency(graph.keyedNodes().get(2));
        assertThat(graph.keyedNodes().get(7)).doesNotHaveEventualDependency(graph.keyedNodes().get(9));
        assertThat(graph.keyedNodes().get(9)).doesNotHaveEventualDependency(graph.keyedNodes().get(7));

        // Empty segment
        graph = Workflow.create(
                BuilderChain.<Integer, Task>ofTasks(ImmutableSet.of())
                        .setHeadKey(1)
                        .setTailKey(2)
                        .getContents()
        );
        assertThat(graph.keyedNodes().keySet()).containsExactly(1, 2);
        assertThat(graph.keyedNodes().get(2)).hasEventualDependency(graph.keyedNodes().get(1));
    }

    @Test
    public void testGetContents()
    {
        BuilderChain<Void, Task> chain = BuilderChain.ofTasks(_task(1));
        Set<WorkflowNode.Builder<Void, Task>> contents1 = chain.getContents();
        Set<WorkflowNode.Builder<Void, Task>> contents2 = chain.getContents();
        assertThat(contents1).hasSize(3);
        assertThat(contents2).hasSize(3);

        WorkflowNode.Builder<Void, Task> builder1 = TaskNode.builder(_task(2));
        WorkflowNode.Builder<Void, Task> builder2 = TaskNode.builder(_task(3));
        Set<WorkflowNode.Builder<Void, Task>> empty = ImmutableSet.of();

        // Changes to one set should be reflected in the other
        contents1.add(builder1);
        assertThat(contents1).hasSize(4);
        assertThat(contents2).hasSize(4);

        // The head and tail should be included in the sets
        assertThat(contents1).contains(chain.getHead());
        assertThat(contents1).contains(chain.getTail());
        assertThat(contents1.add(chain.getHead())).isFalse();
        assertThat(contents1.add(chain.getTail())).isFalse();
        assertThat(contents1).hasSize(4);
        assertThat(contents2).hasSize(4);

        // Attempting to remove elements from a set should throw an exception
        _assertUnsupportedOperation(() -> contents1.remove(builder1));
        _assertUnsupportedOperation(() -> contents1.remove(builder2));
        _assertUnsupportedOperation(() -> contents1.removeAll(contents2));
        _assertUnsupportedOperation(() -> contents1.removeAll(empty));
        _assertUnsupportedOperation(() -> contents1.retainAll(contents2));
        _assertUnsupportedOperation(() -> contents1.retainAll(empty));
        _assertUnsupportedOperation(() -> contents1.removeIf(x -> true));
        _assertUnsupportedOperation(() -> contents1.removeIf(x -> false));

        Iterator<WorkflowNode.Builder<Void, Task>> iter = contents1.iterator();
        iter.next();
        _assertUnsupportedOperation(iter::remove);
    }

    private void _assertUnsupportedOperation(Runnable runnable)
    {
        try
        {
            runnable.run();
            fail("Operation should have thrown an exception");
        }
        catch (UnsupportedOperationException e)
        {
            // Return normally
        }
    }
}
