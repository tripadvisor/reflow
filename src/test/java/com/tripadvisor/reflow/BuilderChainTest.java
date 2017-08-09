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
    @Test
    public void testBuild()
    {
        Workflow<NoOpTask> graph;

        // Two segments, two nodes per segment, head key only
        graph = Workflow.create(
                BuilderChain.ofTasks(new NoOpTask(), new NoOpTask())
                        .andThenTasks(new NoOpTask(), new NoOpTask())
                        .setHeadKey("A")
                        .getContents()
        );
        assertThat(graph.getNodes().keySet()).contains("A");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(6);

        // Two segments, two nodes per segment, tail key only
        graph = Workflow.create(
                BuilderChain.ofTasks(new NoOpTask(), new NoOpTask())
                        .andThenTasks(new NoOpTask(), new NoOpTask())
                        .setTailKey("A")
                        .getContents()
        );
        assertThat(graph.getNodes().keySet()).contains("A");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(6);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(0);

        // Two segments, two nodes per segment, keys between segments
        graph = Workflow.create(
                BuilderChain.ofTasks(new NoOpTask(), new NoOpTask())
                        .setTailKey("A")
                        .andThenTasks(new NoOpTask(), new NoOpTask())
                        .setTailKey("B")
                        .setHeadKey("C")
                        .getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B", "C");

        assertThat(graph.getNodes().get("C")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("C")).hasEventualDependentCount(6);
        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(3);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(3);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(6);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(0);

        assertThat(graph.getNodes().get("A")).hasEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("A"));

        // Nested chains, head/tail keys for inner chains
        graph = Workflow.create(
                BuilderChain.ofChains(
                        BuilderChain.ofTasks(new NoOpTask())
                                .andThenTasks(new NoOpTask())
                                .setHeadKey("A")
                                .setTailKey("B"),
                        BuilderChain.ofTasks(new NoOpTask())
                                .andThenTasks(new NoOpTask())
                                .setHeadKey("C")
                                .setTailKey("D")
                ).getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B", "C", "D");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(1);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(5);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(5);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(1);
        assertThat(graph.getNodes().get("C")).hasEventualDependencyCount(1);
        assertThat(graph.getNodes().get("C")).hasEventualDependentCount(5);
        assertThat(graph.getNodes().get("D")).hasEventualDependencyCount(5);
        assertThat(graph.getNodes().get("D")).hasEventualDependentCount(1);

        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("B")).doesNotHaveEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("D")).doesNotHaveEventualDependency(graph.getNodes().get("A"));

        // Nested chains, head/tail key for outer chain
        graph = Workflow.create(
                BuilderChain.ofChains(
                        BuilderChain.ofTasks(new NoOpTask()).andThenTasks(new NoOpTask()),
                        BuilderChain.ofTasks(new NoOpTask()).andThenTasks(new NoOpTask())
                ).setHeadKey("A").setTailKey("B").getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(11);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(11);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(0);

        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("A"));

        // Nested chains, head/tail key for all chains
        graph = Workflow.create(
                BuilderChain.ofChains(
                        BuilderChain.ofTasks(new NoOpTask())
                                .andThenTasks(new NoOpTask())
                                .setHeadKey("A")
                                .setTailKey("B"),
                        BuilderChain.ofTasks(new NoOpTask())
                                .andThenTasks(new NoOpTask())
                                .setHeadKey("C")
                                .setTailKey("D")
                ).setHeadKey("E").setTailKey("F").getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B", "C", "D", "E", "F");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(1);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(5);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(5);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(1);
        assertThat(graph.getNodes().get("C")).hasEventualDependencyCount(1);
        assertThat(graph.getNodes().get("C")).hasEventualDependentCount(5);
        assertThat(graph.getNodes().get("D")).hasEventualDependencyCount(5);
        assertThat(graph.getNodes().get("D")).hasEventualDependentCount(1);
        assertThat(graph.getNodes().get("E")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("E")).hasEventualDependentCount(11);
        assertThat(graph.getNodes().get("F")).hasEventualDependencyCount(11);
        assertThat(graph.getNodes().get("F")).hasEventualDependentCount(0);

        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("B")).hasEventualDependency(graph.getNodes().get("E"));
        assertThat(graph.getNodes().get("F")).hasEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("E"));
        assertThat(graph.getNodes().get("F")).hasEventualDependency(graph.getNodes().get("D"));
        assertThat(graph.getNodes().get("F")).hasEventualDependency(graph.getNodes().get("E"));
        assertThat(graph.getNodes().get("B")).doesNotHaveEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("D")).doesNotHaveEventualDependency(graph.getNodes().get("A"));

        // Keys associated with non-structural nodes
        graph = Workflow.create(
                BuilderChain.ofChains(
                        BuilderChain.of(TaskNode.builder("A", new NoOpTask())),
                        BuilderChain.of(TaskNode.builder("B", new NoOpTask()))
                                .andThenTasks(new NoOpTask())
                                .andThen(TaskNode.builder("C", new NoOpTask()))
                ).setTailKey("D").getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B", "C", "D");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(2);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(2);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(2);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(6);
        assertThat(graph.getNodes().get("C")).hasEventualDependencyCount(6);
        assertThat(graph.getNodes().get("C")).hasEventualDependentCount(2);
        assertThat(graph.getNodes().get("D")).hasEventualDependencyCount(11);
        assertThat(graph.getNodes().get("D")).hasEventualDependentCount(0);

        assertThat(graph.getNodes().get("C")).hasEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("C"));
        assertThat(graph.getNodes().get("B")).doesNotHaveEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("A")).doesNotHaveEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("C")).doesNotHaveEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("A")).doesNotHaveEventualDependency(graph.getNodes().get("C"));

        // Hanging chains off the side of a main chain
        BuilderChain<NoOpTask> main = BuilderChain.ofTasks(new NoOpTask())
                .setHeadKey("A")
                .andThenTasks(new NoOpTask());
        BuilderChain<NoOpTask> sub2 = BuilderChain.ofTasks(new NoOpTask()).setHeadKey("B");
        BuilderChain<NoOpTask> sub3 = BuilderChain.ofTasks(new NoOpTask()).setTailKey("C");

        main.getTail().addDependencies(sub2.getTail());
        main.getContents().addAll(sub2.getContents());

        sub3.getHead().addDependencies(main.getTail());
        main.getContents().addAll(sub3.getContents());

        graph = Workflow.create(
                main.andThenTasks(new NoOpTask())
                        .setTailKey("D")
                        .getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("A", "B", "C", "D");

        assertThat(graph.getNodes().get("A")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("A")).hasEventualDependentCount(9);
        assertThat(graph.getNodes().get("B")).hasEventualDependencyCount(0);
        assertThat(graph.getNodes().get("B")).hasEventualDependentCount(8);
        assertThat(graph.getNodes().get("C")).hasEventualDependencyCount(10);
        assertThat(graph.getNodes().get("C")).hasEventualDependentCount(0);
        assertThat(graph.getNodes().get("D")).hasEventualDependencyCount(9);
        assertThat(graph.getNodes().get("D")).hasEventualDependentCount(0);

        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("D")).hasEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("C")).hasEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("C")).hasEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("A")).doesNotHaveEventualDependency(graph.getNodes().get("B"));
        assertThat(graph.getNodes().get("B")).doesNotHaveEventualDependency(graph.getNodes().get("A"));
        assertThat(graph.getNodes().get("C")).doesNotHaveEventualDependency(graph.getNodes().get("D"));
        assertThat(graph.getNodes().get("D")).doesNotHaveEventualDependency(graph.getNodes().get("C"));

        // Empty segment
        graph = Workflow.create(
                BuilderChain.<NoOpTask>ofTasks(ImmutableSet.of())
                        .setHeadKey("1")
                        .setTailKey("A")
                        .getContents()
        );
        assertThat(graph.getNodes().keySet()).containsAllOf("1", "A");
        assertThat(graph.getNodes().get("A")).hasEventualDependency(graph.getNodes().get("1"));
    }

    @Test
    public void testGetContents()
    {
        BuilderChain<Task> chain = BuilderChain.ofTasks(new NoOpTask());
        Set<WorkflowNode.Builder<Task>> contents1 = chain.getContents();
        Set<WorkflowNode.Builder<Task>> contents2 = chain.getContents();
        assertThat(contents1).hasSize(3);
        assertThat(contents2).hasSize(3);

        WorkflowNode.Builder<Task> builder1 = TaskNode.builder(new NoOpTask());
        WorkflowNode.Builder<Task> builder2 = TaskNode.builder(new NoOpTask());
        Set<WorkflowNode.Builder<Task>> empty = ImmutableSet.of();

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

        Iterator<WorkflowNode.Builder<Task>> iter = contents1.iterator();
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
