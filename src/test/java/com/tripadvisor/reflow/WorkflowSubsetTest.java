package com.tripadvisor.reflow;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import static java.util.stream.Collectors.toMap;

import static com.google.common.truth.Truth.assertThat;

public class WorkflowSubsetTest
{
    // We're using the same old eight-node graph for these tests:
    //
    // 0-1-2-3-4
    //    \ /
    //   5-6-7

    @Test
    public void testSubsetBeginningAt()
    {
        Workflow<Task> workflow = Workflow.create(BuilderAssembler.withoutTasks().builderListTestConfig2());
        Target<Task> subset = workflow.startingFromKeys("1");
        assertThat(subset.getNodes().keySet()).containsExactly("1", "2", "3", "4", "6", "7");
    }

    @Test
    public void testSubsetEndingAt()
    {
        Workflow<Task> workflow = Workflow.create(BuilderAssembler.withoutTasks().builderListTestConfig2());
        Target<Task> subset = workflow.stoppingAfterKeys("6");
        assertThat(subset.getNodes().keySet()).containsExactly("0", "1", "5", "6");
    }

    @Test
    public void testSubsetWithTwoBounds()
    {
        Workflow<Task> workflow = Workflow.create(BuilderAssembler.withoutTasks().builderListTestConfig2());
        Target<Task> subset = workflow.startingFromKeys("0").stoppingAfterKeys("7");
        assertThat(subset.getNodes().keySet()).containsExactly("0", "1", "6", "7");
    }

    @Test
    public void testSubsetOfDiscontinuousTarget()
    {
        Workflow<Task> workflow = Workflow.create(BuilderAssembler.withoutTasks().builderListTestConfig2());

        // We can't build a discontinuous target using WorkflowSubset, so we need a custom target.
        // This checks that we don't consider nodes outside of the target when building a subset:
        // An incorrect implementation might follow all dependency references and only filter nodes at the end

        Map<String, WorkflowNode<Task>> discontinuousNodes = Stream.of("5", "7").collect(toMap(
                Function.identity(), workflow.getNodes()::get
        ));

        Target<Task> discontinuousTarget = new Target<Task>()
        {
            @Override
            Workflow<Task> getWorkflow()
            {
                return workflow;
            }

            @Override
            public Map<String, WorkflowNode<Task>> getNodes()
            {
                return discontinuousNodes;
            }

            @Override
            boolean containsNode(WorkflowNode<Task> node)
            {
                return workflow.containsNode(node) && discontinuousNodes.keySet().contains(node.getKey());
            }
        };

        Target<Task> subset = discontinuousTarget.startingFromKeys("5");
        assertThat(subset.getNodes().keySet()).containsExactly("5");
    }
}
