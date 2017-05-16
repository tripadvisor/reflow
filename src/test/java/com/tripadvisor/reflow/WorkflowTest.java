package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.tripadvisor.reflow.TaskNode.Builder;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import static com.google.common.truth.Truth.assertThat;
import static org.testng.Assert.fail;

public final class WorkflowTest
{
    private static final String TEMPLATE_EMPTY = "Input collection is empty";
    private static final String TEMPLATE_CONTAINS_DUPLICATE = "Input collection contains repeated elements";
    private static final String TEMPLATE_INCOMPLETE = "Input collection is incomplete";
    private static final String GRAPH_CONTAINS_CYCLE = "Input graph contains a cycle";

    @DataProvider
    public Object[][] testBuildGraphDataSet()
    {
        BuilderAssembler<Task> graphBuilder = new BuilderAssembler<>(NoOpTask::new);

        List<Object[]> dataSet = new ArrayList<>();
        List<Builder<Integer, Task>> b;

        // Empty graph
        dataSet.add(new Object[] { ImmutableList.of(), IllegalStateException.class, TEMPLATE_EMPTY });

        // Null dependency set
        b = graphBuilder.builderList(1);
        b.get(0).setDependencies(null);
        dataSet.add(new Object[] { b, null, null });

        // Null user object
        b = graphBuilder.builderList(1);
        b.get(0).setTask(null);
        dataSet.add(new Object[] { b, NullPointerException.class, null });

        // Single-node graph with no edges
        b = graphBuilder.builderList(1);
        b.get(0).setDependencies(ImmutableSet.of());
        dataSet.add(new Object[] { b, null, null });

        // Single-node graph with a loop
        b = graphBuilder.builderList(1);
        b.get(0).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, IllegalStateException.class, GRAPH_CONTAINS_CYCLE });

        // Two-node graph with no edges
        b = graphBuilder.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).setDependencies(ImmutableSet.of());
        dataSet.add(new Object[] { b, null, null });

        // Two-node graph with one edge
        b = graphBuilder.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, null, null });

        // Two-node graph with two edges
        b = graphBuilder.builderList(2);
        b.get(0).addDependencies(b.get(1));
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, IllegalStateException.class, GRAPH_CONTAINS_CYCLE });

        // Builder list with a duplicate element
        b = graphBuilder.builderList(1);
        dataSet.add(new Object[] {
                ImmutableList.of(b.get(0), b.get(0)), IllegalStateException.class, TEMPLATE_CONTAINS_DUPLICATE
        });

        // Incomplete builder list
        b = graphBuilder.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { ImmutableList.of(b.get(1)), IllegalStateException.class, TEMPLATE_INCOMPLETE });

        // Three-node graph with a cycle, but also a node with no dependencies
        b = graphBuilder.builderList(3);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0), b.get(2));
        b.get(2).addDependencies(b.get(1));
        dataSet.add(new Object[] { b, IllegalStateException.class, GRAPH_CONTAINS_CYCLE });

        // 0-1-2-3-4
        //    \ /
        //   5-6-7
        b = graphBuilder.builderListTestConfig2();
        dataSet.add(new Object[] { b, null, null });

        return dataSet.toArray(new Object[dataSet.size()][]);
    }

    @Test(dataProvider = "testBuildGraphDataSet")
    public void testBuildGraph(List<Builder<Integer, Task>> template,
                               @Nullable Class<? extends Exception> expectedException,
                               @Nullable String expectedMessagePrefix)
    {
        // Build a map of the expected dependent builders
        Map<WorkflowNode.Builder<Integer, Task>, Set<Builder<Integer, Task>>> dependentMap =
                new HashMap<>(template.size());

        for (Builder<Integer, Task> builder : template)
        {
            if (builder.getDependencies() == null)
            {
                continue;
            }
            for (WorkflowNode.Builder<Integer, Task> dependency : builder.getDependencies())
            {
                dependentMap.computeIfAbsent(dependency, key -> new HashSet<>()).add(builder);
            }
        }

        // Build the workflow itself
        Workflow<Integer, Task> graph;
        try
        {
            graph = Workflow.create(template);
            if (expectedException != null || expectedMessagePrefix != null)
            {
                fail("Workflow creation should have thrown an exception");
            }
        }
        catch (Exception e)
        {
            if (expectedException == null && expectedMessagePrefix == null)
            {
                throw e;
            }
            if (expectedException != null)
            {
                assertThat(e).isInstanceOf(expectedException);
            }
            if (expectedMessagePrefix != null)
            {
                assertThat(e).hasMessageThat().startsWith(expectedMessagePrefix);
            }
            return;
        }

        // Verify that the returned workflow matches what we passed in
        assertThat(graph.getNodes()).hasSize(template.size());
        assertThat(graph.keyedNodes().values()).containsExactlyElementsIn(graph.getNodes());

        Map<Builder<Integer, Task>, WorkflowNode<Task>> builderNodeMap = IntStream.range(0, template.size())
                .boxed()
                .collect(toMap(template::get, graph.keyedNodes()::get));

        builderNodeMap.forEach((builder, node) ->
        {
            assertThat(node.getTask()).isSameAs(builder.getTask());

            Set<WorkflowNode<Task>> expectedDependencies = builder.getDependencies().stream()
                    .map(builderNodeMap::get)
                    .collect(toSet());
            Set<WorkflowNode<Task>> expectedDependents = dependentMap.getOrDefault(builder, ImmutableSet.of()).stream()
                    .map(builderNodeMap::get)
                    .collect(toSet());

            assertThat(node.getDependencies()).containsExactlyElementsIn(expectedDependencies);
            assertThat(node.getDependents()).containsExactlyElementsIn(expectedDependents);
        });
    }
}
