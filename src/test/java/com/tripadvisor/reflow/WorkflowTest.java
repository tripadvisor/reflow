package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
    private static final String TEMPLATE_INCOMPLETE = "Input collection is incomplete";
    private static final String GRAPH_CONTAINS_CYCLE = "Input graph contains a cycle";

    @DataProvider
    public Object[][] testBuildGraphDataSet()
    {
        BuilderAssembler<Task, Builder<Task>> builderAssembler = BuilderAssembler.usingTasks(NoOpTask::new);

        List<Object[]> dataSet = new ArrayList<>();
        List<Builder<Task>> b;

        // Empty graph
        dataSet.add(new Object[] { ImmutableList.of(), IllegalArgumentException.class, TEMPLATE_EMPTY });

        // Null dependency set
        b = builderAssembler.builderList(1);
        b.get(0).setDependencies(null);
        dataSet.add(new Object[] { b, null, null });

        // Null task
        b = builderAssembler.builderList(1);
        b.get(0).setTask(null);
        dataSet.add(new Object[] { b, NullPointerException.class, null });

        // Single-node graph with no edges
        b = builderAssembler.builderList(1);
        b.get(0).setDependencies(ImmutableSet.of());
        dataSet.add(new Object[] { b, null, null });

        // Single-node graph with a loop
        b = builderAssembler.builderList(1);
        b.get(0).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, IllegalArgumentException.class, GRAPH_CONTAINS_CYCLE });

        // Two-node graph with no edges
        b = builderAssembler.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).setDependencies(ImmutableSet.of());
        dataSet.add(new Object[] { b, null, null });

        // Two-node graph with one edge
        b = builderAssembler.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, null, null });

        // Two-node graph with two edges
        b = builderAssembler.builderList(2);
        b.get(0).addDependencies(b.get(1));
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { b, IllegalArgumentException.class, GRAPH_CONTAINS_CYCLE });

        // Builder list with a duplicate element
        b = builderAssembler.builderList(1);
        dataSet.add(new Object[] { ImmutableList.of(b.get(0), b.get(0)), IllegalArgumentException.class, null });

        // Builder list with a duplicate key
        b = builderAssembler.builderList(2);
        b.get(1).setKey(b.get(0).getKey());
        dataSet.add(new Object[] { b, IllegalArgumentException.class, null });

        // Incomplete builder list
        b = builderAssembler.builderList(2);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0));
        dataSet.add(new Object[] { ImmutableList.of(b.get(1)), IllegalArgumentException.class, TEMPLATE_INCOMPLETE });

        // Three-node graph with a cycle, but also a node with no dependencies
        b = builderAssembler.builderList(3);
        b.get(0).setDependencies(ImmutableSet.of());
        b.get(1).addDependencies(b.get(0), b.get(2));
        b.get(2).addDependencies(b.get(1));
        dataSet.add(new Object[] { b, IllegalArgumentException.class, GRAPH_CONTAINS_CYCLE });

        // 0-1-2-3-4
        //    \ /
        //   5-6-7
        b = builderAssembler.builderListTestConfig2();
        dataSet.add(new Object[] { b, null, null });

        return dataSet.toArray(new Object[dataSet.size()][]);
    }

    @Test(dataProvider = "testBuildGraphDataSet")
    public void testBuildGraph(List<Builder<Task>> template,
                               @Nullable Class<? extends Exception> expectedException,
                               @Nullable String expectedMessagePrefix)
    {
        // Build a map of the expected dependent builders
        Map<WorkflowNode.Builder<Task>, Set<Builder<Task>>> dependentMap =
                Maps.newHashMapWithExpectedSize(template.size());

        for (Builder<Task> builder : template)
        {
            if (builder.getDependencies() == null)
            {
                continue;
            }
            for (WorkflowNode.Builder<Task> dependency : builder.getDependencies())
            {
                dependentMap.computeIfAbsent(dependency, key -> new HashSet<>()).add(builder);
            }
        }

        // Build the workflow itself
        Workflow<Task> graph;
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

        Map<Builder<Task>, WorkflowNode<Task>> builderNodeMap = template.stream().collect(toMap(
                Function.identity(), builder -> graph.getNodes().get(builder.getKey())
        ));

        builderNodeMap.forEach((builder, node) ->
        {
            assertThat(node.getKey()).isEqualTo(builder.getKey());
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

    @DataProvider
    public Object[][] testValidKeyDataSet()
    {
        return new Object[][] {
                new Object[] { null },
                new Object[] { "aZ-_1" },
        };
    }

    @DataProvider
    public Object[][] testInvalidKeyDataSet()
    {
        return new Object[][] {
                new Object[] { "" },
                new Object[] { Strings.repeat("a", 257) },
                new Object[] { "no good" },
        };
    }

    @Test(dataProvider = "testValidKeyDataSet")
    public void testValidKey(String key)
    {
        Workflow<NoOpTask> workflow = Workflow.create(ImmutableList.of(TaskNode.builder(key, new NoOpTask())));
        workflow.getNodes().values().forEach(node -> assertThat(node.getKey()).isNotNull());
    }

    @Test(dataProvider = "testInvalidKeyDataSet",
          expectedExceptions = IllegalArgumentException.class,
          expectedExceptionsMessageRegExp = "^Key must match pattern " +
                  "\\[a-zA-Z0-9\\]\\(\\?:\\[a-zA-Z0-9_-\\]\\{0,254\\}\\[a-zA-Z0-9\\]\\)\\?$")
    public void testInvalidKey(String key)
    {
        Workflow.create(ImmutableList.of(TaskNode.builder(key, new NoOpTask())));
    }
}
