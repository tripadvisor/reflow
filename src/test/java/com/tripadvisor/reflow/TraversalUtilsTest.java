/*
 * Copyright (C) 2017 TripAdvisor LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tripadvisor.reflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.toMap;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.google.common.truth.Truth8.assertThat;

public final class TraversalUtilsTest
{
    @DataProvider
    public Object[][] testCollectNodesDataSet()
    {
        BuilderAssembler<Task, StructureNode.Builder<Task>> builderAssembler = BuilderAssembler.withoutTasks();

        List<Object[]> dataSet = new ArrayList<>();
        Workflow<Task> workflow;

        Function<WorkflowNode<Task>, Set<WorkflowNode<Task>>> getDependencies = WorkflowNode::getDependencies;
        Function<WorkflowNode<Task>, Set<WorkflowNode<Task>>> getDependents = WorkflowNode::getDependents;
        Function<WorkflowNode<Task>, Set<WorkflowNode<Task>>> noNeighbors = node -> ImmutableSet.of();
        Function<WorkflowNode<Task>, Set<WorkflowNode<Task>>> allNeighbors = node -> Sets.union(node.getDependencies(),
                                                                                                node.getDependents());

        // Empty graph
        dataSet.add(new Object[] {
                ImmutableSet.of(),
                getDependencies,
                ImmutableSet.of()
        });

        // Single-node graph
        workflow = Workflow.create(builderAssembler.builderList(1));
        dataSet.add(new Object[] {
                ImmutableSet.copyOf(workflow.getNodes().values()),
                getDependencies,
                ImmutableSet.copyOf(workflow.getNodes().values())
        });

        // Three nodes in series
        workflow = Workflow.create(builderAssembler.builderListTestConfig1());
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("1")),
                getDependencies,
                ImmutableSet.of(workflow.getNodes().get("0"),
                                workflow.getNodes().get("1"))
        });
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("1")),
                getDependents,
                ImmutableSet.of(workflow.getNodes().get("1"),
                                workflow.getNodes().get("2"))
        });
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("1")),
                noNeighbors,
                ImmutableSet.of(workflow.getNodes().get("1"))
        });
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("1")),
                allNeighbors,
                ImmutableSet.copyOf(workflow.getNodes().values())
        });

        // 0-1-2-3-4
        //    \ /
        //   5-6-7
        workflow = Workflow.create(builderAssembler.builderListTestConfig2());
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("7")),
                getDependencies,
                ImmutableSet.of(workflow.getNodes().get("0"),
                                workflow.getNodes().get("1"),
                                workflow.getNodes().get("5"),
                                workflow.getNodes().get("6"),
                                workflow.getNodes().get("7"))
        });
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("3"),
                                workflow.getNodes().get("5")),
                getDependents,
                ImmutableSet.of(workflow.getNodes().get("3"),
                                workflow.getNodes().get("4"),
                                workflow.getNodes().get("5"),
                                workflow.getNodes().get("6"),
                                workflow.getNodes().get("7"))
        });
        dataSet.add(new Object[] {
                ImmutableSet.of(workflow.getNodes().get("6")),
                allNeighbors,
                ImmutableSet.copyOf(workflow.getNodes().values())
        });

        return dataSet.toArray(new Object[dataSet.size()][]);
    }

    @DataProvider
    public Object[][] testTopologicalSortDataSet()
    {
        BuilderAssembler<Task, StructureNode.Builder<Task>> builderAssembler = BuilderAssembler.withoutTasks();

        List<Set<WorkflowNode<Task>>> dataSet = new ArrayList<>();
        Workflow<Task> workflow;

        // Empty graph
        dataSet.add(ImmutableSet.of());

        // Single-node graph
        workflow = Workflow.create(builderAssembler.builderList(1));
        dataSet.add(ImmutableSet.copyOf(workflow.getNodes().values()));

        // Three nodes in series
        workflow = Workflow.create(builderAssembler.builderListTestConfig1());
        dataSet.add(ImmutableSet.copyOf(workflow.getNodes().values()));
        dataSet.add(ImmutableSet.of(workflow.getNodes().get("0"), workflow.getNodes().get("1")));
        dataSet.add(ImmutableSet.of(workflow.getNodes().get("1"), workflow.getNodes().get("2")));

        // In this case, the two nodes can be sorted in any order
        dataSet.add(ImmutableSet.of(workflow.getNodes().get("0"), workflow.getNodes().get("2")));

        // 0-1-2-3-4
        //    \ /
        //   5-6-7
        workflow = Workflow.create(builderAssembler.builderListTestConfig2());
        dataSet.add(ImmutableSet.copyOf(workflow.getNodes().values()));

        return dataSet.stream().map(x -> new Object[] { x }).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testCollectNodesDataSet")
    public void testCollectNodes(Set<WorkflowNode<Task>> startNodes,
                                 Function<WorkflowNode<Task>, Set<WorkflowNode<Task>>> neighborsFunc,
                                 Set<WorkflowNode> expectedResult)
    {
        Set<WorkflowNode<Task>> result1 = TraversalUtils.collectNodes(startNodes, neighborsFunc);

        Set<WorkflowNode<Task>> result2 = TraversalUtils.collectNodes(
                startNodes.iterator(), neighborsFunc.andThen(Set::iterator));

        List<WorkflowNode<Task>> result3 = ImmutableList.copyOf(TraversalUtils.traverseNodes(
                startNodes.iterator(), neighborsFunc.andThen(Set::iterator)));

        assertThat(result1).containsExactlyElementsIn(expectedResult);
        assertThat(result2).containsExactlyElementsIn(expectedResult);
        assertThat(result3).containsExactlyElementsIn(expectedResult);
    }

    @Test(dataProvider = "testTopologicalSortDataSet")
    public void testTopologicalSort(Set<WorkflowNode<Task>> nodes)
    {
        Optional<List<WorkflowNode<Task>>> boxedResult = TraversalUtils.topologicalSort(ImmutableSet.copyOf(nodes));

        // We cover cycle detection in the workflow construction tests
        assertThat(boxedResult).isPresent();

        List<WorkflowNode<Task>> result = boxedResult.get();
        Map<WorkflowNode, Integer> nodeIndexMap = IntStream.range(0, result.size()).boxed().collect(toMap(
                result::get, Function.identity()
        ));

        for (WorkflowNode<Task> node : nodes)
        {
            int nodeIndex = nodeIndexMap.get(node);

            node.getDependencies().stream()
                    .filter(nodes::contains)
                    .forEach(dependency -> assertWithMessage("Dependency %s should appear before %s", dependency, node)
                            .that(nodeIndexMap.get(dependency))
                            .isLessThan(nodeIndex));

            node.getDependents().stream()
                    .filter(nodes::contains)
                    .forEach(dependent -> assertWithMessage("Dependent %s should appear after %s", dependent, node)
                            .that(nodeIndexMap.get(dependent))
                            .isGreaterThan(nodeIndex));
        }
    }
}
