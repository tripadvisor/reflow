package com.tripadvisor.reflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;

import com.google.common.collect.ImmutableSetMultimap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.toSet;

import static com.google.common.collect.ImmutableSetMultimap.flatteningToImmutableSetMultimap;
import static com.google.common.truth.Truth.assertThat;

public class SerializationTest
{
    @DataProvider
    public Object[][] testSerializeStructureNodeDataSet()
    {
        BuilderAssembler<Task, StructureNode.Builder<Task>> builderAssembler = BuilderAssembler.withoutTasks();
        Workflow<Task> workflow = Workflow.create(builderAssembler.builderListTestConfig1());
        Iterator<WorkflowNode<Task>> iter = workflow.getNodes().values().iterator();
        return new Object[][] {
                new Object[] { iter.next() },
                new Object[] { iter.next() },
                new Object[] { iter.next() },
        };
    }

    @Test(dataProvider = "testSerializeStructureNodeDataSet")
    public <T extends Task> void testSerializeStructureNode(StructureNode<T> structureNode)
    {
        compareWorkflowNodes(reconstitute(structureNode), structureNode);
    }

    @DataProvider
    public Object[][] testSerializeTaskNodeDataSet()
    {
        BuilderAssembler<Task, TaskNode.Builder<Task>> builderAssembler = BuilderAssembler.usingTasks(NoOpTask::new);
        Workflow<Task> workflow = Workflow.create(builderAssembler.builderListTestConfig1());
        Iterator<WorkflowNode<Task>> iter = workflow.getNodes().values().iterator();
        return new Object[][] {
                new Object[] { iter.next() },
                new Object[] { iter.next() },
                new Object[] { iter.next() },
        };
    }

    @Test(dataProvider = "testSerializeTaskNodeDataSet")
    public <T extends Task> void testSerializeTaskNode(TaskNode<T> taskNode)
    {
        compareWorkflowNodes(reconstitute(taskNode), taskNode);
    }

    @DataProvider
    public Object[][] testSerializeWorkflowDataSet()
    {
        BuilderAssembler<Task, TaskNode.Builder<Task>> builderAssembler = BuilderAssembler.usingTasks(NoOpTask::new);
        Workflow<Task> workflow = Workflow.create(builderAssembler.builderListTestConfig1());
        return new Object[][] { new Object[] { workflow } };
    }

    @Test(dataProvider = "testSerializeWorkflowDataSet")
    public <T extends Task> void testSerializeWorkflow(Workflow<T> workflow)
    {
        compareWorkflows(reconstitute(workflow), workflow);
    }

    @DataProvider
    public Object[][] testSerializeWorkflowSubsetDataSet()
    {
        BuilderAssembler<Task, TaskNode.Builder<Task>> builderAssembler = BuilderAssembler.usingTasks(NoOpTask::new);
        Workflow<Task> workflow = Workflow.create(builderAssembler.builderListTestConfig2());

        // 0-1-2-3-4
        //    \ /
        //   5-6-7
        return new Object[][] { new Object[] { workflow.stoppingAfterKeys("3") } };
    }

    @Test(dataProvider = "testSerializeWorkflowSubsetDataSet")
    public <T extends Task> void testSerializeWorkflowSubset(WorkflowSubset<T> workflowSubset)
    {
        compareWorkflowSubsets(reconstitute(workflowSubset), workflowSubset);
    }

    private <T extends Task> void compareWorkflowNodes(WorkflowNode<T> serialized, WorkflowNode<T> original)
    {
        assertThat(serialized.getKey()).isEqualTo(original.getKey());
        assertThat(serialized.hasTask()).isEqualTo(original.hasTask());
        assertThat(serialized.getDependencies()).hasSize(original.getDependencies().size());
        assertThat(serialized.getDependents()).isNull();
    }

    private <T extends Task> void compareWorkflows(Workflow<T> serialized, Workflow<T> original)
    {
        assertThat(serialized.getNodes().keySet()).containsExactlyElementsIn(original.getNodes().keySet());

        ImmutableSetMultimap<String, String> dependencyMap = original.getNodes().values().stream()
                .collect(flatteningToImmutableSetMultimap(
                        WorkflowNode::getKey, node -> node.getDependencies().stream().map(WorkflowNode::getKey)
                ));
        ImmutableSetMultimap<String, String> dependentMap = dependencyMap.inverse();

        serialized.getNodes().forEach((key, node) -> assertThat(
                node.getDependencies().stream()
                        .map(WorkflowNode::getKey)
                        .collect(toSet())
        ).containsExactlyElementsIn(dependencyMap.get(key)));

        serialized.getNodes().forEach((key, node) -> assertThat(
                node.getDependents().stream()
                        .map(WorkflowNode::getKey)
                        .collect(toSet())
        ).containsExactlyElementsIn(dependentMap.get(key)));
    }

    private <T extends Task> void compareWorkflowSubsets(WorkflowSubset<T> serialized, WorkflowSubset<T> original)
    {
        assertThat(serialized.getNodes().keySet()).containsExactlyElementsIn(original.getNodes().keySet());
        serialized.getNodes().forEach((key, node) -> assertThat(node.getKey()).isEqualTo(key));
        compareWorkflows(serialized.getWorkflow(), original.getWorkflow());
    }

    private <T extends Serializable> T reconstitute(T object)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(baos))
        {
            oos.writeObject(object);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())))
        {
            return (T) ois.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }
}
