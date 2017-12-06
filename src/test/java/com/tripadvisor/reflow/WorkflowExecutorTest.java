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

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;

import com.tripadvisor.reflow.TaskNode.Builder;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.testng.Assert.fail;

import static com.tripadvisor.reflow.TestTaskSubject.assertThat;

public final class WorkflowExecutorTest
{
    private static final long RUNNABLE_DURATION_SEED = 1798045396195092384L;
    private static final int MAX_RUNNABLE_DURATION_MS = 50;

    private interface ExecutorConsumer
    {
        void accept(Executor executor) throws IOException, InterruptedException, ExecutionException;
    }

    private void testWithDirectExecutor(ExecutorConsumer test) throws IOException, InterruptedException, ExecutionException
    {
        test.accept(MoreExecutors.directExecutor());
    }

    private void testWithThreadPool(ExecutorConsumer test) throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newCachedThreadPool();
        test.accept(es);
        es.shutdown();
        if (!es.awaitTermination(100, TimeUnit.MILLISECONDS))
        {
            es.shutdownNow();
            fail("Executor service was still running something");
        }
    }

    @Test
    public void testRunRerun() throws IOException, InterruptedException, ExecutionException
    {
        testWithDirectExecutor(this::testRunRerun);
    }

    @Test
    public void testRunRerunConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        testWithThreadPool(this::testRunRerun);
    }

    private void testRunRerun(Executor executor) throws IOException, InterruptedException, ExecutionException
    {
        // First, assemble a test graph with mock runnable and output objects.
        // We're using this eight-node graph:
        //
        // 0-1-2-3-4
        //    \ /
        //   5-6-7

        Random random = new Random(RUNNABLE_DURATION_SEED);
        AtomicBoolean outputMutabilityFlag = new AtomicBoolean();
        BuilderAssembler<TestTask, Builder<TestTask>> builderAssembler = BuilderAssembler.usingTasks(
                () -> TestTask.succeeding(random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag)
        );

        Workflow<TestTask> workflow = Workflow.create(builderAssembler.builderListTestConfig2());
        Target<TestTask> upTo2 = workflow.stoppingAfterKeys("2");

        TaskScheduler<Runnable> scheduler = LocalTaskScheduler.create(executor);
        OutputHandler outputHandler = OutputHandler.create();

        // Run everything!
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        Execution.newExecution(workflow, scheduler, outputHandler).run();

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        workflow.getNodes().values().forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        checkDependenciesFrom(workflow.getNodes().get("4"));
        checkDependenciesFrom(workflow.getNodes().get("7"));

        // Rerun node 2 and dependencies - nothing else should be executed
        Instant stage2start = Instant.now();
        outputMutabilityFlag.set(true);

        outputHandler.removeOutput(upTo2);
        Execution.newExecutionFromExistingOutput(upTo2, scheduler, outputHandler).run();

        outputMutabilityFlag.set(false);
        Instant stage2finish = Instant.now();
        Range<Instant> stage2 = Range.closed(stage2start, stage2finish);

        Stream.of("3", "4", "5", "6", "7").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        Stream.of("0", "1", "2").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));

        // Run everything - dependents of dependencies of node 2 should be executed
        Instant stage3start = Instant.now();
        outputMutabilityFlag.set(true);

        Execution.newExecutionFromExistingOutput(workflow, scheduler, outputHandler).run();

        outputMutabilityFlag.set(false);
        Instant stage3finish = Instant.now();
        Range<Instant> stage3 = Range.closed(stage3start, stage3finish);

        Stream.of("5").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        Stream.of("0", "1", "2").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));
        Stream.of("3", "4", "6", "7").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage3));
        checkDependenciesFrom(workflow.getNodes().get("4"));

        // Manually delete output of node 2, then run everything - only dependents of node 2 should be executed
        Instant stage4start = Instant.now();
        outputMutabilityFlag.set(true);

        for (Output output : workflow.getNodes().get("2").getTask().getOutputs())
        {
            output.delete();
        }
        Execution.newExecutionFromExistingOutput(workflow, scheduler, outputHandler).run();

        outputMutabilityFlag.set(false);
        Instant stage4finish = Instant.now();
        Range<Instant> stage4 = Range.closed(stage4start, stage4finish);

        Stream.of("5").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        Stream.of("0", "1").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));
        Stream.of("6", "7").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage3));
        Stream.of("2", "3", "4").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage4));
        checkDependenciesFrom(workflow.getNodes().get("4"));
        checkDependenciesFrom(workflow.getNodes().get("7"));
    }

    @Test
    public void testTaskExceptionHandling() throws IOException, InterruptedException, ExecutionException
    {
        testWithDirectExecutor(this::testTaskExceptionHandling);
    }

    @Test
    public void testTaskExceptionHandlingConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        testWithThreadPool(this::testTaskExceptionHandling);
    }

    private void testTaskExceptionHandling(Executor executor) throws IOException, InterruptedException
    {
        // Same eight-node graph as above, except node 2 will throw an exception when run.
        //
        // 0-1-2-3-4
        //    \ /
        //   5-6-7

        Random random = new Random(RUNNABLE_DURATION_SEED);
        AtomicBoolean outputMutabilityFlag = new AtomicBoolean();
        BuilderAssembler<TestTask, Builder<TestTask>> builderAssembler = BuilderAssembler.usingTasks(
                i -> i == 2 ?
                        TestTask.failingOnRun(random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag) :
                        TestTask.succeeding(random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag)
        );

        Workflow<TestTask> workflow = Workflow.create(builderAssembler.builderListTestConfig2());

        TaskScheduler<Runnable> scheduler = LocalTaskScheduler.create(executor);
        OutputHandler outputHandler = OutputHandler.create();

        // Run everything.
        // Nodes 0-1 should be executed (dependencies of node 2)
        // Node 2 should have its output removed on failure
        // Nodes 3-4 should not be executed (dependents of node 2)
        // Nodes 5-7 may execute and should execute completely if they do
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        try
        {
            Execution.newExecutionFromExistingOutput(workflow, scheduler, outputHandler).run();
            fail("Exception not propagated");
        }
        catch (ExecutionException e)
        {
            assertThat(e).hasCauseThat().isInstanceOf(TestTask.TestException.class);
        }

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        Stream.of("0", "1").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        Stream.of("2", "3", "4").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasNoOutput());
        checkDependenciesFrom(workflow.getNodes().get("7"));
    }

    @Test
    public void testOutputExceptionHandling() throws IOException, InterruptedException, ExecutionException
    {
        testWithDirectExecutor(this::testOutputExceptionHandling);
    }

    @Test
    public void testOutputExceptionHandlingConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        testWithThreadPool(this::testOutputExceptionHandling);
    }

    private void testOutputExceptionHandling(Executor executor) throws IOException, InterruptedException
    {
        // Same eight-node graph as above, except node 2 will throw an exception
        // when run or when outputs are deleted. Node run durations are set
        // such that in fully parallel environments, node 2 will fail while node
        // 6 is running. We want to make sure that the initial exception and the
        // resulting output-deletion exception are handled correctly.
        //
        // 0-1-2-3-4
        //    \ /
        //   5-6-7

        int[] durations = { 10, 10, 50, 10, 10, 20, 100, 20 };
        AtomicBoolean outputMutabilityFlag = new AtomicBoolean();
        BuilderAssembler<TestTask, Builder<TestTask>> builderAssembler = BuilderAssembler.usingTasks(
                i -> i == 2 ?
                        TestTask.failingOnOutputDelete(durations[i], outputMutabilityFlag) :
                        TestTask.succeeding(durations[i], outputMutabilityFlag)
        );

        Workflow<TestTask> workflow = Workflow.create(builderAssembler.builderListTestConfig2());

        TaskScheduler<Runnable> scheduler = LocalTaskScheduler.create(executor);
        OutputHandler outputHandler = OutputHandler.create();

        // Run everything.
        // Nodes 0-1 should be executed (dependencies of node 2)
        // Node 2 should have its output removed on failure
        // Nodes 3-4 should not be executed (dependents of node 2)
        // Nodes 5-7 may execute and should execute completely if they do
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        try
        {
            Execution.newExecutionFromExistingOutput(workflow, scheduler, outputHandler).run();
            fail("Exception not propagated");
        }
        catch (ExecutionException e)
        {
            assertThat(e).hasCauseThat().isInstanceOf(TestTask.TestException.class);
            assertThat(e.getSuppressed()).hasLength(1);
            for (Throwable t : e.getSuppressed())
            {
                assertThat(t).isInstanceOf(TestOutput.TestOutputException.class);
            }
        }

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        Stream.of("0", "1").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        Stream.of("2", "3", "4").map(workflow.getNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasNoOutput());
        checkDependenciesFrom(workflow.getNodes().get("7"));
    }

    /**
     * Starting at the given node, walks down the dependency tree and checks
     * that no dependency ran later than it should have.
     */
    private void checkDependenciesFrom(WorkflowNode<TestTask> node)
    {
        checkDependenciesFromHelper(node, Instant.MAX);
    }

    private void checkDependenciesFromHelper(WorkflowNode<TestTask> node, Instant maxTimestamp)
    {
        Instant maxDependencyTimestamp = maxTimestamp;

        if (node.hasTask())
        {
            TestTask task = node.getTask();
            assertThat(task).doesNotHaveOutputWithin(Range.greaterThan(maxTimestamp));

            Optional<Instant> startTimestamp = task.getStartOutput().getTimestamp();
            if (startTimestamp.isPresent())
            {
                Optional<Instant> finishTimestamp = task.getFinishOutput().getTimestamp();
                assertThat(finishTimestamp).isPresent();
                assertThat(finishTimestamp.get()).isAtLeast(startTimestamp.get());
                maxDependencyTimestamp = startTimestamp.get();
            }
        }

        for (WorkflowNode<TestTask> dependency : node.getDependencies())
        {
            checkDependenciesFromHelper(dependency, maxDependencyTimestamp);
        }
    }
}
