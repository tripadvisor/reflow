package com.tripadvisor.reflow;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;

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

    private void _withDirectExecutor(ExecutorConsumer test) throws IOException, InterruptedException, ExecutionException
    {
        test.accept(MoreExecutors.directExecutor());
    }

    private void _withThreadPool(ExecutorConsumer test) throws IOException, InterruptedException, ExecutionException
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
        _withDirectExecutor(this::_testRunRerun);
    }

    @Test
    public void testRunRerunConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        _withThreadPool(this::_testRunRerun);
    }

    private void _testRunRerun(Executor executor) throws IOException, InterruptedException, ExecutionException
    {
        // First, assemble a test graph with mock runnable and output objects.
        // We're using this eight-node graph again:
        //
        // 0-1-2-3-4
        //    \ /
        //   5-6-7

        Random random = new Random(RUNNABLE_DURATION_SEED);
        AtomicBoolean outputMutabilityFlag = new AtomicBoolean();
        BuilderAssembler<TestTask> builderAssembler = new BuilderAssembler<>(
                i -> TestTask.succeeding(i, random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag)
        );

        List<TaskNode.Builder<Integer, TestTask>> template = builderAssembler.builderListTestConfig2();
        Workflow<Integer, TestTask> graph = Workflow.create(template);
        Target<TestTask> upTo2 = graph.stoppingAfterKeys(2);

        WorkflowExecutor<TestTask> workflowExecutor = WorkflowExecutor.create(
                ExecutorWorkflowCompletionService.from(executor, TestTask::run)
        );

        // Run everything!
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        workflowExecutor.executeFromExistingOutput(graph).run();

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        graph.getNodes().forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        _checkDependenciesFrom(graph.keyedNodes().get(4));
        _checkDependenciesFrom(graph.keyedNodes().get(7));

        // Rerun node 2 and dependencies - nothing else should be executed
        Instant stage2start = Instant.now();
        outputMutabilityFlag.set(true);

        workflowExecutor.clearOutput(upTo2);
        workflowExecutor.executeFromExistingOutput(upTo2).run();

        outputMutabilityFlag.set(false);
        Instant stage2finish = Instant.now();
        Range<Instant> stage2 = Range.closed(stage2start, stage2finish);

        IntStream.of(3, 4, 5, 6, 7).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        IntStream.of(0, 1, 2).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));

        // Run everything - dependents of dependencies of node 2 should be executed
        Instant stage3start = Instant.now();
        outputMutabilityFlag.set(true);

        workflowExecutor.executeFromExistingOutput(graph).run();

        outputMutabilityFlag.set(false);
        Instant stage3finish = Instant.now();
        Range<Instant> stage3 = Range.closed(stage3start, stage3finish);

        IntStream.of(5).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        IntStream.of(0, 1, 2).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));
        IntStream.of(3, 4, 6, 7).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage3));
        _checkDependenciesFrom(graph.keyedNodes().get(4));

        // Manually delete output of node 2, then run everything - only dependents of node 2 should be executed
        Instant stage4start = Instant.now();
        outputMutabilityFlag.set(true);

        for (Output output : graph.keyedNodes().get(2).getTask().getOutputs())
        {
            output.delete();
        }
        workflowExecutor.executeFromExistingOutput(graph).run();

        outputMutabilityFlag.set(false);
        Instant stage4finish = Instant.now();
        Range<Instant> stage4 = Range.closed(stage4start, stage4finish);

        IntStream.of(5).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        IntStream.of(0, 1).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage2));
        IntStream.of(6, 7).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage3));
        IntStream.of(2, 3, 4).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage4));
        _checkDependenciesFrom(graph.keyedNodes().get(4));
        _checkDependenciesFrom(graph.keyedNodes().get(7));
    }

    @Test
    public void testTaskExceptionHandling() throws IOException, InterruptedException, ExecutionException
    {
        _withDirectExecutor(this::_testTaskExceptionHandling);
    }

    @Test
    public void testTaskExceptionHandlingConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        _withThreadPool(this::_testTaskExceptionHandling);
    }

    private void _testTaskExceptionHandling(Executor executor) throws IOException, InterruptedException
    {
        // Same eight-node graph as above, except node 2 will throw an exception when run.
        //
        // 0-1-2-3-4
        //    \ /
        //   5-6-7

        Random random = new Random(RUNNABLE_DURATION_SEED);
        AtomicBoolean outputMutabilityFlag = new AtomicBoolean();
        BuilderAssembler<TestTask> builderAssembler = new BuilderAssembler<>(
                i -> i == 2 ?
                        TestTask.failingOnRun(i, random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag) :
                        TestTask.succeeding(i, random.nextInt(MAX_RUNNABLE_DURATION_MS), outputMutabilityFlag)
        );

        List<TaskNode.Builder<Integer, TestTask>> template = builderAssembler.builderListTestConfig2();
        Workflow<Integer, TestTask> graph = Workflow.create(template);

        WorkflowExecutor<TestTask> workflowExecutor = WorkflowExecutor.create(
                ExecutorWorkflowCompletionService.from(executor, TestTask::run)
        );

        // Run everything.
        // Nodes 0-1 should be executed (dependencies of node 2)
        // Node 2 should have its output removed on failure
        // Nodes 3-4 should not be executed (dependents of node 2)
        // Nodes 5-7 may execute and should execute completely if they do
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        try
        {
            workflowExecutor.executeFromExistingOutput(graph).run();
            fail("Exception not propagated");
        }
        catch (ExecutionException e)
        {
            assertThat(e).hasCauseThat().isInstanceOf(TestTask.TestException.class);
        }

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        IntStream.of(0, 1).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        IntStream.of(2, 3, 4).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasNoOutput());
        _checkDependenciesFrom(graph.keyedNodes().get(7));
    }

    @Test
    public void testOutputExceptionHandling() throws IOException, InterruptedException, ExecutionException
    {
        _withDirectExecutor(this::_testOutputExceptionHandling);
    }

    @Test
    public void testOutputExceptionHandlingConcurrent() throws IOException, InterruptedException, ExecutionException
    {
        _withThreadPool(this::_testOutputExceptionHandling);
    }

    private void _testOutputExceptionHandling(Executor executor) throws IOException, InterruptedException
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
        BuilderAssembler<TestTask> builderAssembler = new BuilderAssembler<>(
                i -> i == 2 ?
                        TestTask.failingOnOutputDelete(i, durations[i], outputMutabilityFlag) :
                        TestTask.succeeding(i, durations[i], outputMutabilityFlag)
        );

        List<TaskNode.Builder<Integer, TestTask>> template = builderAssembler.builderListTestConfig2();
        Workflow<Integer, TestTask> graph = Workflow.create(template);

        WorkflowExecutor<TestTask> workflowExecutor = WorkflowExecutor.create(
                ExecutorWorkflowCompletionService.from(executor, TestTask::run)
        );

        // Run everything.
        // Nodes 0-1 should be executed (dependencies of node 2)
        // Node 2 should have its output removed on failure
        // Nodes 3-4 should not be executed (dependents of node 2)
        // Nodes 5-7 may execute and should execute completely if they do
        Instant stage1start = Instant.now();
        outputMutabilityFlag.set(true);

        try
        {
            workflowExecutor.executeFromExistingOutput(graph).run();
            fail("Exception not propagated");
        }
        catch (ExecutionException e)
        {
            assertThat(e).hasCauseThat().isInstanceOf(TestTask.TestException.class);
            assertThat(e.getSuppressed()).hasLength(2);
            for (Throwable t : e.getSuppressed())
            {
                assertThat(t).isInstanceOf(TestOutput.TestOutputException.class);
            }
        }

        outputMutabilityFlag.set(false);
        Instant stage1finish = Instant.now();
        Range<Instant> stage1 = Range.closed(stage1start, stage1finish);

        IntStream.of(0, 1).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasAllOutputWithin(stage1));
        IntStream.of(2, 3, 4).mapToObj(graph.keyedNodes()::get)
                .forEach(node -> assertThat(node.getTask()).hasNoOutput());
        _checkDependenciesFrom(graph.keyedNodes().get(7));
    }

    /**
     * Starting at the given node, walks down the dependency tree and checks
     * that no dependency ran later than it should have.
     */
    private void _checkDependenciesFrom(WorkflowNode<TestTask> node)
    {
        _checkDependenciesFromHelper(node, Instant.MAX);
    }

    private void _checkDependenciesFromHelper(WorkflowNode<TestTask> node, Instant maxTimestamp)
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
            };
        }

        for (WorkflowNode<TestTask> dependency : node.getDependencies())
        {
            _checkDependenciesFromHelper(dependency, maxDependencyTimestamp);
        }
    }
}
