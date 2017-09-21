package com.tripadvisor.reflow;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

final class TestTask implements Task, Runnable
{
    private final int m_durationMs;
    private final TestOutput m_startOutput;
    private final TestOutput m_finishOutput;
    private final boolean m_failOnRun;

    public static class TestException extends RuntimeException
    {}

    private TestTask(int durationMs, AtomicBoolean outputMutabilityFlag,
                     boolean failOnRun, boolean failOnOutputDelete)
    {
        Preconditions.checkArgument(durationMs >= 0);
        m_durationMs = durationMs;
        m_startOutput = new TestOutput(outputMutabilityFlag, failOnOutputDelete);
        m_finishOutput = new TestOutput(outputMutabilityFlag, failOnOutputDelete);
        m_failOnRun = failOnRun;
    }

    public static TestTask succeeding(int durationMs, AtomicBoolean outputMutabilityFlag)
    {
        return new TestTask(durationMs, outputMutabilityFlag, false, false);
    }

    public static TestTask failingOnRun(int durationMs, AtomicBoolean outputMutabilityFlag)
    {
        return new TestTask(durationMs, outputMutabilityFlag, true, false);
    }

    public static TestTask failingOnOutputDelete(int durationMs, AtomicBoolean outputMutabilityFlag)
    {
        return new TestTask(durationMs, outputMutabilityFlag, true, true);
    }

    public TestOutput getStartOutput()
    {
        return m_startOutput;
    }

    public TestOutput getFinishOutput()
    {
        return m_finishOutput;
    }

    public Set<TestOutput> getTestOutputs()
    {
        return ImmutableSet.of(m_startOutput, m_finishOutput);
    }

    @Override
    public Set<Output> getOutputs()
    {
        return ImmutableSet.copyOf(getTestOutputs());
    }

    @Override
    public void run()
    {
        m_startOutput.create();

        try
        {
            Thread.sleep(m_durationMs);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        if (m_failOnRun)
        {
            throw new TestException();
        }

        m_finishOutput.create();
    }
}
