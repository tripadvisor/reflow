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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
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
        // Wrap output instances each time to make sure we don't depend on object identity
        return ImmutableSet.copyOf(Collections2.transform(getTestOutputs(), ForwardingOutput::new));
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
