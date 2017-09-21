package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

final class TestOutput implements Output
{
    private final AtomicBoolean m_mutabilityFlag;
    private final boolean m_failOnDelete;

    @Nullable
    private Instant m_timestamp;

    public static class TestOutputException extends IOException
    {}

    public TestOutput(AtomicBoolean mutabilityFlag, boolean failOnDelete)
    {
        m_mutabilityFlag = Preconditions.checkNotNull(mutabilityFlag);
        m_failOnDelete = failOnDelete;
    }

    public void create()
    {
        Preconditions.checkState(m_mutabilityFlag.get());
        m_timestamp = Instant.now();
    }

    @Override
    public Optional<Instant> getTimestamp()
    {
        return Optional.ofNullable(m_timestamp);
    }

    @Override
    public void delete() throws IOException
    {
        Preconditions.checkState(m_mutabilityFlag.get());
        m_timestamp = null;
        if (m_failOnDelete)
        {
            throw new TestOutputException();
        }
    }
}
