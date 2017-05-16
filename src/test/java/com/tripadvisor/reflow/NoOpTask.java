package com.tripadvisor.reflow;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

final class NoOpTask implements Task
{
    private final int m_id;

    public NoOpTask(int id)
    {
        m_id = id;
    }

    @Override
    public Set<Output> getOutputs()
    {
        return ImmutableSet.of();
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), m_id);
    }
}
