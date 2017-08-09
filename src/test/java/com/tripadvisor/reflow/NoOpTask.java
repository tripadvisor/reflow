package com.tripadvisor.reflow;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

final class NoOpTask implements Task
{
    @Override
    public Set<Output> getOutputs()
    {
        return ImmutableSet.of();
    }
}
