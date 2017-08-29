package com.tripadvisor.reflow;

import java.io.Serializable;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

final class NoOpTask implements Task, Serializable
{
    @Override
    public Set<Output> getOutputs()
    {
        return ImmutableSet.of();
    }
}
