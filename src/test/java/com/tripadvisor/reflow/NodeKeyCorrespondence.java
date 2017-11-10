package com.tripadvisor.reflow;

import javax.annotation.Nullable;

import com.google.common.truth.Correspondence;

public class NodeKeyCorrespondence<T extends Task> extends Correspondence<WorkflowNode<T>, WorkflowNode<T>>
{
    @Override
    public boolean compare(@Nullable WorkflowNode<T> actual, @Nullable WorkflowNode<T> expected)
    {
        return expected.getKey().equals(actual.getKey());
    }

    @Override
    public String toString()
    {
        return "is a node with the same key as";
    }
}
