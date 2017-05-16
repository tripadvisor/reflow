package com.tripadvisor.reflow;

import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import com.tripadvisor.reflow.TaskNode.Builder;

import static java.util.stream.Collectors.toList;

/**
 * Given a task supplier, instantiates builders with incrementing keys
 * and assembles them into graphs.
 */
final class BuilderAssembler<T extends Task>
{
    private final IntFunction<T> m_taskFunc;

    public BuilderAssembler(Supplier<T> taskSupplier)
    {
        Preconditions.checkNotNull(taskSupplier);
        m_taskFunc = i -> taskSupplier.get();
    }

    public BuilderAssembler(IntFunction<T> taskFunc)
    {
        m_taskFunc = Preconditions.checkNotNull(taskFunc);
    }

    /**
     * Returns a list containing the given number of builders.
     */
    public List<Builder<Integer, T>> builderList(int size)
    {
        return IntStream.range(0, size)
                .mapToObj(i -> TaskNode.builder(i, m_taskFunc.apply(i)))
                .collect(toList());
    }

    /**
     * Returns a list of builders with a fixed dependency configuration.
     */
    public List<Builder<Integer, T>> builderListTestConfig1()
    {
        List<Builder<Integer, T>> builders = builderList(3);
        builders.get(1).addDependencies(builders.get(0));
        builders.get(2).addDependencies(builders.get(1));
        return builders;
    }

    /**
     * Returns a list of builders with a fixed dependency configuration.
     */
    public List<Builder<Integer, T>> builderListTestConfig2()
    {
        List<Builder<Integer, T>> builders = builderList(8);
        builders.get(1).addDependencies(builders.get(0));
        builders.get(2).addDependencies(builders.get(1));
        builders.get(3).addDependencies(builders.get(2), builders.get(6));
        builders.get(4).addDependencies(builders.get(3));
        builders.get(6).addDependencies(builders.get(1), builders.get(5));
        builders.get(7).addDependencies(builders.get(6));
        return builders;
    }
}
