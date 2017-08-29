package com.tripadvisor.reflow;

import java.util.List;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import static java.util.stream.Collectors.toList;

/**
 * Instantiates builders with incrementing keys and assembles them into graphs.
 */
final class BuilderAssembler<T extends Task, B extends WorkflowNode.Builder<T>>
{
    private final IntFunction<B> m_builderFunc;

    public static <U extends Task> BuilderAssembler<U, StructureNode.Builder<U>> withoutTasks()
    {
        return new BuilderAssembler<>(i -> StructureNode.builder(Integer.toString(i)));
    }

    public static <U extends Task> BuilderAssembler<U, TaskNode.Builder<U>> usingTasks(Supplier<U> taskSupplier)
    {
        Preconditions.checkNotNull(taskSupplier);
        return new BuilderAssembler<>(i -> TaskNode.builder(Integer.toString(i), taskSupplier.get()));
    }

    public static <U extends Task> BuilderAssembler<U, TaskNode.Builder<U>> usingTasks(IntFunction<U> taskFunc)
    {
        Preconditions.checkNotNull(taskFunc);
        return new BuilderAssembler<>(i -> TaskNode.builder(Integer.toString(i), taskFunc.apply(i)));
    }

    private BuilderAssembler(IntFunction<B> builderFunc)
    {
        m_builderFunc = Preconditions.checkNotNull(builderFunc);
    }

    /**
     * Returns a list containing the given number of builders.
     */
    public List<B> builderList(int size)
    {
        return IntStream.range(0, size)
                .mapToObj(m_builderFunc)
                .collect(toList());
    }

    /**
     * Returns a list of builders with a fixed dependency configuration.
     */
    public List<B> builderListTestConfig1()
    {
        List<B> builders = builderList(3);
        builders.get(1).addDependencies(builders.get(0));
        builders.get(2).addDependencies(builders.get(1));
        return builders;
    }

    /**
     * Returns a list of builders with a fixed dependency configuration.
     */
    public List<B> builderListTestConfig2()
    {
        List<B> builders = builderList(8);
        builders.get(1).addDependencies(builders.get(0));
        builders.get(2).addDependencies(builders.get(1));
        builders.get(3).addDependencies(builders.get(2), builders.get(6));
        builders.get(4).addDependencies(builders.get(3));
        builders.get(6).addDependencies(builders.get(1), builders.get(5));
        builders.get(7).addDependencies(builders.get(6));
        return builders;
    }
}
