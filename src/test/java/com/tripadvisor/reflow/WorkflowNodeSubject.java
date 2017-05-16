package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.truth.FailureStrategy;
import com.google.common.truth.Subject;
import com.google.common.truth.SubjectFactory;
import com.google.common.truth.Truth;

/**
 * Propositions for {@link WorkflowNode} typed subjects.
 */
final class WorkflowNodeSubject<T extends Task> extends Subject<WorkflowNodeSubject<T>, WorkflowNode<T>>
{
    private WorkflowNodeSubject(FailureStrategy failureStrategy, @Nullable WorkflowNode<T> actual)
    {
        super(failureStrategy, actual);
    }

    public static <U extends Task> WorkflowNodeSubject<U> assertThat(WorkflowNode<U> node)
    {
        return Truth.<WorkflowNodeSubject<U>, WorkflowNode<U>>assertAbout(nodes()).that(node);
    }

    public static <U extends Task> SubjectFactory<WorkflowNodeSubject<U>, WorkflowNode<U>> nodes()
    {
        return new SubjectFactory<WorkflowNodeSubject<U>, WorkflowNode<U>>()
        {
            @Override
            public WorkflowNodeSubject<U> getSubject(FailureStrategy failureStrategy, WorkflowNode<U> target)
            {
                return new WorkflowNodeSubject<>(failureStrategy, target);
            }
        };
    }

    /**
     * Fails if the given node is not a direct or indirect dependency
     * of the subject.
     */
    public void hasEventualDependency(WorkflowNode<T> dependency)
    {
        Preconditions.checkNotNull(dependency);
        check().withFailureMessage("%s is not a dependency of itself", actual())
                .that(actual()).isNotEqualTo(dependency);
        check().withFailureMessage("%s should have eventual dependency %s", actual(), dependency)
                .that(_nodeReachableFromSubject(dependency, node -> node.getDependencies().iterator())).isTrue();
    }

    /**
     * Fails if the given node is a direct or indirect dependency
     * of the subject.
     */
    public void doesNotHaveEventualDependency(WorkflowNode<T> dependency)
    {
        Preconditions.checkNotNull(dependency);
        check().withFailureMessage("%s should not have eventual dependency %s", actual(), dependency)
                .that(_nodeReachableFromSubject(dependency, node -> node.getDependencies().iterator())).isFalse();
    }

    private boolean _nodeReachableFromSubject(WorkflowNode<T> node,
                                              Function<WorkflowNode<T>, Iterator<WorkflowNode<T>>> neighborsFunc)
    {
        return Iterators.tryFind(
                TraversalUtils.traverseNodes(Iterators.singletonIterator(actual()), neighborsFunc),
                Predicates.equalTo(node)
        ).isPresent();
    }

    /**
     * Fails if the subject does not have the given number of direct
     * and indirect dependencies.
     */
    public void hasEventualDependencyCount(int count)
    {
        Preconditions.checkArgument(count >= 0);

        Set<WorkflowNode<T>> actualPlusDependencies = TraversalUtils.collectNodes(ImmutableSet.of(actual()),
                                                                                  WorkflowNode::getDependencies);

        check().withFailureMessage("Not true that %s has %s eventual dependencies", actual(), count)
                .that(actualPlusDependencies).hasSize(count + 1);
    }

    /**
     * Fails if the subject does not have the given number of direct
     * and indirect dependents.
     */
    public void hasEventualDependentCount(int count)
    {
        Preconditions.checkArgument(count >= 0);

        Set<WorkflowNode<T>> actualPlusDependents = TraversalUtils.collectNodes(ImmutableSet.of(actual()),
                                                                                WorkflowNode::getDependents);

        check().withFailureMessage("Not true that %s has %s eventual dependents", actual(), count)
                .that(actualPlusDependents).hasSize(count + 1);
    }
}
