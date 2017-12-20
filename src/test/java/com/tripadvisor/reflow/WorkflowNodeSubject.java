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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import com.google.common.truth.Truth;

/**
 * Propositions for {@link WorkflowNode} typed subjects.
 */
final class WorkflowNodeSubject<T extends Task> extends Subject<WorkflowNodeSubject<T>, WorkflowNode<T>>
{
    private WorkflowNodeSubject(FailureMetadata metadata, @Nullable WorkflowNode<T> actual)
    {
        super(metadata, actual);
    }

    public static <U extends Task> WorkflowNodeSubject<U> assertThat(WorkflowNode<U> node)
    {
        return Truth.<WorkflowNodeSubject<U>, WorkflowNode<U>>assertAbout(nodes()).that(node);
    }

    public static <U extends Task> Subject.Factory<WorkflowNodeSubject<U>, WorkflowNode<U>> nodes()
    {
        return WorkflowNodeSubject::new;
    }

    /**
     * Fails if the given node is not a direct or indirect dependency
     * of the subject.
     */
    public void hasEventualDependency(WorkflowNode<T> dependency)
    {
        Preconditions.checkNotNull(dependency);
        check().withMessage("%s is not a dependency of itself", actual())
                .that(actual()).isNotEqualTo(dependency);
        check().withMessage("%s should have eventual dependency %s", actual(), dependency)
                .that(nodeReachableFromSubject(dependency, node -> node.getDependencies().iterator())).isTrue();
    }

    /**
     * Fails if the given node is a direct or indirect dependency
     * of the subject.
     */
    public void doesNotHaveEventualDependency(WorkflowNode<T> dependency)
    {
        Preconditions.checkNotNull(dependency);
        check().withMessage("%s should not have eventual dependency %s", actual(), dependency)
                .that(nodeReachableFromSubject(dependency, node -> node.getDependencies().iterator())).isFalse();
    }

    private boolean nodeReachableFromSubject(WorkflowNode<T> node,
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

        check().withMessage("Not true that %s has %s eventual dependencies", actual(), count)
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

        check().withMessage("Not true that %s has %s eventual dependents", actual(), count)
                .that(actualPlusDependents).hasSize(count + 1);
    }
}
