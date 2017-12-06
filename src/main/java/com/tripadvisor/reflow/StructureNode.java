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
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * A node that has no associated task and serves only to link other nodes.
 */
public class StructureNode<T extends Task> extends WorkflowNode<T>
{
    private static final long serialVersionUID = 0L;

    private StructureNode(String key)
    {
        super(key);
    }

    /**
     * Returns {@code false}.
     */
    @Override
    public boolean hasTask()
    {
        return false;
    }

    /**
     * Immediately throws an exception.
     *
     * @deprecated
     * There's no task to get.
     *
     * @throws NoSuchElementException always
     */
    @Override
    @Deprecated
    public T getTask()
    {
        throw new NoSuchElementException();
    }

    public static final class Builder<U extends Task> extends WorkflowNode.Builder<U>
    {
        /**
         * {@inheritDoc}
         */
        @CanIgnoreReturnValue
        @Override
        public Builder<U> setKey(@Nullable String key)
        {
            return (Builder<U>) super.setKey(key);
        }

        /**
         * {@inheritDoc}
         */
        @CanIgnoreReturnValue
        @Override
        public Builder<U> setDependencies(Set<WorkflowNode.Builder<U>> dependencies)
        {
            return (Builder<U>) super.setDependencies(dependencies);
        }

        @Override
        StructureNode<U> build(String key)
        {
            return new StructureNode<>(key);
        }
    }

    /**
     * Returns a new builder.
     */
    public static <U extends Task> StructureNode.Builder<U> builder()
    {
        return new StructureNode.Builder<>();
    }

    /**
     * Returns a new builder associated with the given key.
     */
    public static <U extends Task> StructureNode.Builder<U> builder(@Nullable String key)
    {
        return new StructureNode.Builder<U>().setKey(key);
    }
}
