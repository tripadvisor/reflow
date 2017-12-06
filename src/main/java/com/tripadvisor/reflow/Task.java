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

import java.util.Collection;

/**
 * An abstract unit of work. When run, a task should create all
 * of the outputs returned by its {@link #getOutputs()} method.
 */
public interface Task
{
    /**
     * Indicates what output data this task will create when run.
     * Any two calls to this method on this task should return equivalent
     * collections, in the sense that they represent the same output data.
     */
    Collection<Output> getOutputs();
}
