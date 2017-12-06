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

/**
 * The state of an execution.
 *
 * <p>Possible transitions between states:
 *
 * <p>{@link #IDLE} -&gt; {@link #RUNNING}
 * <br>{@link #RUNNING} -&gt; {@link #IDLE}
 * <br>{@link #RUNNING} -&gt; {@link #SHUTDOWN}
 * <br>{@link #SHUTDOWN} -&gt; {@link #IDLE}
 */
public enum ExecutionState
{
    /**
     * Execution is ready to start.
     *
     * <p>Note that this state indicates only that the scheduling loop is idle.
     * Tasks that have been scheduled but not completed may continue to run.</p>
     */
    IDLE,

    /**
     * Execution is ongoing.
     */
    RUNNING,

    /**
     * Execution is ongoing, but no new tasks will be scheduled.
     * Execution will stop once all scheduled tasks have finished.
     */
    SHUTDOWN,
}
