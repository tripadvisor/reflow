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
 * A collection of callback methods to be invoked
 * when a scheduled task is completed.
 */
public interface TaskCompletionCallback
{
    /**
     * Reports the success of a task.
     */
    void reportSuccess();

    /**
     * Reports the failure of a task.
     */
    void reportFailure();

    /**
     * Reports the failure of a task with an explanatory message.
     */
    void reportFailure(String message);

    /**
     * Reports the failure of a task, caused by a non-null throwable,
     * with an explanatory message.
     */
    void reportFailure(String message, Throwable cause);

    /**
     * Reports the failure of a task, caused by a non-null throwable.
     */
    void reportFailure(Throwable cause);
}
