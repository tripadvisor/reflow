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
 * Possible reasons for output removal.
 */
public enum OutputRemovalReason
{
    /**
     * Outputs are being removed because execution of the
     * task that creates them failed.
     */
    EXECUTION_FAILED,

    /**
     * Outputs are being removed via to a call to
     * {@link OutputHandler#removeOutput(Target)}.
     */
    REMOVAL_REQUESTED,

    /**
     * Outputs are being invalidated via to a call to
     * {@link OutputHandler#removeInvalidOutput(Target)},
     * and the output of a direct or indirect dependency is more recent.
     */
    PREDATES_DEPENDENCY,
}
