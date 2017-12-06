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
import java.util.Map;

/**
 * Logic for preserving output when it would otherwise be removed.
 */
public interface OutputRemovalFilter
{
    /**
     * From a batch of output that is slated for removal, determines which
     * outputs to actually remove. Outputs are preserved by removing them from
     * the provided collection. The collection might not be thread safe, and it
     * should not be modified after this method returns.
     *
     * @param outputs outputs slated for removal, grouped by associated node
     * @param reason the reason outputs are being removed
     */
    void filterRemovals(Map<WorkflowNode<?>, Collection<Output>> outputs, OutputRemovalReason reason);
}
