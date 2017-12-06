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

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

/**
 * An abstract unit of task output. For example, subclasses could represent a
 * file or data in a database. Outputs can be in a "created" state (in the case
 * of a file, the file exists) or "deleted" state (the file does not exist).
 */
public interface Output
{
    /**
     * Returns the time at which this output was created,
     * or an empty optional if the output does not exist.
     *
     * @throws IOException if an I/O error occurs
     */
    Optional<Instant> getTimestamp() throws IOException;

    /**
     * Deletes this output if it exists.
     *
     * @throws IOException if an I/O error occurs
     */
    void delete() throws IOException;
}
