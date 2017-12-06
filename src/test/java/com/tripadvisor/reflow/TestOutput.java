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
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

final class TestOutput implements Output
{
    private final AtomicBoolean m_mutabilityFlag;
    private final boolean m_failOnDelete;

    @Nullable
    private Instant m_timestamp;

    public static class TestOutputException extends IOException
    {}

    public TestOutput(AtomicBoolean mutabilityFlag, boolean failOnDelete)
    {
        m_mutabilityFlag = Preconditions.checkNotNull(mutabilityFlag);
        m_failOnDelete = failOnDelete;
    }

    public void create()
    {
        Preconditions.checkState(m_mutabilityFlag.get());
        m_timestamp = Instant.now();
    }

    @Override
    public Optional<Instant> getTimestamp()
    {
        return Optional.ofNullable(m_timestamp);
    }

    @Override
    public void delete() throws IOException
    {
        Preconditions.checkState(m_mutabilityFlag.get());
        m_timestamp = null;
        if (m_failOnDelete)
        {
            throw new TestOutputException();
        }
    }
}
