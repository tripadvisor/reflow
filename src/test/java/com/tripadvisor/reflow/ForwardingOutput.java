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

import com.google.common.base.Preconditions;

final class ForwardingOutput implements Output
{
    private final Output m_delegate;

    public ForwardingOutput(Output delegate)
    {
        m_delegate = Preconditions.checkNotNull(delegate);
    }

    @Override
    public Optional<Instant> getTimestamp() throws IOException
    {
        return m_delegate.getTimestamp();
    }

    @Override
    public void delete() throws IOException
    {
        m_delegate.delete();
    }
}
