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

import java.lang.ref.WeakReference;

import com.google.common.base.Preconditions;

/**
 * A weak reference to a {@link TaskCompletionCallback}.
 * Does nothing if the wrapped callback has been reclaimed.
 */
class WeakCallback implements TaskCompletionCallback
{
    private final WeakReference<TaskCompletionCallback> m_delegate;

    public WeakCallback(TaskCompletionCallback delegate)
    {
        m_delegate = new WeakReference<>(Preconditions.checkNotNull(delegate));
    }

    @Override
    public void reportSuccess()
    {
        TaskCompletionCallback delegate = m_delegate.get();
        if (delegate != null)
        {
            delegate.reportSuccess();
        }
    }

    @Override
    public void reportFailure()
    {
        TaskCompletionCallback delegate = m_delegate.get();
        if (delegate != null)
        {
            delegate.reportFailure();
        }
    }

    @Override
    public void reportFailure(String message)
    {
        TaskCompletionCallback delegate = m_delegate.get();
        if (delegate != null)
        {
            delegate.reportFailure(message);
        }
    }

    @Override
    public void reportFailure(String message, Throwable cause)
    {
        TaskCompletionCallback delegate = m_delegate.get();
        if (delegate != null)
        {
            delegate.reportFailure(message, cause);
        }
    }

    @Override
    public void reportFailure(Throwable cause)
    {
        TaskCompletionCallback delegate = m_delegate.get();
        if (delegate != null)
        {
            delegate.reportFailure(cause);
        }
    }
}
