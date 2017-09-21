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
