package com.tripadvisor.reflow;

/**
 * Thrown to indicate that a provided {@link ScheduledTaskToken}
 * was invalid or not recognized.
 *
 * @see TaskScheduler
 */
public final class InvalidTokenException extends Exception
{
    public InvalidTokenException()
    {}

    public InvalidTokenException(String message)
    {
        super(message);
    }

    public InvalidTokenException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public InvalidTokenException(Throwable cause)
    {
        super(cause);
    }
}
