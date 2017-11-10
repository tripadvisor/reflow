package com.tripadvisor.reflow;

import java.io.Serializable;

final class TestToken implements ScheduledTaskToken, Serializable
{
    private final int m_id;

    public TestToken(int id)
    {
        m_id = id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        TestToken testToken = (TestToken) o;
        return m_id == testToken.m_id;
    }

    @Override
    public int hashCode()
    {
        return m_id;
    }
}
