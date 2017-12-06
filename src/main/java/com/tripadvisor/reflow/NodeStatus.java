package com.tripadvisor.reflow;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import static com.google.common.collect.Maps.toImmutableEnumMap;

/**
 * The status of a node within a particular execution.
 * Includes a state value and information about task scheduling.
 */
public abstract class NodeStatus implements Serializable
{
    private static final long serialVersionUID = 0L;

    /**
     * Returns the state of the associated node.
     */
    public abstract NodeState getState();

    /**
     * Returns the scheduled task token for the associated node, or an empty
     * optional if this status does not include a token. A token will generally
     * be available if the node state is {@link NodeState#SCHEDULED} and the
     * node includes a task.
     */
    public abstract Optional<ScheduledTaskToken> getToken();

    /**
     * Returns a status consisting of the given state value
     * and no scheduled task token.
     */
    static NodeStatus withoutToken(NodeState state)
    {
        return WithoutToken.get(state);
    }

    /**
     * Returns a status consisting of {@link NodeState#SCHEDULED}
     * and the given token.
     */
    static NodeStatus scheduledWithToken(ScheduledTaskToken token)
    {
        return ScheduledWithToken.create(token);
    }

    private static class WithoutToken extends NodeStatus implements Serializable
    {
        private static final ImmutableMap<NodeState, WithoutToken> INSTANCES = Arrays.stream(NodeState.values())
                .map(WithoutToken::new)
                .collect(toImmutableEnumMap(NodeStatus::getState, Function.identity()));

        private static final long serialVersionUID = 0L;

        private final NodeState m_state;

        private WithoutToken(NodeState state)
        {
            m_state = state;
        }

        public static WithoutToken get(NodeState state)
        {
            WithoutToken detail = INSTANCES.get(state);
            Preconditions.checkArgument(detail != null, "Invalid state value " + state);
            return detail;
        }

        private Object readResolve()
        {
            return get(m_state);
        }

        @Override
        public NodeState getState()
        {
            return m_state;
        }

        @Override
        public Optional<ScheduledTaskToken> getToken()
        {
            return Optional.empty();
        }
    }

    private static class ScheduledWithToken extends NodeStatus implements Serializable
    {
        private static final long serialVersionUID = 0L;

        private final ScheduledTaskToken m_token;

        private ScheduledWithToken(ScheduledTaskToken token)
        {
            m_token = token;
            validateState();
        }

        public static ScheduledWithToken create(ScheduledTaskToken token)
        {
            return new ScheduledWithToken(token);
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException
        {
            stream.defaultReadObject();
            validateState();
        }

        private void readObjectNoData() throws ObjectStreamException
        {
            throw new InvalidObjectException("No object data");
        }

        private void validateState()
        {
            Preconditions.checkNotNull(m_token, "Null token");
        }

        @Override
        public NodeState getState()
        {
            return NodeState.SCHEDULED;
        }

        @Override
        public Optional<ScheduledTaskToken> getToken()
        {
            return Optional.of(m_token);
        }
    }
}
