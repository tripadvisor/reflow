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
