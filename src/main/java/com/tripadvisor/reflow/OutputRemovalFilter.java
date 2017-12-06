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
