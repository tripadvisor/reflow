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
     */
    Optional<Instant> getTimestamp() throws IOException;

    /**
     * Deletes this output if it exists.
     */
    void delete() throws IOException;
}
