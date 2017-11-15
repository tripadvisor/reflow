package com.tripadvisor.reflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;

import com.google.common.base.Preconditions;

/**
 * An output consisting of a single file or directory.
 */
public class PathOutput implements Output
{
    private final Path m_path;

    private PathOutput(Path path)
    {
        m_path = Preconditions.checkNotNull(path);
    }

    /**
     * Returns an output consisting of the given path.
     */
    public static PathOutput of(Path path)
    {
        return new PathOutput(path);
    }

    /**
     * Returns an output consisting of the given path.
     */
    public static PathOutput of(File file)
    {
        return new PathOutput(file.toPath());
    }

    /**
     * Returns an output consisting of the given path.
     */
    public static PathOutput of(String path)
    {
        return new PathOutput(Paths.get(path));
    }

    /**
     * Returns the modification time of the output file or directory,
     * or an empty optional if it does not exist.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Optional<Instant> getTimestamp() throws IOException
    {
        if (Files.notExists(m_path))
        {
            return Optional.empty();
        }

        return Optional.of(Files.readAttributes(m_path, BasicFileAttributes.class).lastModifiedTime().toInstant());
    }

    /**
     * Deletes the output file or directory.
     * Directories will be deleted even if they are not empty.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void delete() throws IOException
    {
        if (Files.notExists(m_path))
        {
            return;
        }

        Files.walkFileTree(m_path, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
            {
                if (exc != null)
                {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Returns a string representation of this output.
     */
    @Override
    public String toString()
    {
        return String.format("PathOutput(%s)", m_path.toString());
    }
}
