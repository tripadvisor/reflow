package com.tripadvisor.reflow;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.truth.FailureStrategy;
import com.google.common.truth.Subject;
import com.google.common.truth.SubjectFactory;

import static com.google.common.truth.Truth.assertAbout;

/**
 * Propositions for {@link TestTask} typed subjects.
 */
final class TestTaskSubject extends Subject<TestTaskSubject, TestTask>
{
    private TestTaskSubject(FailureStrategy failureStrategy, @Nullable TestTask actual)
    {
        super(failureStrategy, actual);
    }

    public static TestTaskSubject assertThat(TestTask task)
    {
        return assertAbout(tasks()).that(task);
    }

    public static SubjectFactory<TestTaskSubject, TestTask> tasks()
    {
        return TEST_TASK_SUBJECT_FACTORY;
    }

    private static final SubjectFactory<TestTaskSubject, TestTask> TEST_TASK_SUBJECT_FACTORY =
            new SubjectFactory<TestTaskSubject, TestTask>()
            {
                @Override
                public TestTaskSubject getSubject(FailureStrategy failureStrategy, TestTask target)
                {
                    return new TestTaskSubject(failureStrategy, target);
                }
            };


    /**
     * Fails if any of the subject's output is present.
     */
    public void hasNoOutput()
    {
        for (TestOutput output : actual().getTestOutputs())
        {
            check().withFailureMessage("%s should not have written output %s", actual(), output)
                    .that(output.getTimestamp().isPresent()).isFalse();  // Not clear how to use OptionalSubject here
        }
    }

    /**
     * Fails if any of the subject's output is present
     * and has a timestamp greater than the given timestamp.
     */
    public void doesNotHaveOutputWithin(Range<Instant> timestampRange)
    {
        Preconditions.checkNotNull(timestampRange);

        for (TestOutput output : actual().getTestOutputs())
        {
            output.getTimestamp().ifPresent(timestamp -> check()
                    .withFailureMessage("%s should not have written output %s inside %s",
                                        actual(), output, timestampRange)
                    .that(timestamp).isNotIn(timestampRange));
        }
    }

    /**
     * Fails if any of the subject's output is missing
     * or has a timestamp outside of the given range.
     */
    public void hasAllOutputWithin(Range<Instant> timestampRange)
    {
        Preconditions.checkNotNull(timestampRange);

        for (TestOutput output : actual().getTestOutputs())
        {
            Optional<Instant> timestamp = output.getTimestamp();
            check().withFailureMessage("%s should have written output %s", actual(), output)
                    .that(timestamp.isPresent()).isTrue();  // Not clear how to use OptionalSubject here
            check().withFailureMessage("%s should have written output %s inside %s", actual(), output, timestampRange)
                    .that(timestamp.get()).isIn(timestampRange);
        }
    }
}
