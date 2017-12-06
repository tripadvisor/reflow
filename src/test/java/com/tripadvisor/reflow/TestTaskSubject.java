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

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.truth.FailureStrategy;
import com.google.common.truth.Subject;
import com.google.common.truth.SubjectFactory;

import static com.google.common.truth.OptionalSubject.optionals;
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
            check().withMessage("%s should not have written output %s", actual(), output)
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
                    .withMessage("%s should not have written output %s inside %s",
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
            check().withMessage("%s should have written output %s", actual(), output).about(optionals())
                    .that(timestamp).isPresent();
            check().withMessage("%s should have written output %s inside %s", actual(), output, timestampRange)
                    .that(timestamp.get()).isIn(timestampRange);
        }
    }
}
