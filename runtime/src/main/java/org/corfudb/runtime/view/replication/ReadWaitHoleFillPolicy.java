package org.corfudb.runtime.view.replication;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.HoleFillRequiredException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;


/**
 * A hole filling policy which reads several times,
 * waiting a static amount of time in between, before
 * requiring a hole fill.
 *
 * <p>Created by mwei on 4/6/17.
 */
public class ReadWaitHoleFillPolicy implements IHoleFillPolicy {

    /**
     * Maximum duration after which no more read attempts are made and an address hole fill is
     * attempted.
     */
    private final Duration maxHoleFillDuration;

    /**
     * Max wait interval between consequtive read attempts to cap exponential back-off.
     */
    private final Duration maxRetryWaitThreshold;

    /**
     * Create a ReadWaitHoleFillPolicy with the given wait times
     * and retries.
     *
     * @param maxHoleFillDuration   The amount of time to wait before retrying.
     * @param maxRetryWaitThreshold The number of retries to apply before requiring a
     *                              hole fill.
     */
    public ReadWaitHoleFillPolicy(Duration maxHoleFillDuration, Duration maxRetryWaitThreshold) {
        this.maxHoleFillDuration = maxHoleFillDuration;
        this.maxRetryWaitThreshold = maxRetryWaitThreshold;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ILogData peekUntilHoleFillRequired(long address, Function<Long, ILogData> peekFunction)
            throws HoleFillRequiredException {
        final AtomicLong startTime = new AtomicLong();

        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {

                // Try the read
                ILogData data = peekFunction.apply(address);
                // If it was not null, we can return it.
                if (data != null) {
                    return data;
                }

                if (startTime.get() == 0) {
                    startTime.set(System.currentTimeMillis());
                } else if (System.currentTimeMillis() - startTime.get() >= maxHoleFillDuration
                        .toMillis()) {
                    throw new RetryExhaustedException("Retries Exhausted.");
                }
                // Otherwise try again.
                throw new RetryNeededException();
            }).setOptions(x -> x.setMaxRetryThreshold(maxRetryWaitThreshold)).run();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (RetryExhaustedException ree) {
            // Retries exhausted. Hole filling.
        }

        throw new HoleFillRequiredException("No data after " + maxHoleFillDuration.toMillis() + "ms.");
    }
}
