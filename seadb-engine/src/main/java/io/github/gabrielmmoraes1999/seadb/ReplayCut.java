package io.github.gabrielmmoraes1999.seadb;

import java.time.Instant;

/**
 * Upper bound for {@link Database#applyLog} on this thread.
 * A commit with no stored time is always applied. A later commit is left out.
 */
final class ReplayCut {

    private static final ThreadLocal<Instant> UNTIL = new ThreadLocal<>();

    private ReplayCut() {
    }

    static void arm(Instant until) {
        if (until == null) {
            UNTIL.remove();
        } else {
            UNTIL.set(until);
        }
    }

    static void clear() {
        UNTIL.remove();
    }

    static Instant until() {
        return UNTIL.get();
    }
}
