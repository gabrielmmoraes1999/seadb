package io.github.gabrielmmoraes1999.seadb;

/**
 * Deadline and cancel flag for the statement running on this thread.
 * Another thread can mark the session cancelled without taking the engine lock.
 */
final class Pace {

    private static final ThreadLocal<Watch> LOCAL = new ThreadLocal<>();

    private Pace() {
    }

    static void arm(Session session, long deadlineMillis) {
        if (session != null) {
            session.cancelled = false;
        }
        LOCAL.set(new Watch(session, deadlineMillis));
    }

    static void clear() {
        LOCAL.remove();
    }

    static void check() {
        Watch watch = LOCAL.get();
        if (watch == null) {
            return;
        }
        if (watch.session != null && watch.session.cancelled) {
            throw new SeaException("command cancelled");
        }
        if (watch.deadlineMillis > 0 && System.currentTimeMillis() >= watch.deadlineMillis) {
            throw new SeaException("command timed out");
        }
    }

    static int seconds(int configured, int requested) {
        if (configured > 0 && requested > 0) {
            return Math.min(configured, requested);
        }
        return Math.max(configured, requested);
    }

    private record Watch(Session session, long deadlineMillis) {
    }
}
