package io.github.gabrielmmoraes1999.seadb;

/** A PSQL {@code EXCEPTION} statement. {@code WHEN EXCEPTION} catches this and nothing else. */
final class Raised extends SeaException {

    Raised(String message) {
        super(message);
    }
}
