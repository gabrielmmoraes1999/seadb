package io.github.gabrielmmoraes1999.seadb;

import java.text.Normalizer;
import java.util.Locale;

/**
 * Column comparison. {@code 0} is binary UTF-8. {@code 1} is case- and accent-insensitive.
 */
final class Collation {

    static final int BINARY = 0;
    static final int UNICODE_CI = 1;

    private Collation() {
    }

    static int parse(String name) {
        if (name == null || name.equalsIgnoreCase("BINARY")) {
            return BINARY;
        }
        if (name.equalsIgnoreCase("UNICODE_CI")) {
            return UNICODE_CI;
        }
        throw new SeaException("unknown collation: " + name);
    }

    static String fold(String text) {
        if (text == null) {
            return null;
        }
        String decomposed = Normalizer.normalize(text, Normalizer.Form.NFD);
        StringBuilder out = new StringBuilder(decomposed.length());
        for (int i = 0; i < decomposed.length(); ) {
            int point = decomposed.codePointAt(i);
            i += Character.charCount(point);
            if (Character.getType(point) == Character.NON_SPACING_MARK) {
                continue;
            }
            out.appendCodePoint(point);
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }

    static int compare(Object left, Object right, int collation) {
        if (collation == UNICODE_CI && left instanceof String leftText && right instanceof String rightText) {
            return fold(leftText).compareTo(fold(rightText));
        }
        return Values.compare(left, right);
    }
}
