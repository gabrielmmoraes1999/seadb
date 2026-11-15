package io.github.gabrielmmoraes1999.seadb;

import java.math.BigDecimal;

/**
 * A small JSON reader. Objects and arrays keep their text; scalars become SQL values.
 */
final class JsonDoc {

    private JsonDoc() {
    }

    static void check(String text) {
        if (text == null) {
            throw new SeaException("invalid JSON");
        }
        Parser parser = new Parser(text);
        parser.value();
        parser.skip();
        if (parser.pos != parser.text.length()) {
            throw new SeaException("invalid JSON");
        }
    }

    static String typeName(String text) {
        if (text == null) {
            return null;
        }
        Parser parser = new Parser(text);
        String kind = parser.kind();
        parser.skip();
        if (parser.pos != parser.text.length()) {
            throw new SeaException("invalid JSON");
        }
        return kind;
    }

    static Object extract(String text, String path) {
        if (text == null || path == null) {
            return null;
        }
        if (!path.startsWith("$")) {
            throw new SeaException("JSON path must start with $");
        }
        Parser parser = new Parser(text);
        int start = parser.posAfterSkip();
        parser.value();
        int end = parser.pos;
        parser.skip();
        if (parser.pos != parser.text.length()) {
            throw new SeaException("invalid JSON");
        }
        return walk(text, start, end, path, 1);
    }

    private static Object walk(String text, int start, int end, String path, int at) {
        if (at >= path.length()) {
            return scalar(text.substring(start, end).trim());
        }
        char mark = path.charAt(at);
        Parser parser = new Parser(text.substring(start, end));
        if (mark == '.') {
            int nameAt = at + 1;
            int stop = nameAt;
            while (stop < path.length()) {
                char ch = path.charAt(stop);
                if (ch == '.' || ch == '[') {
                    break;
                }
                stop++;
            }
            if (stop == nameAt) {
                throw new SeaException("invalid JSON path");
            }
            String name = path.substring(nameAt, stop);
            Span found = parser.field(name);
            if (found == null) {
                return null;
            }
            return walk(text, start + found.start, start + found.end, path, stop);
        }
        if (mark == '[') {
            int close = path.indexOf(']', at);
            if (close < 0) {
                throw new SeaException("invalid JSON path");
            }
            int index;
            try {
                index = Integer.parseInt(path.substring(at + 1, close));
            } catch (NumberFormatException exception) {
                throw new SeaException("invalid JSON path");
            }
            Span found = parser.element(index);
            if (found == null) {
                return null;
            }
            return walk(text, start + found.start, start + found.end, path, close + 1);
        }
        throw new SeaException("invalid JSON path");
    }

    private static Object scalar(String raw) {
        String trimmed = raw.trim();
        if (trimmed.equals("null")) {
            return null;
        }
        if (trimmed.equals("true")) {
            return Boolean.TRUE;
        }
        if (trimmed.equals("false")) {
            return Boolean.FALSE;
        }
        if (trimmed.startsWith("\"")) {
            return unquote(trimmed);
        }
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return trimmed;
        }
        try {
            if (trimmed.indexOf('.') >= 0 || trimmed.indexOf('e') >= 0 || trimmed.indexOf('E') >= 0) {
                return new BigDecimal(trimmed);
            }
            return Long.parseLong(trimmed);
        } catch (NumberFormatException exception) {
            throw new SeaException("invalid JSON");
        }
    }

    private static String unquote(String raw) {
        StringBuilder out = new StringBuilder();
        for (int i = 1; i < raw.length() - 1; i++) {
            char ch = raw.charAt(i);
            if (ch != '\\') {
                out.append(ch);
                continue;
            }
            char next = raw.charAt(++i);
            switch (next) {
                case '"', '\\', '/' -> out.append(next);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    int code = Integer.parseInt(raw.substring(i + 1, i + 5), 16);
                    out.append((char) code);
                    i += 4;
                }
                default -> throw new SeaException("invalid JSON");
            }
        }
        return out.toString();
    }

    private record Span(int start, int end) {
    }

    private static final class Parser {
        private final String text;
        private int pos;

        private Parser(String text) {
            this.text = text;
        }

        private int posAfterSkip() {
            skip();
            return pos;
        }

        private void value() {
            kind();
        }

        private String kind() {
            skip();
            if (pos >= text.length()) {
                throw new SeaException("invalid JSON");
            }
            char ch = text.charAt(pos);
            if (ch == '{') {
                object();
                return "object";
            }
            if (ch == '[') {
                array();
                return "array";
            }
            if (ch == '"') {
                string();
                return "string";
            }
            if (ch == 't' || ch == 'f') {
                literal(ch == 't' ? "true" : "false");
                return "boolean";
            }
            if (ch == 'n') {
                literal("null");
                return "null";
            }
            if (ch == '-' || (ch >= '0' && ch <= '9')) {
                number();
                return "number";
            }
            throw new SeaException("invalid JSON");
        }

        private Span field(String name) {
            skip();
            if (pos >= text.length() || text.charAt(pos) != '{') {
                return null;
            }
            pos++;
            skip();
            if (pos < text.length() && text.charAt(pos) == '}') {
                return null;
            }
            while (pos < text.length()) {
                skip();
                int keyStart = pos;
                string();
                String key = unquote(text.substring(keyStart, pos));
                skip();
                expect(':');
                skip();
                int valueStart = pos;
                value();
                int valueEnd = pos;
                if (key.equals(name)) {
                    return new Span(valueStart, valueEnd);
                }
                skip();
                if (pos < text.length() && text.charAt(pos) == ',') {
                    pos++;
                    continue;
                }
                break;
            }
            return null;
        }

        private Span element(int index) {
            skip();
            if (pos >= text.length() || text.charAt(pos) != '[') {
                return null;
            }
            pos++;
            skip();
            if (pos < text.length() && text.charAt(pos) == ']') {
                return null;
            }
            int at = 0;
            while (pos < text.length()) {
                skip();
                int valueStart = pos;
                value();
                int valueEnd = pos;
                if (at == index) {
                    return new Span(valueStart, valueEnd);
                }
                at++;
                skip();
                if (pos < text.length() && text.charAt(pos) == ',') {
                    pos++;
                    continue;
                }
                break;
            }
            return null;
        }

        private void object() {
            expect('{');
            skip();
            if (consume('}')) {
                return;
            }
            while (true) {
                string();
                skip();
                expect(':');
                value();
                skip();
                if (consume('}')) {
                    return;
                }
                expect(',');
            }
        }

        private void array() {
            expect('[');
            skip();
            if (consume(']')) {
                return;
            }
            while (true) {
                value();
                skip();
                if (consume(']')) {
                    return;
                }
                expect(',');
            }
        }

        private void string() {
            expect('"');
            while (pos < text.length()) {
                char ch = text.charAt(pos++);
                if (ch == '"') {
                    return;
                }
                if (ch == '\\') {
                    if (pos >= text.length()) {
                        throw new SeaException("invalid JSON");
                    }
                    pos++;
                }
            }
            throw new SeaException("invalid JSON");
        }

        private void number() {
            int start = pos;
            if (pos < text.length() && text.charAt(pos) == '-') {
                pos++;
            }
            if (pos >= text.length() || text.charAt(pos) < '0' || text.charAt(pos) > '9') {
                throw new SeaException("invalid JSON");
            }
            while (pos < text.length()) {
                char ch = text.charAt(pos);
                if ((ch >= '0' && ch <= '9') || ch == '.' || ch == 'e' || ch == 'E' || ch == '+' || ch == '-') {
                    pos++;
                    continue;
                }
                break;
            }
            try {
                new BigDecimal(text.substring(start, pos));
            } catch (NumberFormatException exception) {
                throw new SeaException("invalid JSON");
            }
        }

        private void literal(String word) {
            if (!text.startsWith(word, pos)) {
                throw new SeaException("invalid JSON");
            }
            pos += word.length();
        }

        private void skip() {
            while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
                pos++;
            }
        }

        private boolean consume(char ch) {
            if (pos < text.length() && text.charAt(pos) == ch) {
                pos++;
                return true;
            }
            return false;
        }

        private void expect(char ch) {
            if (!consume(ch)) {
                throw new SeaException("invalid JSON");
            }
        }
    }
}
