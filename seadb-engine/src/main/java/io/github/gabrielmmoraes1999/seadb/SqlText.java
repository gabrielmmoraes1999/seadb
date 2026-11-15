package io.github.gabrielmmoraes1999.seadb;

/**
 * Replaces a simple identifier in stored SQL. Literals and comments stay as they are.
 */
final class SqlText {

    private SqlText() {
    }

    static String rename(String sql, String from, String to) {
        if (sql == null || sql.isEmpty() || from == null) {
            return sql;
        }
        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        while (i < sql.length()) {
            char ch = sql.charAt(i);
            if (ch == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                if (end < 0) {
                    out.append(sql.substring(i));
                    break;
                }
                out.append(sql, i, end);
                i = end;
                continue;
            }
            if (ch == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end < 0) {
                    out.append(sql.substring(i));
                    break;
                }
                out.append(sql, i, end + 2);
                i = end + 2;
                continue;
            }
            if (ch == '\'') {
                int end = i + 1;
                while (end < sql.length()) {
                    if (sql.charAt(end) == '\'' && end + 1 < sql.length() && sql.charAt(end + 1) == '\'') {
                        end += 2;
                        continue;
                    }
                    if (sql.charAt(end) == '\'') {
                        end++;
                        break;
                    }
                    end++;
                }
                out.append(sql, i, end);
                i = end;
                continue;
            }
            if (ch == '"') {
                int end = i + 1;
                while (end < sql.length()) {
                    if (sql.charAt(end) == '"' && end + 1 < sql.length() && sql.charAt(end + 1) == '"') {
                        end += 2;
                        continue;
                    }
                    if (sql.charAt(end) == '"') {
                        end++;
                        break;
                    }
                    end++;
                }
                String body = sql.substring(i + 1, Math.max(i + 1, end - 1)).replace("\"\"", "\"");
                if (Names.same(body, from)) {
                    out.append('"').append(to.replace("\"", "\"\"")).append('"');
                } else {
                    out.append(sql, i, end);
                }
                i = end;
                continue;
            }
            if (Character.isLetter(ch) || ch == '_' || ch == '$') {
                int end = i + 1;
                while (end < sql.length()) {
                    char next = sql.charAt(end);
                    if (Character.isLetterOrDigit(next) || next == '_' || next == '$') {
                        end++;
                        continue;
                    }
                    break;
                }
                String word = sql.substring(i, end);
                out.append(Names.same(word, from) ? matchCase(word, to) : word);
                i = end;
                continue;
            }
            out.append(ch);
            i++;
        }
        return out.toString();
    }

    /** Keep the replacement readable when the old token was written in one case. */
    private static String matchCase(String word, String to) {
        return to;
    }

    static boolean cites(String sql, String name) {
        if (sql == null || name == null) {
            return false;
        }
        String renamed = rename(sql, name, name + "\u0001");
        return !renamed.equals(sql);
    }
}
