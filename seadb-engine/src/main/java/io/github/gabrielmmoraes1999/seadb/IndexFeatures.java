package io.github.gabrielmmoraes1999.seadb;

import java.util.Locale;
import java.util.Set;

/**
 * Partial and expression indexes. The predicate may use only key columns and literals.
 * A query uses the index only when its filter implies that predicate.
 */
final class IndexFeatures {

    private static final Set<String> PURE = Set.of(
            "LOWER", "UPPER", "LENGTH", "CHAR_LENGTH", "COALESCE", "SUBSTRING", "TRIM", "LTRIM", "RTRIM",
            "REPLACE", "POSITION", "ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "SIGN", "MOD");

    private IndexFeatures() {
    }

    static IndexDef define(Schema schema, CreateIndex statement) {
        int count = statement.keys().size();
        int[] columns = new int[count];
        String[] expressions = new String[count];
        boolean anyExpression = false;
        for (int i = 0; i < count; i++) {
            Expr expr = SqlParser.parseExpression(statement.keys().get(i));
            if (expr instanceof ColumnRef column) {
                int index = columnIndex(schema, column);
                rejectKeyColumn(schema, index);
                columns[i] = index;
                expressions[i] = null;
            } else {
                checkKeyExpression(expr, schema);
                columns[i] = typeColumn(expr, schema);
                expressions[i] = statement.keys().get(i);
                anyExpression = true;
            }
        }
        int[] include = new int[statement.include().size()];
        for (int i = 0; i < include.length; i++) {
            int index = schema.indexOf(statement.include().get(i));
            if (index < 0) {
                throw new SeaException("column not found: " + statement.include().get(i));
            }
            Column column = schema.columns.get(index);
            if (column.type == DataType.BLOB) {
                throw new SeaException("BLOB cannot be included in an index");
            }
            include[i] = index;
        }
        String predicate = statement.predicate();
        if (predicate != null) {
            checkPredicate(SqlParser.parseExpression(predicate), columns, schema);
        }
        return new IndexDef(statement.name(), columns, statement.unique(), statement.descending(), predicate,
                anyExpression ? expressions : null, include);
    }

    static boolean implies(Expr indexPredicate, Expr query, Schema schema) {
        if (indexPredicate == null) {
            return true;
        }
        if (query == null) {
            return false;
        }
        return entails(query, indexPredicate, schema);
    }

    static String canonical(Expr expr) {
        if (expr instanceof ColumnRef column) {
            String name = column.name();
            if (column.qualifier() != null) {
                return "c:" + Names.fold(column.qualifier()) + "." + Names.fold(name);
            }
            return "c:" + Names.fold(name);
        }
        if (expr instanceof Literal literal) {
            return "l:" + String.valueOf(literal.value());
        }
        if (expr instanceof Call call) {
            StringBuilder builder = new StringBuilder();
            builder.append(call.name().toUpperCase(Locale.ROOT)).append('(');
            for (int i = 0; i < call.args().size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                String arg = canonical(call.args().get(i));
                if (arg == null) {
                    return null;
                }
                builder.append(arg);
            }
            builder.append(')');
            return builder.toString();
        }
        return null;
    }

    private static void checkKeyExpression(Expr expr, Schema schema) {
        if (!(expr instanceof Call call) || !PURE.contains(call.name().toUpperCase(Locale.ROOT))) {
            throw new SeaException("index expression is not supported");
        }
        if (call.distinct() || call.star() || call.args().isEmpty()) {
            throw new SeaException("index expression is not supported");
        }
        if (typeColumn(expr, schema) < 0) {
            throw new SeaException("index expression needs a column");
        }
        for (Expr arg : call.args()) {
            checkKeyArgument(arg, schema);
        }
    }

    private static void checkKeyArgument(Expr expr, Schema schema) {
        if (expr instanceof ColumnRef column) {
            rejectKeyColumn(schema, columnIndex(schema, column));
            return;
        }
        if (expr instanceof Literal) {
            return;
        }
        if (expr instanceof Call) {
            checkKeyExpression(expr, schema);
            return;
        }
        throw new SeaException("index expression is not supported");
    }

    private static int typeColumn(Expr expr, Schema schema) {
        if (expr instanceof ColumnRef column) {
            return columnIndex(schema, column);
        }
        if (expr instanceof Call call) {
            for (Expr arg : call.args()) {
                int found = typeColumn(arg, schema);
                if (found >= 0) {
                    return found;
                }
            }
        }
        return -1;
    }

    private static void checkPredicate(Expr expr, int[] keyColumns, Schema schema) {
        if (expr instanceof Binary binary && (binary.op() == BinaryOp.AND || binary.op() == BinaryOp.OR)) {
            checkPredicate(binary.left(), keyColumns, schema);
            checkPredicate(binary.right(), keyColumns, schema);
            return;
        }
        if (expr instanceof Unary unary && unary.op() == UnaryOp.NOT) {
            checkPredicate(unary.expr(), keyColumns, schema);
            return;
        }
        if (expr instanceof Between between) {
            checkColumn(between.expr(), keyColumns, schema);
            checkLiteral(between.low());
            checkLiteral(between.high());
            return;
        }
        if (expr instanceof IsNull isNull) {
            checkColumn(isNull.expr(), keyColumns, schema);
            return;
        }
        if (expr instanceof Binary binary && comparison(binary.op())) {
            Expr column;
            Expr literal;
            if (binary.left() instanceof ColumnRef && binary.right() instanceof Literal) {
                column = binary.left();
                literal = binary.right();
            } else if (binary.right() instanceof ColumnRef && binary.left() instanceof Literal) {
                column = binary.right();
                literal = binary.left();
            } else {
                throw new SeaException("index predicate is not supported");
            }
            checkColumn(column, keyColumns, schema);
            if (((Literal) literal).value() == null) {
                throw new SeaException("index predicate does not accept NULL");
            }
            return;
        }
        throw new SeaException("index predicate is not supported");
    }

    private static void checkColumn(Expr expr, int[] keyColumns, Schema schema) {
        if (!(expr instanceof ColumnRef column)) {
            throw new SeaException("index predicate is not supported");
        }
        int index = columnIndex(schema, column);
        for (int key : keyColumns) {
            if (key == index) {
                return;
            }
        }
        throw new SeaException("index predicate must use a key column: " + column.name());
    }

    private static void checkLiteral(Expr expr) {
        if (!(expr instanceof Literal literal) || literal.value() == null) {
            throw new SeaException("index predicate is not supported");
        }
    }

    private static boolean comparison(BinaryOp op) {
        return op == BinaryOp.EQ || op == BinaryOp.NE || op == BinaryOp.LT || op == BinaryOp.GT
                || op == BinaryOp.LE || op == BinaryOp.GE;
    }

    private static int columnIndex(Schema schema, ColumnRef column) {
        int index = schema.indexOf(column.name());
        if (index < 0) {
            throw new SeaException("column not found: " + column.name());
        }
        return index;
    }

    private static void rejectKeyColumn(Schema schema, int index) {
        Column column = schema.columns.get(index);
        if (column.type == DataType.BLOB) {
            throw new SeaException("BLOB cannot be a key");
        }
        if (column.generated != null) {
            throw new SeaException("generated column cannot be a key: " + column.name);
        }
    }

    private static boolean entails(Expr query, Expr target, Schema schema) {
        if (target instanceof Binary binary && binary.op() == BinaryOp.AND) {
            return entails(query, binary.left(), schema) && entails(query, binary.right(), schema);
        }
        if (target instanceof Binary binary && binary.op() == BinaryOp.OR) {
            return entails(query, binary.left(), schema) || entails(query, binary.right(), schema);
        }
        if (target instanceof Unary unary && unary.op() == UnaryOp.NOT) {
            return false;
        }
        if (target instanceof Between between && !between.negated()) {
            return entails(query, new Binary(BinaryOp.GE, between.expr(), between.low()), schema)
                    && entails(query, new Binary(BinaryOp.LE, between.expr(), between.high()), schema);
        }
        return branchesEntail(query, target, schema);
    }

    private static boolean branchesEntail(Expr query, Expr atom, Schema schema) {
        if (query instanceof Binary binary && binary.op() == BinaryOp.OR) {
            return branchesEntail(binary.left(), atom, schema) && branchesEntail(binary.right(), atom, schema);
        }
        if (query instanceof Binary binary && binary.op() == BinaryOp.AND) {
            return branchesEntail(binary.left(), atom, schema) || branchesEntail(binary.right(), atom, schema);
        }
        if (query instanceof Between between && !between.negated()) {
            return branchesEntail(new Binary(BinaryOp.GE, between.expr(), between.low()), atom, schema)
                    || branchesEntail(new Binary(BinaryOp.LE, between.expr(), between.high()), atom, schema)
                    || atomImplied(new Binary(BinaryOp.GE, between.expr(), between.low()), atom, schema)
                    && atomImplied(new Binary(BinaryOp.LE, between.expr(), between.high()), atom, schema);
        }
        return atomImplied(query, atom, schema);
    }

    private static boolean atomImplied(Expr have, Expr need, Schema schema) {
        Bound left = bound(have, schema);
        Bound right = bound(need, schema);
        if (left == null || right == null || left.column != right.column) {
            return false;
        }
        try {
            return covers(left, right);
        } catch (SeaException exception) {
            return false;
        }
    }

    private static boolean covers(Bound have, Bound need) {
        int cmp = have.value == null || need.value == null ? 0 : Values.compare(have.value, need.value);
        return switch (need.op) {
            case EQ -> have.op == BoundOp.EQ && cmp == 0;
            case GT -> switch (have.op) {
                case EQ -> cmp > 0;
                case GT -> cmp >= 0;
                case GE -> cmp > 0;
                default -> false;
            };
            case GE -> switch (have.op) {
                case EQ -> cmp >= 0;
                case GT, GE -> cmp >= 0;
                default -> false;
            };
            case LT -> switch (have.op) {
                case EQ -> cmp < 0;
                case LT -> cmp <= 0;
                case LE -> cmp < 0;
                default -> false;
            };
            case LE -> switch (have.op) {
                case EQ -> cmp <= 0;
                case LT, LE -> cmp <= 0;
                default -> false;
            };
            case NOT_NULL -> have.op != BoundOp.NULL;
            case NULL -> have.op == BoundOp.NULL;
            default -> false;
        };
    }

    private static Bound bound(Expr expr, Schema schema) {
        if (expr instanceof IsNull isNull && isNull.expr() instanceof ColumnRef column) {
            int index = schema.indexOf(column.name());
            if (index < 0) {
                return null;
            }
            return new Bound(index, isNull.negated() ? BoundOp.NOT_NULL : BoundOp.NULL, null);
        }
        if (!(expr instanceof Binary binary) || !comparison(binary.op())) {
            return null;
        }
        ColumnRef column;
        Literal literal;
        BinaryOp op = binary.op();
        if (binary.left() instanceof ColumnRef left && binary.right() instanceof Literal right) {
            column = left;
            literal = right;
        } else if (binary.right() instanceof ColumnRef right && binary.left() instanceof Literal left) {
            column = right;
            literal = left;
            op = flip(op);
        } else {
            return null;
        }
        if (literal.value() == null) {
            return null;
        }
        int index = schema.indexOf(column.name());
        if (index < 0) {
            return null;
        }
        Column definition = schema.columns.get(index);
        Object value;
        try {
            value = Values.coerce(definition.type, definition.length, definition.scale, literal.value());
        } catch (SeaException exception) {
            return null;
        }
        BoundOp kind = switch (op) {
            case EQ -> BoundOp.EQ;
            case GT -> BoundOp.GT;
            case GE -> BoundOp.GE;
            case LT -> BoundOp.LT;
            case LE -> BoundOp.LE;
            default -> null;
        };
        if (kind == null) {
            return null;
        }
        return new Bound(index, kind, value);
    }

    private static BinaryOp flip(BinaryOp op) {
        return switch (op) {
            case LT -> BinaryOp.GT;
            case GT -> BinaryOp.LT;
            case LE -> BinaryOp.GE;
            case GE -> BinaryOp.LE;
            default -> op;
        };
    }

    private enum BoundOp {
        EQ, GT, GE, LT, LE, NULL, NOT_NULL
    }

    private record Bound(int column, BoundOp op, Object value) {
    }

    /**
     * A range should become a scan when the estimate is more than half the table.
     * A missing or incomparable bound keeps the index.
     */
    static boolean estimateExceedsHalf(long tableRows, long entries, Object min, Object max, Object low, Object high) {
        if (tableRows <= 0 || entries <= 0 || min == null || max == null) {
            return false;
        }
        double fraction = fraction(min, max, low, high);
        if (fraction < 0) {
            return false;
        }
        return fraction * entries > tableRows / 2.0;
    }

    static boolean wideRange(Schema schema, IndexTree.DiskIndex index, int keyAt, Object low, Object high,
                              String likePrefix, long tableRows, long entries, String storedMin, String storedMax) {
        if (storedMin == null || storedMax == null) {
            return false;
        }
        Column column = schema.columns.get(index.columns[keyAt]);
        Object min;
        Object max;
        Object from;
        Object to;
        try {
            if (likePrefix != null) {
                min = storedMin;
                max = storedMax;
                from = likePrefix;
                to = likePrefix + "\uffff";
            } else {
                min = Values.coerce(column.type, column.length, column.scale, storedMin);
                max = Values.coerce(column.type, column.length, column.scale, storedMax);
                from = low;
                to = high;
            }
        } catch (RuntimeException exception) {
            return false;
        }
        return estimateExceedsHalf(tableRows, entries, min, max, from, to);
    }

    private static double fraction(Object min, Object max, Object low, Object high) {
        Double minAt = position(min);
        Double maxAt = position(max);
        if (minAt == null || maxAt == null) {
            return -1;
        }
        Double from = low == null ? minAt : position(low);
        Double to = high == null ? maxAt : position(high);
        if (from == null || to == null) {
            return -1;
        }
        double width = maxAt - minAt;
        if (width <= 0) {
            return from <= minAt && to >= maxAt ? 1 : 0;
        }
        double covered = (Math.min(to, maxAt) - Math.max(from, minAt)) / width;
        if (covered < 0) {
            return 0;
        }
        return Math.min(1, covered);
    }

    private static Double position(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof java.time.LocalDate date) {
            return (double) date.toEpochDay();
        }
        if (value instanceof java.time.LocalDateTime time) {
            return (double) time.toEpochSecond(java.time.ZoneOffset.UTC);
        }
        if (value instanceof java.time.LocalTime time) {
            return (double) time.toNanoOfDay();
        }
        if (value instanceof String text) {
            double position = 0;
            double scale = 1;
            int limit = Math.min(text.length(), 4);
            for (int i = 0; i < limit; i++) {
                scale /= 65536d;
                position += text.charAt(i) * scale;
            }
            return position;
        }
        return null;
    }
}
