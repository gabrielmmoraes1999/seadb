package io.github.gabrielmmoraes1999.seadb;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.IntConsumer;

final class Evaluator {

    @FunctionalInterface
    interface Sequences {
        long next(String name, Long step);
    }

    @FunctionalInterface
    interface Contexts {
        Object read(String name, Integer precision);
    }

    interface Subqueries {
        Object scalar(Query query, Evaluator outer);

        Object in(Expr expr, Query query, boolean negated, Evaluator outer);

        Object exists(Query query, Evaluator outer);
    }

    @FunctionalInterface
    interface Functions {
        Object call(String name, List<Object> args);
    }

    private final TableStore table;
    private final Object[] params;
    private final String alias;
    private final RoutineScope scope;
    private final Sequences sequences;
    private final Contexts contexts;
    private final Subqueries subqueries;
    private Functions functions;
    Evaluator link;
    private StoredRow current;
    private Schema viewSchema;
    private String viewQualifier;
    private Object[] viewRow;
    private Schema excludedSchema;
    private Object[] excludedRow;
    private List<Frame> frames;
    private Object[] joinedRow;

    Evaluator(TableStore table, Object[] params, String alias) {
        this(table, params, alias, null, null, null, null);
    }

    Evaluator(TableStore table, Object[] params, String alias, RoutineScope scope, Sequences sequences, Contexts contexts,
              Subqueries subqueries) {
        this.table = table;
        this.params = params;
        this.alias = alias;
        this.scope = scope;
        this.sequences = sequences;
        this.contexts = contexts;
        this.subqueries = subqueries;
    }

    void bind(StoredRow row) {
        this.current = row;
        this.joinedRow = null;
    }

    void bindExcluded(Schema schema, Object[] values) {
        this.excludedSchema = schema;
        this.excludedRow = values;
    }

    void useView(Schema schema, String qualifier) {
        this.viewSchema = schema;
        this.viewQualifier = qualifier;
    }

    void bindView(Object[] row) {
        this.viewRow = row;
        this.current = null;
        this.joinedRow = null;
    }

    void useFrames(List<Frame> frames) {
        this.frames = frames;
    }

    boolean joined() {
        return frames != null;
    }

    void bindJoined(Object[] row) {
        this.joinedRow = row;
        this.current = null;
        this.viewRow = null;
    }

    Object evalGrouped(Expr expr, List<Integer> members, IntConsumer bind) {
        IntConsumer groupBind = index -> bind.accept(members.get(index));
        Expr folded = foldBound(expr, members.size(), groupBind);
        if (members.isEmpty()) {
            current = null;
            viewRow = null;
            joinedRow = null;
        } else {
            bind.accept(members.get(0));
        }
        return eval(folded);
    }

    boolean truthGrouped(Expr expr, List<Integer> members, IntConsumer bind) {
        Object value = evalGrouped(expr, members, bind);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new SeaException("condition must be logical");
    }

    boolean owns(ColumnRef column) {
        if (table == null) {
            return false;
        }
        if (column.qualifier() == null) {
            return true;
        }
        return Names.same(column.qualifier(), table.schema().name) || Names.same(column.qualifier(), alias);
    }

    boolean truth(Expr expr) {
        Object value = eval(expr);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new SeaException("condition must be logical");
    }

    Object eval(Expr expr) {
        return switch (expr) {
            case Literal literal -> literal.value();
            case Param param -> param(param);
            case ColumnRef column -> column(column);
            case QualifiedStar ignored -> throw new SeaException("* cannot be used in this expression");
            case VarRef variable -> {
                if (scope == null) {
                    throw new SeaException("variable " + variable.name() + " cannot be used in this statement");
                }
                yield scope.variable(variable.name());
            }
            case NextValue next -> nextValue(next);
            case ContextRef context -> {
                if (contexts == null) {
                    throw new SeaException("context " + context.name() + " cannot be used in this statement");
                }
                yield contexts.read(context.name(), context.precision());
            }
            case Unary unary -> unary(unary);
            case Binary binary -> binary(binary);
            case IsNull isNull -> {
                boolean absent = eval(isNull.expr()) == null;
                yield isNull.negated() ? !absent : absent;
            }
            case Like like -> like(like);
            case InList inList -> inList(inList);
            case Between between -> between(between);
            case Call call -> call(call);
            case WindowExpr ignored -> null;
            case CaseExpr caseExpr -> caseExpr(caseExpr);
            case CastExpr cast -> cast(cast);
            case ScalarSubquery subquery -> subquery(subquery);
            case InSubquery inSubquery -> inSubquery(inSubquery);
            case ExistsExpr exists -> exists(exists);
        };
    }

    Object evalAggregate(Expr expr, List<StoredRow> rows) {
        Expr folded = foldAggregates(expr, rows);
        current = null;
        if (containsColumn(folded)) {
            throw new SeaException("cannot mix aggregates and columns without GROUP BY");
        }
        return eval(folded);
    }

    Object evalAggregateValues(Expr expr, List<Object[]> rows) {
        Expr folded = foldAggregatesValues(expr, rows);
        viewRow = null;
        if (containsColumn(folded)) {
            throw new SeaException("cannot mix aggregates and columns without GROUP BY");
        }
        return eval(folded);
    }

    static boolean containsColumn(Expr expr) {
        return switch (expr) {
            case ColumnRef ignored -> true;
            case Call call -> call.args().stream().anyMatch(Evaluator::containsColumn);
            case Unary unary -> containsColumn(unary.expr());
            case Binary binary -> containsColumn(binary.left()) || containsColumn(binary.right());
            case IsNull isNull -> containsColumn(isNull.expr());
            case Like like -> containsColumn(like.expr()) || containsColumn(like.pattern())
                    || (like.escape() != null && containsColumn(like.escape()));
            case InList inList -> containsColumn(inList.expr()) || inList.values().stream().anyMatch(Evaluator::containsColumn);
            case Between between -> containsColumn(between.expr()) || containsColumn(between.low()) || containsColumn(between.high());
            case CaseExpr caseExpr -> containsCase(caseExpr, Evaluator::containsColumn);
            case CastExpr cast -> containsColumn(cast.expr());
            case InSubquery inSubquery -> containsColumn(inSubquery.expr());
            case ScalarSubquery ignored -> false;
            case ExistsExpr ignored -> false;
            default -> false;
        };
    }

    private Expr foldAggregates(Expr expr, List<StoredRow> rows) {
        return foldBound(expr, rows.size(), index -> bind(rows.get(index)));
    }

    private Expr foldAggregatesValues(Expr expr, List<Object[]> rows) {
        return foldBound(expr, rows.size(), index -> bindView(rows.get(index)));
    }

    private Expr foldBound(Expr expr, int size, IntConsumer bindRow) {
        return switch (expr) {
            case Call call when aggregate(call.name()) -> new Literal(computeAggregate(call, size, bindRow));
            case Call call -> new Call(call.name(), call.distinct(), call.star(), foldAllBound(call.args(), size, bindRow));
            case Unary unary -> new Unary(unary.op(), foldBound(unary.expr(), size, bindRow));
            case Binary binary -> new Binary(binary.op(), foldBound(binary.left(), size, bindRow), foldBound(binary.right(), size, bindRow));
            case IsNull isNull -> new IsNull(foldBound(isNull.expr(), size, bindRow), isNull.negated());
            case Like like -> new Like(foldBound(like.expr(), size, bindRow), foldBound(like.pattern(), size, bindRow),
                    like.escape() == null ? null : foldBound(like.escape(), size, bindRow), like.negated());
            case InList inList -> new InList(foldBound(inList.expr(), size, bindRow), foldAllBound(inList.values(), size, bindRow), inList.negated());
            case Between between -> new Between(foldBound(between.expr(), size, bindRow), foldBound(between.low(), size, bindRow),
                    foldBound(between.high(), size, bindRow), between.negated());
            case CaseExpr caseExpr -> foldCase(caseExpr, size, bindRow);
            case CastExpr cast -> new CastExpr(foldBound(cast.expr(), size, bindRow), cast.type(), cast.length(), cast.scale());
            case InSubquery inSubquery -> new InSubquery(foldBound(inSubquery.expr(), size, bindRow), inSubquery.query(), inSubquery.negated());
            case ScalarSubquery ignored -> expr;
            case ExistsExpr ignored -> expr;
            default -> expr;
        };
    }

    private List<Expr> foldAllBound(List<Expr> expressions, int size, IntConsumer bindRow) {
        List<Expr> folded = new ArrayList<>(expressions.size());
        for (Expr expr : expressions) {
            folded.add(foldBound(expr, size, bindRow));
        }
        return folded;
    }

    static boolean containsAggregate(Expr expr) {
        return switch (expr) {
            case Call call when aggregate(call.name()) -> true;
            case Call call -> call.args().stream().anyMatch(Evaluator::containsAggregate);
            case Unary unary -> containsAggregate(unary.expr());
            case Binary binary -> containsAggregate(binary.left()) || containsAggregate(binary.right());
            case IsNull isNull -> containsAggregate(isNull.expr());
            case Like like -> containsAggregate(like.expr()) || containsAggregate(like.pattern())
                    || (like.escape() != null && containsAggregate(like.escape()));
            case InList inList -> containsAggregate(inList.expr()) || inList.values().stream().anyMatch(Evaluator::containsAggregate);
            case Between between -> containsAggregate(between.expr()) || containsAggregate(between.low()) || containsAggregate(between.high());
            case CaseExpr caseExpr -> containsCase(caseExpr, Evaluator::containsAggregate);
            case CastExpr cast -> containsAggregate(cast.expr());
            case InSubquery inSubquery -> containsAggregate(inSubquery.expr());
            case ScalarSubquery ignored -> false;
            case ExistsExpr ignored -> false;
            default -> false;
        };
    }

    static boolean aggregate(String name) {
        String folded = name.toUpperCase(Locale.ROOT);
        return folded.equals("COUNT") || folded.equals("SUM") || folded.equals("MIN")
                || folded.equals("MAX") || folded.equals("AVG");
    }

    private Object computeAggregate(Call call, List<StoredRow> rows) {
        return computeAggregate(call, rows.size(), index -> bind(rows.get(index)));
    }

    private Object computeAggregateValues(Call call, List<Object[]> rows) {
        return computeAggregate(call, rows.size(), index -> bindView(rows.get(index)));
    }

    private Object computeAggregate(Call call, int size, java.util.function.IntConsumer bindRow) {
        String name = call.name().toUpperCase(Locale.ROOT);
        if (call.star()) {
            if (!name.equals("COUNT")) {
                throw new SeaException(name + " does not accept *");
            }
            return (long) size;
        }
        if (call.args().size() != 1) {
            throw new SeaException(name + " expects one argument");
        }
        List<Object> values = new ArrayList<>();
        Set<IndexKey> seen = new HashSet<>();
        Expr arg = call.args().get(0);
        for (int index = 0; index < size; index++) {
            bindRow.accept(index);
            Object value = eval(arg);
            if (value == null) {
                continue;
            }
            if (call.distinct() && !seen.add(new IndexKey(new Object[]{value}))) {
                continue;
            }
            values.add(value);
        }
        return switch (name) {
            case "COUNT" -> (long) values.size();
            case "SUM" -> sum(values);
            case "AVG" -> average(values);
            case "MIN" -> extreme(values, true);
            case "MAX" -> extreme(values, false);
            default -> throw new SeaException("unknown function: " + call.name());
        };
    }

    private Object sum(List<Object> values) {
        if (values.isEmpty()) {
            return null;
        }
        boolean floating = false;
        boolean decimal = false;
        for (Object value : values) {
            if (value instanceof Double || value instanceof Float) {
                floating = true;
            } else if (value instanceof BigDecimal) {
                decimal = true;
            } else if (!(value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)) {
                throw new SeaException("SUM expects numeric values");
            }
        }
        if (floating) {
            double total = 0;
            for (Object value : values) {
                total += Values.toDouble(value);
            }
            return total;
        }
        if (decimal) {
            BigDecimal total = BigDecimal.ZERO;
            for (Object value : values) {
                total = total.add(Values.asDecimal(value));
            }
            return total;
        }
        long total = 0;
        for (Object value : values) {
            total = Math.addExact(total, Values.toLong(value));
        }
        return total;
    }

    private Object average(List<Object> values) {
        Object total = sum(values);
        if (total == null) {
            return null;
        }
        if (total instanceof BigDecimal decimal) {
            return decimal.divide(BigDecimal.valueOf(values.size()), 8, RoundingMode.HALF_UP).stripTrailingZeros();
        }
        return ((Number) total).doubleValue() / values.size();
    }

    private Object extreme(List<Object> values, boolean minimum) {
        Object best = null;
        for (Object value : values) {
            if (best == null || (minimum ? Values.compare(value, best) < 0 : Values.compare(value, best) > 0)) {
                best = value;
            }
        }
        return best;
    }

    private Object param(Param param) {
        if (param.index() < 0 || param.index() >= params.length) {
            throw new SeaException("parameter ?" + (param.index() + 1) + " was not supplied");
        }
        return params[param.index()];
    }

    private Object column(ColumnRef column) {
        if (column.qualifier() != null && Names.same(column.qualifier(), "EXCLUDED")) {
            if (excludedSchema == null || excludedRow == null) {
                throw new SeaException("EXCLUDED cannot be used in this statement");
            }
            int index = excludedSchema.indexOf(column.name());
            if (index < 0) {
                throw new SeaException("column not found: " + column.name());
            }
            Object value = excludedRow[index];
            return value == Values.MISSING ? null : value;
        }
        if (column.qualifier() != null && (Names.same(column.qualifier(), "NEW") || Names.same(column.qualifier(), "OLD"))) {
            if (scope == null) {
                throw new SeaException(column.qualifier() + " does not exist in this context");
            }
            return scope.field(column.qualifier(), column.name());
        }
        if (frames != null) {
            return joinedColumn(column);
        }
        String qualifier = column.qualifier();
        if (qualifier != null && alias != null && Names.same(qualifier, alias)) {
            qualifier = null;
        }
        if (scope != null && qualifier == null && Names.same(column.name(), "ROW_COUNT") && !scope.hasVariable(column.name())
                && (scope.preferVariables || table == null || current == null)) {
            return scope.rowCount;
        }
        if (scope != null && qualifier == null && scope.preferVariables && scope.hasVariable(column.name())) {
            return scope.variable(column.name());
        }
        if (viewSchema != null) {
            boolean named = qualifier == null
                    || Names.same(qualifier, viewSchema.name)
                    || Names.same(qualifier, viewQualifier)
                    || (alias != null && Names.same(qualifier, alias));
            if (!named) {
                throw new SeaException("unknown qualifier: " + column.qualifier());
            }
            if (viewRow == null) {
                throw new SeaException("column " + column.name() + " cannot be used in this statement");
            }
            int index = viewSchema.indexOf(column.name());
            if (index < 0) {
                throw new SeaException("column not found: " + column.name());
            }
            Object value = viewRow[index];
            return value == Values.MISSING ? null : value;
        }
        if (table == null || current == null) {
            if (scope != null && qualifier == null && scope.hasVariable(column.name())) {
                return scope.variable(column.name());
            }
            throw new SeaException("column " + column.name() + " cannot be used in this statement");
        }
        if (qualifier != null
                && !Names.same(qualifier, table.schema().name)
                && !Names.same(qualifier, table.namespace() + "." + table.schema().name)
                && !Names.same(qualifier, alias)) {
            throw new SeaException("unknown qualifier: " + column.qualifier());
        }
        int index = table.schema().indexOf(column.name());
        if (index < 0) {
            throw new SeaException("column not found: " + column.name());
        }
        return table.value(current, index);
    }

    private long nextValue(NextValue next) {
        if (sequences == null) {
            throw new SeaException("generator cannot be used in this statement");
        }
        return sequences.next(next.name(), null);
    }

    private long genId(Call call) {
        if (call.args().size() != 2) {
            throw new SeaException("GEN_ID requires the generator name and the step");
        }
        if (sequences == null) {
            throw new SeaException("generator cannot be used in this statement");
        }
        Expr name = call.args().get(0);
        String generator;
        if (name instanceof ColumnRef column) {
            generator = column.qualifier() == null ? column.name() : column.qualifier() + "." + column.name();
        } else if (name instanceof Literal literal && literal.value() instanceof String text) {
            generator = text;
        } else {
            throw new SeaException("GEN_ID requires the generator name");
        }
        Object step = eval(call.args().get(1));
        if (step == null) {
            throw new SeaException("GEN_ID step cannot be null");
        }
        return sequences.next(generator, Values.toLong(step));
    }

    private Object unary(Unary unary) {
        Object value = eval(unary.expr());
        if (value == null) {
            return null;
        }
        return switch (unary.op()) {
            case NOT -> {
                if (!(value instanceof Boolean bool)) {
                    throw new SeaException("NOT requires a logical expression");
                }
                yield !bool;
            }
            case PLUS -> number(value, "unary +");
            case NEGATE -> negate(value);
        };
    }

    private Object negate(Object value) {
        if (value instanceof Double || value instanceof Float) {
            return -Values.toDouble(value);
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.negate();
        }
        if (value instanceof Long) {
            return Math.negateExact(((Number) value).longValue());
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return Math.negateExact(((Number) value).intValue());
        }
        throw new SeaException("cannot negate " + Values.display(value));
    }

    private int collationOf(Expr expr) {
        if (!(expr instanceof ColumnRef column)) {
            return Collation.BINARY;
        }
        if (table != null && (column.qualifier() == null || Names.same(column.qualifier(), table.schema().name)
                || (alias != null && Names.same(column.qualifier(), alias)))) {
            int index = table.schema().indexOf(column.name());
            if (index >= 0) {
                return table.schema().columns.get(index).collation;
            }
        }
        if (viewSchema != null) {
            int index = viewSchema.indexOf(column.name());
            if (index >= 0) {
                return viewSchema.columns.get(index).collation;
            }
        }
        if (frames != null) {
            Frame frame = Frame.find(frames, column.qualifier(), column.name());
            int index = frame.schema.indexOf(column.name());
            if (index >= 0) {
                return frame.schema.columns.get(index).collation;
            }
        }
        return Collation.BINARY;
    }

    private Object binary(Binary binary) {
        if (binary.op() == BinaryOp.AND) {
            return logicAnd(binary.left(), binary.right());
        }
        if (binary.op() == BinaryOp.OR) {
            return logicOr(binary.left(), binary.right());
        }
        Object left = eval(binary.left());
        Object right = eval(binary.right());
        if (binary.op() == BinaryOp.CONCAT) {
            if (left == null || right == null) {
                return null;
            }
            return text(left) + text(right);
        }
        if (left == null || right == null) {
            return null;
        }
        int collation = Math.max(collationOf(binary.left()), collationOf(binary.right()));
        return switch (binary.op()) {
            case EQ -> Collation.compare(left, right, collation) == 0;
            case NE -> Collation.compare(left, right, collation) != 0;
            case LT -> Collation.compare(left, right, collation) < 0;
            case GT -> Collation.compare(left, right, collation) > 0;
            case LE -> Collation.compare(left, right, collation) <= 0;
            case GE -> Collation.compare(left, right, collation) >= 0;
            case ADD -> add(left, right);
            case SUB -> subtract(left, right);
            case MUL -> multiply(left, right);
            case DIV -> divide(left, right);
            case MOD -> modulo(left, right);
            case AND, OR, CONCAT -> throw new IllegalStateException();
        };
    }

    private Object logicAnd(Expr left, Expr right) {
        Object first = eval(left);
        if (first instanceof Boolean bool && !bool) {
            return false;
        }
        Object second = eval(right);
        if (!(first == null || first instanceof Boolean) || !(second == null || second instanceof Boolean)) {
            throw new SeaException("AND requires logical expressions");
        }
        if (second instanceof Boolean bool && !bool) {
            return false;
        }
        if (first == null || second == null) {
            return null;
        }
        return true;
    }

    private Object logicOr(Expr left, Expr right) {
        Object first = eval(left);
        if (first instanceof Boolean bool && bool) {
            return true;
        }
        Object second = eval(right);
        if (!(first == null || first instanceof Boolean) || !(second == null || second instanceof Boolean)) {
            throw new SeaException("OR requires logical expressions");
        }
        if (second instanceof Boolean bool && bool) {
            return true;
        }
        if (first == null || second == null) {
            return null;
        }
        return false;
    }

    private Object like(Like like) {
        Object value = eval(like.expr());
        Object pattern = eval(like.pattern());
        if (value == null || pattern == null) {
            return null;
        }
        if (!(value instanceof String text) || !(pattern instanceof String sample)) {
            throw new SeaException("LIKE requires strings");
        }
        char escape = '\\';
        if (like.escape() != null) {
            Object raw = eval(like.escape());
            if (!(raw instanceof String escapeText) || escapeText.length() != 1) {
                throw new SeaException("ESCAPE must be a one-character string");
            }
            escape = escapeText.charAt(0);
        }
        boolean matched = Values.like(text, sample, escape);
        return like.negated() ? !matched : matched;
    }

    private Object inList(InList inList) {
        Object value = eval(inList.expr());
        boolean found = false;
        boolean unknown = value == null;
        for (Expr candidate : inList.values()) {
            Object item = eval(candidate);
            if (item == null || value == null) {
                unknown = true;
                continue;
            }
            if (Values.compare(value, item) == 0) {
                found = true;
            }
        }
        if (found) {
            return !inList.negated();
        }
        if (unknown) {
            return null;
        }
        return inList.negated();
    }

    private Object between(Between between) {
        Object value = eval(between.expr());
        Object low = eval(between.low());
        Object high = eval(between.high());
        Boolean greater = compareNullable(value, low, true);
        Boolean lesser = compareNullable(value, high, false);
        Boolean combined = and(greater, lesser);
        if (combined == null) {
            return null;
        }
        return between.negated() ? !combined : combined;
    }

    private Boolean compareNullable(Object left, Object right, boolean lowerBound) {
        if (left == null || right == null) {
            return null;
        }
        int cmp = Values.compare(left, right);
        return lowerBound ? cmp >= 0 : cmp <= 0;
    }

    private Boolean and(Boolean left, Boolean right) {
        if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
            return false;
        }
        if (left == null || right == null) {
            return null;
        }
        return true;
    }

    private Object call(Call call) {
        if (aggregate(call.name())) {
            throw new SeaException(call.name() + " can only be used in the SELECT list");
        }
        String name = call.name().toUpperCase(Locale.ROOT);
        if (name.equals("GEN_ID")) {
            return genId(call);
        }
        return switch (name) {
            case "LOWER", "UPPER" -> changeCase(call, name.equals("UPPER"));
            case "LENGTH", "CHAR_LENGTH" -> length(call);
            case "COALESCE" -> coalesce(call);
            case "SUBSTRING" -> substring(call);
            case "TRIM" -> trim(call, 0);
            case "LTRIM" -> trim(call, -1);
            case "RTRIM" -> trim(call, 1);
            case "REPLACE" -> replace(call);
            case "POSITION" -> position(call);
            case "ABS" -> abs(call);
            case "ROUND" -> round(call);
            case "CEIL", "CEILING" -> ceil(call, true);
            case "FLOOR" -> ceil(call, false);
            case "SIGN" -> sign(call);
            case "MOD" -> mod(call);
            case "EXTRACT" -> extract(call);
            case "DATEADD" -> dateAdd(call);
            case "DATEDIFF" -> dateDiff(call);
            case "JSON_EXTRACT" -> jsonExtract(call);
            case "JSON_TYPE" -> jsonType(call);
            default -> delegate(call);
        };
    }

    /**
     * Value of {@code column} when this binding owns it, or {@link Values#MISSING} when it belongs elsewhere.
     */
    Object readOwned(ColumnRef column) {
        if (frames != null) {
            if (joinedRow == null) {
                return Values.MISSING;
            }
            boolean found = false;
            for (Frame frame : frames) {
                if (column.qualifier() != null && !frame.matches(column.qualifier())) {
                    continue;
                }
                if (frame.schema.indexOf(column.name()) < 0) {
                    continue;
                }
                if (found) {
                    throw new SeaException("ambiguous column: " + column.name());
                }
                found = true;
            }
            if (!found) {
                return Values.MISSING;
            }
            return joinedColumn(column);
        }
        if (viewSchema != null) {
            boolean named = column.qualifier() == null
                    || Names.same(column.qualifier(), viewSchema.name)
                    || Names.same(column.qualifier(), viewQualifier)
                    || (alias != null && Names.same(column.qualifier(), alias));
            if (!named || viewRow == null || viewSchema.indexOf(column.name()) < 0) {
                return Values.MISSING;
            }
            Object value = viewRow[viewSchema.indexOf(column.name())];
            return value == Values.MISSING ? null : value;
        }
        if (table == null || current == null) {
            return Values.MISSING;
        }
        if (column.qualifier() != null
                && !Names.same(column.qualifier(), table.schema().name)
                && !Names.same(column.qualifier(), table.namespace() + "." + table.schema().name)
                && (alias == null || !Names.same(column.qualifier(), alias))) {
            return Values.MISSING;
        }
        int index = table.schema().indexOf(column.name());
        if (index < 0) {
            return Values.MISSING;
        }
        return table.value(current, index);
    }

    private Object subquery(ScalarSubquery subquery) {
        if (subqueries == null) {
            throw new SeaException("subquery cannot be used in this statement");
        }
        return subqueries.scalar(subquery.query(), this);
    }

    private Object inSubquery(InSubquery subquery) {
        if (subqueries == null) {
            throw new SeaException("subquery cannot be used in this statement");
        }
        return subqueries.in(subquery.expr(), subquery.query(), subquery.negated(), this);
    }

    private Object exists(ExistsExpr exists) {
        if (subqueries == null) {
            throw new SeaException("subquery cannot be used in this statement");
        }
        return subqueries.exists(exists.query(), this);
    }

    private Object caseExpr(CaseExpr expr) {
        if (expr.operand() != null) {
            Object basis = eval(expr.operand());
            for (WhenBranch branch : expr.branches()) {
                Object when = eval(branch.when());
                if (basis != null && when != null && Values.compare(basis, when) == 0) {
                    return eval(branch.then());
                }
            }
        } else {
            for (WhenBranch branch : expr.branches()) {
                if (truth(branch.when())) {
                    return eval(branch.then());
                }
            }
        }
        return expr.otherwise() == null ? null : eval(expr.otherwise());
    }

    private Object cast(CastExpr cast) {
        Object value = eval(cast.expr());
        if (value == null) {
            return null;
        }
        return Values.coerce(cast.type(), cast.length(), cast.scale(), value);
    }

    private static boolean containsCase(CaseExpr expr, java.util.function.Predicate<Expr> test) {
        if (expr.operand() != null && test.test(expr.operand())) {
            return true;
        }
        for (WhenBranch branch : expr.branches()) {
            if (test.test(branch.when()) || test.test(branch.then())) {
                return true;
            }
        }
        return expr.otherwise() != null && test.test(expr.otherwise());
    }

    private CaseExpr foldCase(CaseExpr expr, int size, IntConsumer bindRow) {
        List<WhenBranch> branches = new ArrayList<>();
        for (WhenBranch branch : expr.branches()) {
            branches.add(new WhenBranch(foldBound(branch.when(), size, bindRow), foldBound(branch.then(), size, bindRow)));
        }
        return new CaseExpr(
                expr.operand() == null ? null : foldBound(expr.operand(), size, bindRow),
                branches,
                expr.otherwise() == null ? null : foldBound(expr.otherwise(), size, bindRow));
    }

    private Object substring(Call call) {
        if (call.args().size() < 2 || call.args().size() > 3) {
            throw new SeaException("SUBSTRING expects the text, the start, and the length");
        }
        Object value = eval(call.args().get(0));
        Object startValue = eval(call.args().get(1));
        Object lengthValue = call.args().size() == 3 ? eval(call.args().get(2)) : null;
        if (value == null || startValue == null || (call.args().size() == 3 && lengthValue == null)) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new SeaException("SUBSTRING expects a string");
        }
        int start = Values.toInt(startValue);
        if (start < 1) {
            throw new SeaException("SUBSTRING position starts at 1");
        }
        int from = start - 1;
        if (from >= text.length()) {
            return "";
        }
        if (call.args().size() == 2) {
            return text.substring(from);
        }
        int length = Values.toInt(lengthValue);
        if (length < 0) {
            throw new SeaException("SUBSTRING length cannot be negative");
        }
        int to = Math.min(text.length(), from + length);
        return text.substring(from, to);
    }

    /** {@code side} is -1 for left, 1 for right, and 0 for both. */
    private Object trim(Call call, int side) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new SeaException(call.name() + " expects a string");
        }
        if (side < 0) {
            return text.stripLeading();
        }
        if (side > 0) {
            return text.stripTrailing();
        }
        return text.strip();
    }

    private Object replace(Call call) {
        if (call.args().size() != 3) {
            throw new SeaException("REPLACE expects three arguments");
        }
        Object value = eval(call.args().get(0));
        Object from = eval(call.args().get(1));
        Object to = eval(call.args().get(2));
        if (value == null || from == null || to == null) {
            return null;
        }
        if (!(value instanceof String text) || !(from instanceof String needle) || !(to instanceof String replacement)) {
            throw new SeaException("REPLACE expects strings");
        }
        if (needle.isEmpty()) {
            return text;
        }
        return text.replace(needle, replacement);
    }

    private Object position(Call call) {
        if (call.args().size() != 2) {
            throw new SeaException("POSITION expects two arguments");
        }
        Object needleValue = eval(call.args().get(0));
        Object haystackValue = eval(call.args().get(1));
        if (needleValue == null || haystackValue == null) {
            return null;
        }
        if (!(needleValue instanceof String needle) || !(haystackValue instanceof String haystack)) {
            throw new SeaException("POSITION expects strings");
        }
        int at = haystack.indexOf(needle);
        return at < 0 ? 0L : (long) at + 1;
    }

    private Object abs(Call call) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.abs();
        }
        if (value instanceof Double number) {
            return Math.abs(number);
        }
        if (value instanceof Float number) {
            return Math.abs(number);
        }
        if (value instanceof Long number) {
            return Math.absExact(number);
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return Math.abs(((Number) value).intValue());
        }
        throw new SeaException("ABS expects a number");
    }

    private Object round(Call call) {
        if (call.args().size() != 1 && call.args().size() != 2) {
            throw new SeaException("ROUND expects one or two arguments");
        }
        Object value = eval(call.args().get(0));
        if (value == null) {
            return null;
        }
        int scale = 0;
        if (call.args().size() == 2) {
            Object scaleValue = eval(call.args().get(1));
            if (scaleValue == null) {
                return null;
            }
            scale = Values.toInt(scaleValue);
        }
        if (!(value instanceof Number)) {
            throw new SeaException("ROUND expects a number");
        }
        BigDecimal rounded = Values.asDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        if (value instanceof Double || value instanceof Float) {
            return rounded.doubleValue();
        }
        if (value instanceof BigDecimal) {
            return rounded;
        }
        if (scale > 0) {
            return rounded;
        }
        long whole = rounded.longValueExact();
        if (value instanceof Long) {
            return whole;
        }
        return Math.toIntExact(whole);
    }

    private Object ceil(Call call, boolean up) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return value;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.setScale(0, up ? RoundingMode.CEILING : RoundingMode.FLOOR);
        }
        if (value instanceof Double || value instanceof Float) {
            double number = Values.toDouble(value);
            return up ? Math.ceil(number) : Math.floor(number);
        }
        throw new SeaException(call.name() + " expects a number");
    }

    private Object sign(Call call) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number)) {
            throw new SeaException("SIGN expects a number");
        }
        int cmp = Values.asDecimal(value).signum();
        return cmp;
    }

    private Object mod(Call call) {
        if (call.args().size() != 2) {
            throw new SeaException("MOD expects two arguments");
        }
        Object left = eval(call.args().get(0));
        Object right = eval(call.args().get(1));
        if (left == null || right == null) {
            return null;
        }
        return modulo(left, right);
    }

    private Object extract(Call call) {
        if (call.args().size() != 2) {
            throw new SeaException("EXTRACT expects a part and a value");
        }
        Object partValue = eval(call.args().get(0));
        Object value = eval(call.args().get(1));
        if (partValue == null || value == null) {
            return null;
        }
        String part = datePart(partValue);
        return switch (part) {
            case "YEAR" -> dateTime(value, part).getYear();
            case "MONTH" -> dateTime(value, part).getMonthValue();
            case "DAY" -> dateTime(value, part).getDayOfMonth();
            case "HOUR" -> clock(value, part).getHour();
            case "MINUTE" -> clock(value, part).getMinute();
            case "SECOND" -> clock(value, part).getSecond();
            default -> throw new SeaException("unknown date part: " + part);
        };
    }

    private Object dateAdd(Call call) {
        if (call.args().size() != 3) {
            throw new SeaException("DATEADD expects a part, an amount, and a value");
        }
        Object partValue = eval(call.args().get(0));
        Object amountValue = eval(call.args().get(1));
        Object value = eval(call.args().get(2));
        if (partValue == null || amountValue == null || value == null) {
            return null;
        }
        String part = datePart(partValue);
        long amount = Values.toLong(amountValue);
        if (value instanceof LocalDate date) {
            return switch (part) {
                case "YEAR" -> date.plusYears(amount);
                case "MONTH" -> date.plusMonths(amount);
                case "DAY" -> date.plusDays(amount);
                case "HOUR" -> date.atStartOfDay().plusHours(amount);
                case "MINUTE" -> date.atStartOfDay().plusMinutes(amount);
                case "SECOND" -> date.atStartOfDay().plusSeconds(amount);
                default -> throw new SeaException("unknown date part: " + part);
            };
        }
        if (value instanceof LocalDateTime timestamp) {
            return switch (part) {
                case "YEAR" -> timestamp.plusYears(amount);
                case "MONTH" -> timestamp.plusMonths(amount);
                case "DAY" -> timestamp.plusDays(amount);
                case "HOUR" -> timestamp.plusHours(amount);
                case "MINUTE" -> timestamp.plusMinutes(amount);
                case "SECOND" -> timestamp.plusSeconds(amount);
                default -> throw new SeaException("unknown date part: " + part);
            };
        }
        if (value instanceof LocalTime time) {
            return switch (part) {
                case "HOUR" -> time.plusHours(amount);
                case "MINUTE" -> time.plusMinutes(amount);
                case "SECOND" -> time.plusSeconds(amount);
                default -> throw new SeaException("DATEADD " + part + " expects a date or timestamp");
            };
        }
        throw new SeaException("DATEADD expects a date, time, or timestamp");
    }

    private Object dateDiff(Call call) {
        if (call.args().size() != 3) {
            throw new SeaException("DATEDIFF expects a part and two values");
        }
        Object partValue = eval(call.args().get(0));
        Object start = eval(call.args().get(1));
        Object end = eval(call.args().get(2));
        if (partValue == null || start == null || end == null) {
            return null;
        }
        String part = datePart(partValue);
        if (start instanceof LocalTime || end instanceof LocalTime) {
            if (!(start instanceof LocalTime from) || !(end instanceof LocalTime to)) {
                throw new SeaException("DATEDIFF values do not match");
            }
            return switch (part) {
                case "HOUR" -> java.time.temporal.ChronoUnit.HOURS.between(from, to);
                case "MINUTE" -> java.time.temporal.ChronoUnit.MINUTES.between(from, to);
                case "SECOND" -> java.time.temporal.ChronoUnit.SECONDS.between(from, to);
                default -> throw new SeaException("DATEDIFF " + part + " expects a date or timestamp");
            };
        }
        LocalDateTime from = dateTime(start, part);
        LocalDateTime to = dateTime(end, part);
        return switch (part) {
            case "YEAR" -> java.time.temporal.ChronoUnit.YEARS.between(from.toLocalDate(), to.toLocalDate());
            case "MONTH" -> java.time.temporal.ChronoUnit.MONTHS.between(from.toLocalDate(), to.toLocalDate());
            case "DAY" -> java.time.temporal.ChronoUnit.DAYS.between(from.toLocalDate(), to.toLocalDate());
            case "HOUR" -> java.time.temporal.ChronoUnit.HOURS.between(from, to);
            case "MINUTE" -> java.time.temporal.ChronoUnit.MINUTES.between(from, to);
            case "SECOND" -> java.time.temporal.ChronoUnit.SECONDS.between(from, to);
            default -> throw new SeaException("unknown date part: " + part);
        };
    }

    private static String datePart(Object value) {
        if (value instanceof String text) {
            return text.toUpperCase(Locale.ROOT);
        }
        throw new SeaException("unknown date part");
    }

    private static LocalDateTime dateTime(Object value, String part) {
        if (value instanceof LocalDateTime timestamp) {
            return timestamp;
        }
        if (value instanceof LocalDate date) {
            return date.atStartOfDay();
        }
        throw new SeaException("EXTRACT " + part + " expects a date or timestamp");
    }

    private static LocalTime clock(Object value, String part) {
        if (value instanceof LocalTime time) {
            return time;
        }
        if (value instanceof LocalDateTime timestamp) {
            return timestamp.toLocalTime();
        }
        if (value instanceof LocalDate) {
            return LocalTime.MIDNIGHT;
        }
        throw new SeaException("EXTRACT " + part + " expects a date, time, or timestamp");
    }

    private Object changeCase(Call call, boolean upper) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new SeaException(call.name() + " expects a string");
        }
        return upper ? text.toUpperCase(Locale.ROOT) : text.toLowerCase(Locale.ROOT);
    }

    private Object length(Call call) {
        Object value = oneArg(call);
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            return (long) text.length();
        }
        if (value instanceof byte[] bytes) {
            return (long) bytes.length;
        }
        throw new SeaException("LENGTH expects a string or BLOB");
    }

    private Object coalesce(Call call) {
        if (call.args().isEmpty()) {
            throw new SeaException("COALESCE requires at least one argument");
        }
        for (Expr arg : call.args()) {
            Object value = eval(arg);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private Object oneArg(Call call) {
        if (call.args().size() != 1 || call.star()) {
            throw new SeaException(call.name() + " expects one argument");
        }
        return eval(call.args().get(0));
    }

    void bindFunctions(Functions functions) {
        this.functions = functions;
    }

    private Object delegate(Call call) {
        if (functions == null) {
            throw new SeaException("unknown function: " + call.name());
        }
        List<Object> values = new ArrayList<>();
        for (Expr arg : call.args()) {
            values.add(eval(arg));
        }
        return functions.call(call.name(), values);
    }

    private Object jsonExtract(Call call) {
        if (call.args().size() != 2) {
            throw new SeaException("JSON_EXTRACT expects a document and a path");
        }
        Object document = eval(call.args().get(0));
        Object path = eval(call.args().get(1));
        if (document == null || path == null) {
            return null;
        }
        return JsonDoc.extract(String.valueOf(document), String.valueOf(path));
    }

    private Object jsonType(Call call) {
        if (call.args().size() != 1) {
            throw new SeaException("JSON_TYPE expects one argument");
        }
        Object document = eval(call.args().get(0));
        if (document == null) {
            return null;
        }
        return JsonDoc.typeName(String.valueOf(document));
    }

    private Object shift(Object base, SqlInterval interval, int sign) {
        long millis = interval.millis * sign;
        if (base instanceof LocalDate date) {
            return date.atStartOfDay().plus(millis, java.time.temporal.ChronoUnit.MILLIS);
        }
        if (base instanceof LocalDateTime time) {
            return time.plus(millis, java.time.temporal.ChronoUnit.MILLIS);
        }
        if (base instanceof java.time.OffsetDateTime time) {
            return time.plus(millis, java.time.temporal.ChronoUnit.MILLIS);
        }
        throw new SeaException("INTERVAL requires DATE, TIMESTAMP, or TIMESTAMPTZ");
    }

    private Object add(Object left, Object right) {
        if (left instanceof SqlInterval interval) {
            return shift(right, interval, 1);
        }
        if (right instanceof SqlInterval interval) {
            return shift(left, interval, 1);
        }
        if (floating(left, right)) {
            return Values.toDouble(left) + Values.toDouble(right);
        }
        if (decimal(left, right)) {
            return Values.asDecimal(left).add(Values.asDecimal(right));
        }
        return exactSum(left, right);
    }

    private Object subtract(Object left, Object right) {
        if (right instanceof SqlInterval interval) {
            return shift(left, interval, -1);
        }
        if (floating(left, right)) {
            return Values.toDouble(left) - Values.toDouble(right);
        }
        if (decimal(left, right)) {
            return Values.asDecimal(left).subtract(Values.asDecimal(right));
        }
        return exactDifference(left, right);
    }

    private Object multiply(Object left, Object right) {
        if (floating(left, right)) {
            return Values.toDouble(left) * Values.toDouble(right);
        }
        if (decimal(left, right)) {
            return Values.asDecimal(left).multiply(Values.asDecimal(right));
        }
        return exactProduct(left, right);
    }

    private Object divide(Object left, Object right) {
        if (decimal(left, right) && !(left instanceof Double) && !(right instanceof Double)
                && !(left instanceof Float) && !(right instanceof Float)) {
            BigDecimal divisor = Values.asDecimal(right);
            if (divisor.signum() == 0) {
                throw new SeaException("division by zero");
            }
            return Values.asDecimal(left).divide(divisor, 8, RoundingMode.HALF_UP).stripTrailingZeros();
        }
        if (integral(left) && integral(right)) {
            if (wideInteger(left, right)) {
                long divisor = Values.toLong(right);
                if (divisor == 0) {
                    throw new SeaException("division by zero");
                }
                return Values.toLong(left) / divisor;
            }
            int divisor = Values.toInt(right);
            if (divisor == 0) {
                throw new SeaException("division by zero");
            }
            return Values.toInt(left) / divisor;
        }
        double divisor = Values.toDouble(right);
        if (divisor == 0) {
            throw new SeaException("division by zero");
        }
        return Values.toDouble(left) / divisor;
    }

    private Object modulo(Object left, Object right) {
        if (!integral(left) || !integral(right)) {
            throw new SeaException("modulo requires integers");
        }
        if (wideInteger(left, right)) {
            long divisor = Values.toLong(right);
            if (divisor == 0) {
                throw new SeaException("division by zero");
            }
            return Values.toLong(left) % divisor;
        }
        int divisor = Values.toInt(right);
        if (divisor == 0) {
            throw new SeaException("division by zero");
        }
        return Values.toInt(left) % divisor;
    }

    private static boolean wideInteger(Object left, Object right) {
        return left instanceof Long || right instanceof Long;
    }

    private static Object exactSum(Object left, Object right) {
        if (wideInteger(left, right)) {
            return Math.addExact(Values.toLong(left), Values.toLong(right));
        }
        return Math.addExact(Values.toInt(left), Values.toInt(right));
    }

    private static Object exactDifference(Object left, Object right) {
        if (wideInteger(left, right)) {
            return Math.subtractExact(Values.toLong(left), Values.toLong(right));
        }
        return Math.subtractExact(Values.toInt(left), Values.toInt(right));
    }

    private static Object exactProduct(Object left, Object right) {
        if (wideInteger(left, right)) {
            return Math.multiplyExact(Values.toLong(left), Values.toLong(right));
        }
        return Math.multiplyExact(Values.toInt(left), Values.toInt(right));
    }

    private static Object number(Object value, String operator) {
        if (integral(value) || value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
            return value;
        }
        throw new SeaException(operator + " requires a number");
    }

    private static boolean floating(Object left, Object right) {
        return left instanceof Double || right instanceof Double || left instanceof Float || right instanceof Float;
    }

    private static boolean decimal(Object left, Object right) {
        return left instanceof BigDecimal || right instanceof BigDecimal;
    }

    private static boolean integral(Object value) {
        return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte;
    }

    private static String text(Object value) {
        if (value instanceof byte[]) {
            throw new SeaException("cannot concatenate BLOB; convert it first");
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return String.valueOf(value);
    }

    private Object joinedColumn(ColumnRef column) {
        Frame frame = Frame.find(frames, column.qualifier(), column.name());
        if (joinedRow == null) {
            throw new SeaException("column " + column.name() + " cannot be used in this statement");
        }
        int index = frame.schema.indexOf(column.name());
        return joinedRow[frame.offset + index];
    }

    static final class Frame {
        final Schema schema;
        final String name;
        final String written;
        final String namespace;
        final String alias;
        final int offset;
        final int width;

        Frame(Schema schema, String name, String written, String namespace, String alias, int offset, int width) {
            this.schema = schema;
            this.name = name;
            this.written = written;
            this.namespace = namespace;
            this.alias = alias;
            this.offset = offset;
            this.width = width;
        }

        Frame shift(int delta) {
            return new Frame(schema, name, written, namespace, alias, offset + delta, width);
        }

        Frame withAlias(String alias) {
            return new Frame(schema, name, written, namespace, alias, offset, width);
        }

        boolean matches(String qualifier) {
            if (alias != null && Names.same(alias, qualifier)) {
                return true;
            }
            if (Names.same(name, qualifier)) {
                return true;
            }
            if (written != null && Names.same(written, qualifier)) {
                return true;
            }
            return namespace != null && !namespace.isEmpty() && Names.same(namespace + "." + name, qualifier);
        }

        static Frame find(List<Frame> frames, String qualifier, String name) {
            Frame found = null;
            boolean qualifierHit = false;
            for (Frame frame : frames) {
                if (qualifier != null && !frame.matches(qualifier)) {
                    continue;
                }
                if (qualifier != null) {
                    qualifierHit = true;
                }
                if (frame.schema.indexOf(name) < 0) {
                    continue;
                }
                if (found != null) {
                    throw new SeaException("ambiguous column: " + name);
                }
                found = frame;
            }
            if (found == null) {
                if (qualifier != null && !qualifierHit) {
                    throw new SeaException("unknown qualifier: " + qualifier);
                }
                throw new SeaException("column not found: " + name);
            }
            return found;
        }

        static String identify(List<Frame> frames, ColumnRef column) {
            Frame frame = find(frames, column.qualifier(), column.name());
            return frames.indexOf(frame) + ":" + frame.schema.indexOf(column.name());
        }

        static boolean hasColumn(List<Frame> frames, String name) {
            for (Frame frame : frames) {
                if (frame.schema.indexOf(name) >= 0) {
                    return true;
                }
            }
            return false;
        }

        static DataType type(List<Frame> frames, ColumnRef column) {
            Frame frame = find(frames, column.qualifier(), column.name());
            return frame.schema.column(column.name()).type;
        }
    }
}
