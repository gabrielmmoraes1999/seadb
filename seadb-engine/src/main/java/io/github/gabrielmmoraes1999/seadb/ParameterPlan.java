package io.github.gabrielmmoraes1999.seadb;

import java.util.ArrayList;
import java.util.List;

/**
 * Assigns each {@code ?} the column or procedure argument it writes or compares.
 * A placeholder with no target stays {@link ParameterMeta#unknown()}.
 */
final class ParameterPlan {

    interface Catalog {
        Schema table(String name);

        List<ParamDef> inputs(String procedure);
    }

    private final Catalog catalog;
    private final List<ParameterMeta> out;
    private final List<Schema> relations = new ArrayList<>();

    private ParameterPlan(Catalog catalog, List<ParameterMeta> out) {
        this.catalog = catalog;
        this.out = out;
    }

    static List<ParameterMeta> describe(Stmt statement, Catalog catalog) {
        List<ParameterMeta> found = new ArrayList<>();
        new ParameterPlan(catalog, found).statement(statement);
        return found;
    }

    private void statement(Stmt statement) {
        if (statement instanceof Insert insert) {
            insert(insert);
        } else if (statement instanceof Update update) {
            update(update);
        } else if (statement instanceof Delete delete) {
            relations(delete.table());
            params(delete.where(), null);
        } else if (statement instanceof ExecuteProcedure call) {
            arguments(call.name(), call.arguments());
        } else if (statement instanceof Query query) {
            query(query);
        }
    }

    private void insert(Insert insert) {
        Schema schema = relations(insert.table());
        List<Column> targets = new ArrayList<>();
        if (schema != null) {
            if (insert.columns() == null || insert.columns().isEmpty()) {
                targets.addAll(schema.columns);
            } else {
                for (String name : insert.columns()) {
                    targets.add(column(schema, name));
                }
            }
        }
        if (insert.rows() != null) {
            for (List<Expr> row : insert.rows()) {
                for (int i = 0; i < row.size(); i++) {
                    params(row.get(i), i < targets.size() ? targets.get(i) : null);
                }
            }
        }
        if (insert.source() != null) {
            query(insert.source());
        }
    }

    private void update(Update update) {
        Schema schema = relations(update.table());
        if (update.sets() != null) {
            for (Assignment assignment : update.sets()) {
                Column column = schema == null ? null : column(schema, assignment.column());
                params(assignment.expr(), column);
            }
        }
        params(update.where(), null);
    }

    private Schema relations(String table) {
        relations.clear();
        if (table == null) {
            return null;
        }
        Schema schema = catalog.table(table);
        if (schema != null) {
            relations.add(schema);
        }
        return schema;
    }

    private void arguments(String procedure, List<Expr> arguments) {
        List<ParamDef> inputs = procedure == null ? null : catalog.inputs(procedure);
        if (arguments == null) {
            return;
        }
        for (int i = 0; i < arguments.size(); i++) {
            Column target = null;
            if (inputs != null && i < inputs.size()) {
                ParamDef input = inputs.get(i);
                target = new Column(input.name(), input.type(), input.length(), input.scale(), false, false, 0L, false, null);
            }
            params(arguments.get(i), target);
        }
    }

    private void query(Query query) {
        List<Schema> saved = List.copyOf(relations);
        try {
            switch (query) {
                case Select select -> select(select);
                case SelectProcedure procedure -> arguments(procedure.procedure(), procedure.arguments());
                case UnionQuery union -> {
                    query(union.left());
                    query(union.right());
                }
                case WithQuery with -> {
                    for (Cte cte : with.ctes()) {
                        query(cte.query());
                    }
                    query(with.body());
                }
            }
        } finally {
            relations.clear();
            relations.addAll(saved);
        }
    }

    private void select(Select select) {
        relations.clear();
        if (select.table() != null) {
            Schema schema = catalog.table(select.table());
            if (schema != null) {
                relations.add(schema);
            }
        }
        if (select.joins() != null) {
            for (Join join : select.joins()) {
                if (join.relation() == null) {
                    continue;
                }
                Schema schema = catalog.table(join.relation());
                if (schema != null) {
                    relations.add(schema);
                }
            }
        }
        if (select.items() != null) {
            for (SelectItem item : select.items()) {
                params(item.expr(), null);
            }
        }
        params(select.where(), null);
        params(select.having(), null);
        if (select.groupBy() != null) {
            for (Expr expr : select.groupBy()) {
                params(expr, null);
            }
        }
        if (select.orderBy() != null) {
            for (OrderItem item : select.orderBy()) {
                params(item.expr(), null);
            }
        }
        if (select.joins() != null) {
            for (Join join : select.joins()) {
                params(join.on(), null);
            }
        }
    }

    private void params(Expr expr, Column target) {
        if (expr == null) {
            return;
        }
        switch (expr) {
            case Param ignored -> out.add(of(target));
            case Literal ignored -> {
            }
            case ColumnRef ignored -> {
            }
            case QualifiedStar ignored -> {
            }
            case VarRef ignored -> {
            }
            case NextValue ignored -> {
            }
            case ContextRef ignored -> {
            }
            case Unary unary -> params(unary.expr(), target);
            case Binary binary -> binary(binary, target);
            case IsNull isNull -> params(isNull.expr(), target);
            case Like like -> {
                Column column = target != null ? target : columnSide(like.expr());
                params(like.expr(), column);
                params(like.pattern(), column);
                params(like.escape(), column);
            }
            case InList in -> {
                Column column = target != null ? target : columnSide(in.expr());
                params(in.expr(), column);
                for (Expr value : in.values()) {
                    params(value, column);
                }
            }
            case Between between -> {
                Column column = target != null ? target : columnSide(between.expr());
                params(between.expr(), column);
                params(between.low(), column);
                params(between.high(), column);
            }
            case Call call -> {
                for (Expr arg : call.args()) {
                    params(arg, target);
                }
            }
            case WindowExpr window -> {
                params(window.call(), target);
                for (Expr part : window.partition()) {
                    params(part, target);
                }
                for (OrderItem item : window.orderBy()) {
                    params(item.expr(), target);
                }
            }
            case CaseExpr searched -> {
                params(searched.operand(), target);
                for (WhenBranch branch : searched.branches()) {
                    params(branch.when(), target);
                    params(branch.then(), target);
                }
                params(searched.otherwise(), target);
            }
            case CastExpr cast -> {
                Column casted = target != null ? target
                        : new Column("?", cast.type(), cast.length(), cast.scale(), false, false, 0L, false, null);
                params(cast.expr(), casted);
            }
            case ScalarSubquery subquery -> query(subquery.query());
            case InSubquery in -> {
                Column column = target != null ? target : columnSide(in.expr());
                params(in.expr(), column);
                query(in.query());
            }
            case ExistsExpr exists -> query(exists.query());
        }
    }

    private void binary(Binary binary, Column target) {
        if (binary.op() == BinaryOp.AND || binary.op() == BinaryOp.OR) {
            params(binary.left(), target);
            params(binary.right(), target);
            return;
        }
        Column column = target;
        if (column == null && comparison(binary.op())) {
            column = columnSide(binary.left());
            if (column == null) {
                column = columnSide(binary.right());
            }
        }
        params(binary.left(), column);
        params(binary.right(), column);
    }

    private static boolean comparison(BinaryOp op) {
        return op == BinaryOp.EQ || op == BinaryOp.NE || op == BinaryOp.LT
                || op == BinaryOp.GT || op == BinaryOp.LE || op == BinaryOp.GE;
    }

    private Column columnSide(Expr expr) {
        if (!(expr instanceof ColumnRef ref)) {
            return null;
        }
        Column found = null;
        for (Schema schema : relations) {
            if (ref.qualifier() != null && !Names.same(schema.name, ref.qualifier())) {
                continue;
            }
            Column column = column(schema, ref.name());
            if (column == null) {
                continue;
            }
            if (found != null) {
                return null;
            }
            found = column;
        }
        return found;
    }

    private static Column column(Schema schema, String name) {
        int index = schema.indexOf(name);
        return index < 0 ? null : schema.columns.get(index);
    }

    private static ParameterMeta of(Column column) {
        if (column == null) {
            return ParameterMeta.unknown();
        }
        return new ParameterMeta(column.type, column.length, column.scale, !column.notNull);
    }
}
