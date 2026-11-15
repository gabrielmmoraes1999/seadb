package io.github.gabrielmmoraes1999.seadb;

import java.util.List;

record ParsedSql(List<Stmt> statements, int parameters) {
}

sealed interface Stmt permits CreateTable, CreateIndex, DropTable, DropIndex, AddColumn, DropColumn, AlterColumnPosition,
        AlterColumnType, AlterColumnNull, AlterColumnDefault, RenameColumn, RenameTable, AddCheck, AddPrimaryKey, AddForeignKey,
        AddUnique, DropConstraint, DropPrimaryKey, DropForeignKey, DropUnique, Insert, Update, Delete, Truncate, Merge, Select,
        SelectProcedure, UnionQuery, WithQuery, Grant, Revoke, GrantExecute, RevokeExecute, BeginTx, CommitTx, RollbackTx,
        CreateGenerator, DropGenerator, SetGenerator, CreateProcedure, DropProcedure, ExecuteProcedure, CreateFunction, DropFunction,
        ExecuteBlock, CreateTrigger, DropTrigger, AlterTrigger, CreateView, DropView, CreateDomain, DropDomain,
        CreateRole, DropRole, GrantRole, RevokeRole, DropConnection, CreateSchema, DropSchema, SetSchema,
        Savepoint, RollbackTo, ReleaseSavepoint, SetIsolation, Analyze, Explain, Comment {
}

sealed interface Query permits Select, SelectProcedure, UnionQuery, WithQuery {
}

record CreateView(boolean orReplace, boolean ifNotExists, String name, List<String> columns, String source) implements Stmt {
}

record CreateRole(boolean ifNotExists, String name) implements Stmt {
}

record CreateSchema(boolean ifNotExists, String name) implements Stmt {
}

record DropSchema(boolean ifExists, String name, boolean cascade) implements Stmt {
}

record SetSchema(String name) implements Stmt {
}

record DropRole(boolean ifExists, String name) implements Stmt {
}

record GrantRole(String role, String user, boolean adminOption) implements Stmt {
}

record RevokeRole(String role, String user, boolean adminOptionOnly) implements Stmt {
}

record DropConnection(Expr attachment) implements Stmt {
}

record DropView(boolean ifExists, String name) implements Stmt {
}

record CheckSpec(String name, String column, String expression) {
}

record CreateTable(boolean ifNotExists, String name, List<ColDef> columns, List<String> primaryKey, List<UniqueClause> uniques,
                   String primaryConstraint, String primaryIndex, boolean primaryDescending, List<ForeignClause> foreignKeys,
                   List<CheckSpec> checks) implements Stmt {
}

record UniqueClause(String constraint, List<String> columns) {
}

record ForeignClause(String constraint, List<String> columns, String index, boolean descending,
                     String refTable, List<String> refColumns, ReferentialAction onUpdate, ReferentialAction onDelete) {
}

/** {@code keys} are column names or expressions. {@code predicate} is null without WHERE. */
/** {@code table} is null when every user table is analyzed. */
record Analyze(String table) implements Stmt {
}

record Explain(Stmt statement) implements Stmt {
}

/** {@code text} null removes the comment. */
record Comment(String kind, String schema, String name, String column, String text) implements Stmt {
}

record CreateIndex(boolean unique, boolean ifNotExists, String name, String table, List<String> keys,
                   List<String> include, String predicate, boolean descending) implements Stmt {
}

record DropTable(boolean ifExists, String name) implements Stmt {
}

record DropIndex(boolean ifExists, String name, String table) implements Stmt {
}

record AddColumn(String table, ColDef column) implements Stmt {
}

record DropColumn(String table, String column) implements Stmt {
}

/** Moves a column. {@code position} starts at 1. */
record AlterColumnPosition(String table, String column, int position) implements Stmt {
}

record AlterColumnType(String table, String column, DataType type, int length, int scale) implements Stmt {
}

record AlterColumnNull(String table, String column, boolean notNull) implements Stmt {
}

/** {@code expression} is null when the default is removed. */
record AlterColumnDefault(String table, String column, String expression) implements Stmt {
}

record RenameColumn(String table, String column, String name) implements Stmt {
}

record RenameTable(String table, String name) implements Stmt {
}

/** {@code column} is null for a table check. {@code expression} is the source text. */
record AddCheck(String table, String name, String column, String expression) implements Stmt {
}

record CreateDomain(String name, DataType type, int length, int scale, String defaultSource, String checkSource) implements Stmt {
}

record DropDomain(boolean ifExists, String name) implements Stmt {
}

record GrantExecute(String kind, String name, String user) implements Stmt {
}

record RevokeExecute(String kind, String name, String user) implements Stmt {
}

record ExecuteBlock(List<ParamDef> inputs, List<Expr> defaults, List<ParamDef> outputs, String source) implements Stmt {
}

record AddPrimaryKey(String table, List<String> columns, String constraint, String index, boolean descending) implements Stmt {
}

record AddForeignKey(String table, List<String> columns, String constraint, String index, boolean descending,
                     String refTable, List<String> refColumns, ReferentialAction onUpdate, ReferentialAction onDelete) implements Stmt {
}

record DropConstraint(String table, String name) implements Stmt {
}

record DropPrimaryKey(String table) implements Stmt {
}

record DropForeignKey(String table, String name) implements Stmt {
}

record AddUnique(String table, List<String> columns, String constraint) implements Stmt {
}

record DropUnique(String table, String name) implements Stmt {
}

record Insert(String table, List<String> columns, List<List<Expr>> rows, Query source,
              List<String> conflict, List<Assignment> conflictSets, boolean doNothing,
              boolean returningAll, List<SelectItem> returning) implements Stmt {
}

record Update(String table, List<Assignment> sets, Expr where, boolean returningAll, List<SelectItem> returning) implements Stmt {
}

record Delete(String table, Expr where, boolean returningAll, List<SelectItem> returning) implements Stmt {
}

record Truncate(String table) implements Stmt {
}

record Merge(String target, String targetAlias, String source, String sourceAlias, Query sourceQuery, Expr on,
             Expr matchedWhen, List<Assignment> sets,
             Expr notMatchedWhen, List<String> insertColumns, List<Expr> insertValues) implements Stmt {
}

record Grant(List<String> privileges, String table, String user, boolean grantOption, List<String> columns) implements Stmt {
}

record Revoke(List<String> privileges, String table, String user, List<String> columns) implements Stmt {
}

record Select(boolean distinct, boolean star, List<SelectItem> items, String table, String alias, List<Join> joins,
              Expr where, List<Expr> groupBy, Expr having, List<OrderItem> orderBy, Long limit, Long offset) implements Stmt, Query {
}

enum JoinKind {
    INNER, LEFT, RIGHT, FULL, CROSS
}

record Join(JoinKind kind, String relation, String alias, Expr on, boolean natural, List<String> using) {
    Join(JoinKind kind, String relation, String alias, Expr on) {
        this(kind, relation, alias, on, false, List.of());
    }
}

enum SetOp {
    UNION, EXCEPT, INTERSECT
}

record UnionQuery(Query left, SetOp op, boolean all, Query right, List<OrderItem> orderBy, Long limit, Long offset) implements Stmt, Query {
}

record Cte(String name, List<String> columns, Query query) {
}

record WithQuery(List<Cte> ctes, Query body) implements Stmt, Query {
}

record BeginTx() implements Stmt {
}

record CommitTx() implements Stmt {
}

record RollbackTx() implements Stmt {
}

record Savepoint(String name) implements Stmt {
}

record RollbackTo(String name) implements Stmt {
}

record ReleaseSavepoint(String name) implements Stmt {
}

record SetIsolation(int level) implements Stmt {
}

record ColDef(String name, DataType type, int length, int scale, boolean notNull, boolean primaryKey, boolean unique,
               boolean autoIncrement, boolean hasDefault, Object defaultValue, String defaultSource, String generated,
               String checkName, String checkSource, int collation, String domain) {
}

record Assignment(String column, Expr expr) {
}

record SelectItem(Expr expr, String alias, String label) {
}

record OrderItem(Expr expr, boolean descending) {
}

enum UnaryOp {
    NOT, NEGATE, PLUS
}

enum BinaryOp {
    OR, AND, EQ, NE, LT, GT, LE, GE, CONCAT, ADD, SUB, MUL, DIV, MOD
}

sealed interface Expr permits Literal, Param, ColumnRef, QualifiedStar, VarRef, NextValue, ContextRef, Unary, Binary, IsNull, Like, InList, Between, Call,
        WindowExpr, CaseExpr, CastExpr, ScalarSubquery, InSubquery, ExistsExpr {
}

record ScalarSubquery(Query query) implements Expr {
}

record InSubquery(Expr expr, Query query, boolean negated) implements Expr {
}

record ExistsExpr(Query query) implements Expr {
}

record WhenBranch(Expr when, Expr then) {
}

/** {@code operand} is null for a searched CASE. {@code otherwise} is null when ELSE is absent. */
record CaseExpr(Expr operand, List<WhenBranch> branches, Expr otherwise) implements Expr {
}

record CastExpr(Expr expr, DataType type, int length, int scale) implements Expr {
}

record Literal(Object value) implements Expr {
}

record Param(int index) implements Expr {
}

record ColumnRef(String qualifier, String name) implements Expr {
}

/** Every column of one relation in the SELECT list, as in {@code cr.*}. */
record QualifiedStar(String qualifier) implements Expr {
}

record Unary(UnaryOp op, Expr expr) implements Expr {
}

record Binary(BinaryOp op, Expr left, Expr right) implements Expr {
}

record IsNull(Expr expr, boolean negated) implements Expr {
}

record Like(Expr expr, Expr pattern, Expr escape, boolean negated) implements Expr {
}

record InList(Expr expr, List<Expr> values, boolean negated) implements Expr {
}

record Between(Expr expr, Expr low, Expr high, boolean negated) implements Expr {
}

record Call(String name, boolean distinct, boolean star, List<Expr> args) implements Expr {
}

/** {@code frame} is the optional {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}. */
record WindowExpr(Call call, List<Expr> partition, List<OrderItem> orderBy, boolean frame, List<Object> constants) implements Expr {
}

record VarRef(String name) implements Expr {
}

record NextValue(String name) implements Expr {
}

record ContextRef(String name, Integer precision) implements Expr {
}

record ParamDef(String name, DataType type, int length, int scale) {
}

record CreateGenerator(String name, Long start, Long increment, boolean sequence) implements Stmt {
}

record DropGenerator(boolean ifExists, String name) implements Stmt {
}

record SetGenerator(String name, Expr value, boolean restart) implements Stmt {
}

record CreateProcedure(boolean orReplace, String name, List<ParamDef> inputs, List<ParamDef> outputs, String source) implements Stmt {
}

record DropProcedure(boolean ifExists, String name) implements Stmt {
}

record CreateFunction(boolean orReplace, String name, List<ParamDef> inputs, DataType type, int length, int scale, String source) implements Stmt {
}

record DropFunction(boolean ifExists, String name) implements Stmt {
}

record ExecuteProcedure(String name, List<Expr> arguments, List<String> returning) implements Stmt {
}

record CreateTrigger(String name, String table, boolean inactive, String phase, List<String> events, int position, String source) implements Stmt {
}

record DropTrigger(boolean ifExists, String name) implements Stmt {
}

record AlterTrigger(String name, boolean inactive) implements Stmt {
}

record SelectProcedure(boolean distinct, boolean star, List<SelectItem> items, String procedure, List<Expr> arguments, String alias,
                       Expr where, List<Expr> groupBy, Expr having, List<OrderItem> orderBy, Long limit, Long offset) implements Stmt, Query {
}

record CursorDef(String name, Query query) {
}

record Routine(List<ParamDef> locals, List<CursorDef> cursors, Psql body) {
}

sealed interface Psql permits PsqlBlock, PsqlAssign, PsqlIf, PsqlWhile, PsqlExit, PsqlSuspend, PsqlException, PsqlReturn,
        PsqlExecuteStatement, PsqlInsert, PsqlUpdate, PsqlDelete, PsqlExecute, PsqlSelectInto, PsqlOpen, PsqlFetch, PsqlClose, PsqlForSelect {
}

record PsqlHandler(boolean any, String variable, Psql body) {
}

record PsqlBlock(List<Psql> statements, List<PsqlHandler> handlers) implements Psql {
    PsqlBlock(List<Psql> statements) {
        this(statements, List.of());
    }
}

record PsqlReturn(Expr value) implements Psql {
}

record PsqlExecuteStatement(Expr sql, List<Expr> arguments, List<String> targets) implements Psql {
}

record PsqlAssign(String qualifier, String name, Expr value) implements Psql {
}

record PsqlIf(Expr condition, Psql thenBranch, Psql elseBranch) implements Psql {
}

record PsqlWhile(Expr condition, Psql body) implements Psql {
}

record PsqlExit() implements Psql {
}

record PsqlSuspend() implements Psql {
}

record PsqlException(Expr message) implements Psql {
}

record PsqlInsert(Insert statement) implements Psql {
}

record PsqlUpdate(Update statement) implements Psql {
}

record PsqlDelete(Delete statement) implements Psql {
}

record PsqlExecute(ExecuteProcedure call) implements Psql {
}

record PsqlSelectInto(List<Expr> expressions, List<String> targets, String table, Expr where) implements Psql {
}

record PsqlOpen(String cursor) implements Psql {
}

record PsqlFetch(String cursor, List<String> targets) implements Psql {
}

record PsqlClose(String cursor) implements Psql {
}

record PsqlForSelect(Query query, List<String> targets, Psql body) implements Psql {
}
