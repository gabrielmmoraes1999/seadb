package io.github.gabrielmmoraes1999.seadb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

final class Engine implements AutoCloseable {

    final Path file;
    private final DatabaseFile storage;
    private final Map<String, TableStore> tables = new LinkedHashMap<>();
    private final Map<String, ParsedSql> parsedSql = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, ParsedSql> eldest) {
            return size() > 32;
        }
    };
    private final Map<Long, Session> attachments = new HashMap<>();
    private boolean inTransaction;
    private Session transactionSession;
    private long nextTransaction = 1;
    private Long transactionId;
    private Session installed;
    private final Map<Session, OpenTxn> transactions = new IdentityHashMap<>();
    private final Map<TableStore, Map<Long, Session>> rowLocks = new IdentityHashMap<>();
    private final Map<Session, Session> waiting = new IdentityHashMap<>();
    private Set<String> joinKeys = Set.of();
    private int contextDepth;
    private LocalDateTime contextNow;
    private Session connected;
    private boolean closed;
    private boolean bypass;
    private boolean suppressCapture;
    private int captureDepth;
    private int requestedFetch;
    private int requestedTimeout;
    private long nextCursor;
    private final java.util.IdentityHashMap<Session, java.util.Map<Long, PendingQuery>> openCursors = new java.util.IdentityHashMap<>();
    private final List<TransactionLog.Applied> pending = new ArrayList<>();
    private Session actor;
    private Object[] params = new Object[0];
    private RoutineScope scope;
    private Map<String, OpenCursor> cursors;
    private RoutineFrame frame;
    private boolean psqlHalt;
    private int statementDepth;
    private int procedureDepth;
    private int routineDepth;
    private int viewDepth;
    private int subqueryDepth;
    private Evaluator correlation;
    private SqlResult pendingWrite;
    private int writeDepth;
    private Map<String, Held> cteScope;
    private Set<String> cteDefining = new HashSet<>();
    private List<Evaluator.Frame> inferFrames;
    private Map<TableStore, TableStore.Snapshot> statementSnapshots;
    private final List<Runnable> renameUndo = new ArrayList<>();
    private Object lastReturn;
    private final Map<Projected, Map<Expr, Object>> orderWindows = new IdentityHashMap<>();
    private IdentityHashMap<TableStore, Set<Long>> deletingRows;
    private IdentityHashMap<TableStore, Set<Long>> rewritingRows;
    private String pinnedSchema;
    private CheckpointPolicy policy = CheckpointPolicy.defaults();
    private volatile boolean stopping;
    private Thread checkpointThread;
    private Thread pageSync;
    private volatile boolean forcing;
    private boolean snapshotLsn;
    private boolean checkpointRunning;
    private long lastCheckpointNanos = System.nanoTime();
    private Long commitMarker;
    private Long rollbackMarker;
    private boolean catalogDirty;

    private Engine(DatabaseFile storage) {
        this.storage = storage;
        this.file = storage.path();
    }

    static void create(Path file, DatabaseOptions options) {
        DatabaseFile storage = DatabaseFile.create(file, options.pageSize());
        Engine engine = new Engine(storage);
        try {
            engine.loadTables();
            engine.ensureSystem(options);
        } catch (RuntimeException exception) {
            engine.close();
            try {
                Files.deleteIfExists(storage.path());
                Files.deleteIfExists(TransactionLog.beside(storage.path()));
            } catch (IOException ignored) {
                // the incomplete file is left for the next attempt
            }
            throw exception;
        }
        engine.close();
    }

    static Engine open(Path file) {
        return open(file, null);
    }

    static Engine open(Path file, Cache cache) {
        return open(file, cache, true);
    }

    /** Opens for {@code -a}. No checkpoint is written until the log is accepted. */
    static Engine openApplying(Path file, Cache cache) {
        return open(file, cache, false);
    }

    static Engine openApplying(Path file, Cache cache, Consumer<String> messages) {
        return open(file, cache, false, messages);
    }

    private static Engine open(Path file, Cache cache, boolean session) {
        return open(file, cache, session, null);
    }

    private static Engine open(Path file, Cache cache, boolean session, Consumer<String> messages) {
        DatabaseFile storage = session ? DatabaseFile.open(file, cache) : DatabaseFile.openApplying(file, cache, messages);
        Engine engine = new Engine(storage);
        try {
            engine.loadTables();
            if (!session) {
                if (messages != null) messages.accept("Undoing uncommitted transactions");
                engine.recoverUncommitted();
                if (messages != null) messages.accept("Recovering committed transactions");
                engine.recoverCommittedSql();
            }
            engine.ensureSystem(DatabaseOptions.defaults());
            if (session) {
                engine.closeStaleConnections();
                engine.storage.beginSession();
                engine.startCheckpoints();
            }
            return engine;
        } catch (RuntimeException exception) {
            engine.close();
            throw exception;
        }
    }

    long checkpointLsn() {
        return storage.checkpointLsn();
    }

    long logLength() {
        return storage.logLength();
    }

    void applyExternalLog(Path logFile) {
        applyExternalLog(logFile, null);
    }

    /**
     * {@code applied} receives the percent complete, from 1 to 100, when that percent changes.
     * It is called after the transactions for that percent are in the database.
     */
    void applyExternalLog(Path logFile, IntConsumer applied) {
        DatabaseFile.ApplyPlan plan = storage.planApply(logFile);
        if (plan.idle()) {
            storage.finishApply();
            return;
        }
        Session replay = new Session("DBA", true, 0);
        boolean previousBypass = bypass;
        boolean previousSuppress = suppressCapture;
        bypass = true;
        suppressCapture = false;
        try {
            int total = plan.batches.size();
            int done = 0;
            int shown = 0;
            for (TransactionLog.Batch batch : plan.batches) {
                for (TransactionLog.Applied statement : batch.statements) {
                    replay.schema(statement.schema);
                    execute(replay, statement.sql, statement.params);
                }
                storage.noteApplied(batch.end);
                done++;
                int percent = (int) Math.min(100L, done * 100L / total);
                if (applied != null && percent > shown) {
                    shown = percent;
                    applied.accept(percent);
                }
            }
            storage.completeApply(plan);
            storage.finishApply();
        } finally {
            bypass = previousBypass;
            suppressCapture = previousSuppress;
        }
    }

    Session login(String user, String password) {
        return login(user, password, null, null);
    }

    Session login(String user, String password, String role) {
        return login(user, password, role, null);
    }

    Session login(String user, String password, String role, RemoteClient remote) {
        checkOpen();
        Account account = authenticate(user, password);
        String active = normalizeRole(role);
        if (active != null) {
            if (findRole(active) == null) {
                throw new SeaException("role not found: " + active);
            }
            if (!account.admin && !member(active, account.user)) {
                throw new SeaException("user " + account.user + " does not have role " + active);
            }
        }
        int limit = configInt("Max.Connections");
        if (limit > 0 && attachments.size() >= limit) {
            throw new SeaException("connection limit reached", "08001");
        }
        return attach(account, active, remote);
    }

    void detach(Session session) {
        checkOpen();
        if (session == null || session.closed()) {
            return;
        }
        closeCursors(session);
        if (transactions.containsKey(session)) {
            rollbackSession(session);
        }
        if (storage.leaveForRecovery()) {
            attachments.remove(session.attachmentId);
            session.markClosed();
            return;
        }
        maintainConnections(
                "DELETE FROM SEA$CONNECTIONS WHERE SEA$ATTACHMENT_ID = ?",
                session.attachmentId);
        attachments.remove(session.attachmentId);
        session.markClosed();
    }

    void discardTransaction() {
        checkOpen();
        discardAll();
        if (inTransaction) {
            rollback();
        }
    }

    List<ParameterMeta> describeParameters(Session session, String sql) {
        if (sql == null || sql.isBlank()) {
            throw new SeaException("SQL is required");
        }
        Session previousActor = actor;
        Session previousConnected = connected;
        actor = session;
        connected = session;
        try {
            ParsedSql parsed = SqlParser.parse(sql);
            ParameterPlan.Catalog catalog = new ParameterPlan.Catalog() {
                @Override
                public Schema table(String name) {
                    TableStore found = findTable(name);
                    return found == null ? null : found.schema();
                }

                @Override
                public List<ParamDef> inputs(String procedure) {
                    StoredProcedure found = findProcedure(procedure);
                    if (found == null) {
                        return null;
                    }
                    return parameters(found.schema(), found.name(), 0);
                }
            };
            List<ParameterMeta> described = new ArrayList<>();
            for (Stmt statement : parsed.statements()) {
                described.addAll(ParameterPlan.describe(statement, catalog));
            }
            while (described.size() < parsed.parameters()) {
                described.add(ParameterMeta.unknown());
            }
            return described;
        } finally {
            actor = previousActor;
            connected = previousConnected;
        }
    }

    SqlResult execute(Session session, String sql, Object[] parameters, int fetch) {
        return execute(session, sql, parameters, fetch, 0);
    }

    SqlResult execute(Session session, String sql, Object[] parameters, int fetch, int timeoutSeconds) {
        int previous = requestedFetch;
        int previousTimeout = requestedTimeout;
        requestedFetch = fetch;
        requestedTimeout = timeoutSeconds;
        try {
            return execute(session, sql, parameters);
        } finally {
            requestedFetch = previous;
            requestedTimeout = previousTimeout;
        }
    }

    void cancel(Session session) {
        if (session != null) {
            session.cancelled = true;
        }
    }

    SqlResult execute(Session session, String sql, Object[] parameters) {
        checkOpen();
        if (session != null && session.closed()) {
            throw new SeaException("session closed");
        }
        boolean outer = contextDepth == 0;
        Session previousConnected = connected;
        if (outer) {
            closeCursors(session);
            connected = session;
            contextDepth = 1;
            contextNow = null;
            armPace(session);
        }
        Session previous = actor;
        actor = session;
        if (outer && session != null && transactions.containsKey(session)) {
            activate(session);
        }
        int mark = pending.size();
        try {
            if (!suppressCapture && captureDepth == 0) {
                Object[] copy = copyParams(parameters);
                pending.add(new TransactionLog.Applied(activeSchema(), sql, copy));
            }
            captureDepth++;
            boolean userStatement = !suppressCapture && captureDepth == 1;
            try {
                SqlResult result = executeBody(sql, parameters);
                if (userStatement) {
                    audit(sql, true);
                }
                return result;
            } catch (RuntimeException exception) {
                if (userStatement) {
                    audit(sql, false);
                }
                throw exception;
            } finally {
                captureDepth--;
            }
        } catch (RuntimeException exception) {
            if (pending.size() > mark) {
                pending.subList(mark, pending.size()).clear();
            }
            throw exception;
        } finally {
            if (outer) {
                Pace.clear();
            }
            actor = previous;
            if (outer) {
                boolean openTxn = session != null && transactions.containsKey(session);
                if (openTxn && !suppressCapture) {
                    spillPending(session);
                    if (!storage.leaveForRecovery()) {
                        maybeCheckpoint();
                    }
                }
                if (openTxn) {
                    park(session);
                    inTransaction = false;
                    transactionSession = null;
                }
                transactionId = null;
                contextDepth = 0;
                contextNow = null;
                connected = previousConnected;
            }
        }
    }

    private SqlResult executeBody(String sql, Object[] parameters) {
        ParsedSql parsed = parsed(sql);
        Object[] bound = bind(parameters, parsed.parameters());
        for (int i = 0; i < parsed.statements().size() - 1; i++) {
            if (parsed.statements().get(i) instanceof Query || parsed.statements().get(i) instanceof ExecuteProcedure) {
                throw new SeaException("SELECT must be the last statement in the script");
            }
        }
        this.params = bound;
        SqlResult last = SqlResult.update(0);
        for (Stmt statement : parsed.statements()) {
            last = executeOne(statement);
        }
        return last;
    }

    /**
     * JDBC repeats the same text for every execution of a prepared statement.
     * The tree is only read after parsing.
     */
    private ParsedSql parsed(String sql) {
        ParsedSql cached = parsedSql.get(sql);
        if (cached != null) {
            return cached;
        }
        ParsedSql parsed = SqlParser.parse(sql);
        parsedSql.put(sql, parsed);
        return parsed;
    }

    boolean transactionActive() {
        checkOpen();
        return inTransaction || !transactions.isEmpty();
    }

    boolean transactionActive(Session session) {
        checkOpen();
        return session != null && transactions.containsKey(session);
    }

    List<String> tableNames() {
        checkOpen();
        return names(false);
    }

    List<String> schemaNames() {
        return schemaNames(null);
    }

    List<String> schemaNames(Session session) {
        return seeing(session, this::readSchemaNames);
    }

    private List<String> readSchemaNames() {
        checkOpen();
        TableStore table = systemTable(SystemTables.SCHEMAS);
        List<String> names = new ArrayList<>();
        if (table == null) {
            return names;
        }
        int index = table.schema().indexOf("SEA$SCHEMA_NAME");
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof String text) {
                names.add(text);
            }
        }
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    void useSchema(Session session, String name) {
        checkOpen();
        if (session == null || session.closed()) {
            throw new SeaException("session closed");
        }
        session.schema(canonicalSchema(name));
    }

    List<TableDescription> describeAllTables() {
        return describeAllTables(null);
    }

    List<TableDescription> describeAllTables(Session session) {
        return seeing(session, () -> {
            checkOpen();
            return describeStores(true);
        });
    }

    List<TableDescription> describeViews() {
        return describeViews(null);
    }

    List<TableDescription> describeViews(Session session) {
        return seeing(session, this::readViews);
    }

    private List<TableDescription> readViews() {
        checkOpen();
        List<TableDescription> descriptions = new ArrayList<>();
        for (StoredView view : allViews()) {
            Schema schema = new Schema(view.name(), viewOutputColumns(view), new int[0], List.of(), List.of());
            descriptions.add(describe(view.schema(), schema));
        }
        descriptions.sort(Comparator.comparing(TableDescription::schema, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(TableDescription::name, String.CASE_INSENSITIVE_ORDER));
        return descriptions;
    }

    private List<String> names(boolean includeSystem) {
        List<String> names = new ArrayList<>();
        for (TableStore table : tables.values()) {
            if (includeSystem || !SystemTables.isSystem(table.schema().name)) {
                names.add(Relation.catalogName(table.namespace(), table.schema().name));
            }
        }
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    List<TableDescription> describeTables() {
        return describeTables(null);
    }

    List<TableDescription> describeTables(Session session) {
        return seeing(session, () -> describeStores(false));
    }

    private List<TableDescription> describeStores(boolean includeSystem) {
        List<TableDescription> descriptions = new ArrayList<>();
        for (TableStore table : tables.values()) {
            if (!includeSystem && SystemTables.isSystem(table.schema().name)) {
                continue;
            }
            descriptions.add(describe(table.namespace(), table.schema()));
        }
        descriptions.sort(Comparator.comparing(TableDescription::schema, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(TableDescription::name, String.CASE_INSENSITIVE_ORDER));
        return descriptions;
    }

    private static TableDescription describe(String namespace, Schema schema) {
        List<TableDescription.TableColumn> columns = new ArrayList<>();
        for (int i = 0; i < schema.columns.size(); i++) {
            Column column = schema.columns.get(i);
            int position = 0;
            for (int key = 0; key < schema.primaryKey.length; key++) {
                if (schema.primaryKey[key] == i) {
                    position = key + 1;
                    break;
                }
            }
            columns.add(new TableDescription.TableColumn(
                    schema.name, column.name, column.type, column.length, column.scale,
                    !column.notNull, column.autoIncrement, position, defaultSql(column)));
        }
        List<TableDescription.TableIndex> indexes = new ArrayList<>();
        if (schema.primaryKey.length > 0) {
            indexes.add(new TableDescription.TableIndex(
                    schema.name, schema.primaryIndex, true, names(schema, schema.primaryKey), schema.primaryDescending));
        }
        for (UniqueKey unique : schema.uniques) {
            indexes.add(new TableDescription.TableIndex(
                    schema.name, unique.name, true, names(schema, unique.columns), false));
        }
        for (IndexDef index : schema.indexes) {
            indexes.add(new TableDescription.TableIndex(
                    schema.name, index.name, index.unique, names(schema, index.columns), index.descending));
        }
        List<TableDescription.ForeignKey> foreignKeys = new ArrayList<>();
        for (ForeignKey key : schema.foreignKeys) {
            foreignKeys.add(new TableDescription.ForeignKey(
                    key.name,
                    names(schema, key.columns),
                    key.referencedSchema,
                    key.referencedTable,
                    List.of(key.referencedColumns),
                    key.indexName,
                    key.descending,
                    key.onUpdate,
                    key.onDelete));
        }
        return new TableDescription(namespace, schema.name, List.copyOf(columns), List.copyOf(indexes),
                schema.primaryConstraint, schema.primaryIndex, schema.primaryDescending, foreignKeys);
    }

    private static List<String> names(Schema schema, int[] indexes) {
        List<String> names = new ArrayList<>();
        for (int index : indexes) {
            names.add(schema.columns.get(index).name);
        }
        return List.copyOf(names);
    }

    private static String defaultSql(Column column) {
        if (!column.hasDefault) {
            return null;
        }
        Object value = column.defaultValue;
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String text) {
            return "'" + text.replace("'", "''") + "'";
        }
        if (value instanceof Boolean bool) {
            return bool ? "TRUE" : "FALSE";
        }
        if (value instanceof byte[] bytes) {
            StringBuilder hex = new StringBuilder("X'");
            for (byte piece : bytes) {
                hex.append(String.format("%02X", piece & 0xFF));
            }
            return hex.append('\'').toString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime) {
            return "'" + value.toString().replace('T', ' ') + "'";
        }
        return String.valueOf(value);
    }

    List<BackupPiece> beginBackup(boolean incremental) {
        if (closed) {
            throw new SeaException("session closed");
        }
        awaitPageForce();
        if (forcing) throw new SeaException("backup interrupted while waiting for checkpoint");
        checkpoint(true);
        return storage.beginBackup(incremental);
    }

    byte[] readBackup(int index, long offset, int length) {
        if (closed) {
            throw new SeaException("session closed");
        }
        return storage.readBackup(index, offset, length);
    }

    void finishBackup() {
        if (closed) {
            throw new SeaException("session closed");
        }
        awaitPageForce();
        if (forcing) throw new SeaException("backup interrupted while waiting for checkpoint");
        storage.finishBackup(transactions.isEmpty());
    }

    void abortBackup() {
        if (closed) {
            return;
        }
        storage.abortBackup();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        stopping = true;
        if (checkpointThread != null) {
            checkpointThread.interrupt();
        }
        awaitPageForce();
        try {
            try {
                discardAll();
                if (inTransaction) {
                    rollback();
                }
                flushCommitted();
            } catch (RuntimeException ignored) {
                // closing discards the open transaction
            }
        } finally {
            closed = true;
            inTransaction = false;
            for (TableStore table : tables.values()) {
                table.close();
            }
            tables.clear();
            storage.dropDeferredCheckpoint();
            storage.close();
        }
    }

    private SqlResult executeOne(Stmt statement) {
        return dispatch(statement);
    }

    private SqlResult dispatch(Stmt statement) {
        pendingWrite = null;
        return switch (statement) {
            case BeginTx ignored -> {
                if (actor != null && transactions.containsKey(actor)) {
                    throw new SeaException("a transaction is already open");
                }
                if (actor == null && inTransaction) {
                    throw new SeaException("a transaction is already open");
                }
                OpenTxn txn = new OpenTxn(nextTransaction++);
                if (actor != null) {
                    transactions.put(actor, txn);
                }
                inTransaction = true;
                transactionSession = actor;
                transactionId = txn.id;
                installed = actor;
                yield SqlResult.update(0);
            }
            case CommitTx ignored -> {
                Session owner = transactionSession != null ? transactionSession : actor;
                OpenTxn txn = owner == null ? null : transactions.get(owner);
            if (txn == null) {
                if (!inTransaction) {
                    throw new SeaException("there is no open transaction");
                }
                flush();
                    inTransaction = false;
                    transactionSession = null;
                    transactionId = null;
                    yield SqlResult.update(0);
                }
                commitTxn(owner, txn);
                yield SqlResult.update(0);
            }
            case RollbackTx ignored -> {
                Session owner = transactionSession != null ? transactionSession : actor;
                if (owner != null && transactions.containsKey(owner)) {
                    rollbackSession(owner);
                } else if (inTransaction) {
                    rollback();
                } else {
                    throw new SeaException("there is no open transaction");
                }
                yield SqlResult.update(0);
            }
            case Savepoint savepoint -> {
                makeSavepoint(savepoint.name());
                yield SqlResult.update(0);
            }
            case RollbackTo rollbackTo -> {
                rollbackToSavepoint(rollbackTo.name());
                yield SqlResult.update(0);
            }
            case ReleaseSavepoint release -> {
                releaseSavepoint(release.name());
                yield SqlResult.update(0);
            }
            case SetIsolation set -> {
                if (actor == null) {
                    throw new SeaException("session closed");
                }
                actor.isolation = set.level();
                yield SqlResult.update(0);
            }
            case Select select -> runQuery(select);
            case SelectProcedure select -> runQuery(select);
            case UnionQuery union -> runQuery(union);
            case WithQuery with -> runQuery(with);
            case CreateGenerator generator -> {
                createGenerator(generator);
                yield SqlResult.update(0);
            }
            case DropGenerator generator -> {
                dropGenerator(generator);
                yield SqlResult.update(0);
            }
            case SetGenerator generator -> {
                setGenerator(generator);
                yield SqlResult.update(0);
            }
            case CreateProcedure procedure -> {
                createProcedure(procedure);
                yield SqlResult.update(0);
            }
            case DropProcedure procedure -> {
                dropProcedure(procedure);
                yield SqlResult.update(0);
            }
            case ExecuteProcedure procedure -> invokeProcedure(procedure, false);
            case CreateTrigger trigger -> {
                createTrigger(trigger);
                yield SqlResult.update(0);
            }
            case DropTrigger trigger -> {
                dropTrigger(trigger);
                yield SqlResult.update(0);
            }
            case AlterTrigger trigger -> {
                alterTrigger(trigger);
                yield SqlResult.update(0);
            }
            case CreateView view -> {
                createView(view);
                yield SqlResult.update(0);
            }
            case DropView view -> {
                dropView(view);
                yield SqlResult.update(0);
            }
            case CreateRole role -> {
                createRole(role);
                yield SqlResult.update(0);
            }
            case DropRole role -> {
                dropRole(role);
                yield SqlResult.update(0);
            }
            case GrantRole role -> {
                grantRole(role);
                yield SqlResult.update(0);
            }
            case RevokeRole role -> {
                revokeRole(role);
                yield SqlResult.update(0);
            }
            case DropConnection connection -> {
                dropConnection(connection);
                yield SqlResult.update(0);
            }
            case CreateSchema schema -> {
                createSchema(schema);
                yield SqlResult.update(0);
            }
            case DropSchema schema -> {
                dropSchema(schema);
                yield SqlResult.update(0);
            }
            case SetSchema schema -> {
                setSchema(schema);
                yield SqlResult.update(0);
            }
            case CreateTable create -> {
                create(create);
                yield SqlResult.update(0);
            }
            case DropTable drop -> {
                drop(drop);
                yield SqlResult.update(0);
            }
            case CreateIndex index -> {
                createIndex(index);
                yield SqlResult.update(0);
            }
            case DropIndex index -> {
                dropIndex(index);
                yield SqlResult.update(0);
            }
            case AddColumn add -> {
                guardDdl(add.table());
                rejectBorrowed(require(add.table()));
                mutating(add.table(), () -> {
                    TableStore table = require(add.table());
                    String uniqueName = null;
                    if (add.column().unique() && !add.column().primaryKey()) {
                        uniqueName = freshUnique(table.schema(), table.namespace());
                    }
                    if (add.column().primaryKey()) {
                        ensureIndexFree(table.namespace(), "PK_" + table.schema().name);
                    }
                    table.addColumn(column(add.column()), add.column().primaryKey(), uniqueName);
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case DropColumn drop -> {
                guardDdl(drop.table());
                rejectBorrowed(require(drop.table()));
                mutating(drop.table(), () -> {
                    TableStore table = require(drop.table());
                    guardReferencedColumn(table, drop.column());
                    table.dropColumn(drop.column());
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case AlterColumnPosition alter -> {
                guardDdl(alter.table());
                rejectBorrowed(require(alter.table()));
                mutating(alter.table(), () -> {
                    TableStore table = require(alter.table());
                    table.moveColumn(alter.column(), alter.position(), () -> bindViewColumns(table));
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case AlterColumnType alter -> {
                alterColumnType(alter);
                yield SqlResult.update(0);
            }
            case AlterColumnNull alter -> {
                alterColumnNull(alter);
                yield SqlResult.update(0);
            }
            case AlterColumnDefault alter -> {
                alterColumnDefault(alter);
                yield SqlResult.update(0);
            }
            case RenameColumn alter -> {
                renameColumn(alter);
                yield SqlResult.update(0);
            }
            case RenameTable alter -> {
                renameTable(alter);
                yield SqlResult.update(0);
            }
            case AddCheck alter -> {
                addCheck(alter);
                yield SqlResult.update(0);
            }
            case CreateDomain domain -> {
                createDomain(domain);
                yield SqlResult.update(0);
            }
            case DropDomain domain -> {
                dropDomain(domain);
                yield SqlResult.update(0);
            }
            case GrantExecute grant -> {
                grantExecute(grant.kind(), grant.name(), grant.user(), true);
                yield SqlResult.update(0);
            }
            case RevokeExecute revoke -> {
                grantExecute(revoke.kind(), revoke.name(), revoke.user(), false);
                yield SqlResult.update(0);
            }
            case CreateFunction function -> {
                createFunction(function);
                yield SqlResult.update(0);
            }
            case DropFunction function -> {
                dropFunction(function);
                yield SqlResult.update(0);
            }
            case ExecuteBlock block -> executeBlock(block);
            case AddPrimaryKey add -> {
                guardDdl(add.table());
                rejectBorrowed(require(add.table()));
                mutating(add.table(), () -> {
                    TableStore table = require(add.table());
                    String constraint = add.constraint() == null || add.constraint().isBlank()
                            ? "PK_" + table.schema().name : add.constraint();
                    String index = add.index() == null || add.index().isBlank() ? constraint : add.index();
                    claim(table.namespace(), table.schema(), constraint, index, true);
                    table.addPrimaryKey(resolve(table.schema().columns, add.columns(), "primary key"),
                            constraint, index, add.descending());
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case AddForeignKey add -> {
                guardDdl(add.table());
                rejectBorrowed(require(add.table()));
                mutating(add.table(), () -> {
                    TableStore table = require(add.table());
                    ForeignKey key = buildForeignKey(table.schema(), table.namespace(), add.constraint(), add.columns(),
                            add.index(), add.descending(), add.refTable(), add.refColumns(), add.onUpdate(), add.onDelete());
                    checkForeignRows(table, key);
                    table.addForeignKey(key);
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case DropConstraint drop -> {
                guardDdl(drop.table());
                rejectBorrowed(require(drop.table()));
                mutating(drop.table(), () -> {
                    TableStore table = require(drop.table());
                    if (table.schema().primaryKey.length > 0
                            && Names.same(table.schema().primaryConstraint, drop.name())) {
                        guardPrimaryReferenced(table);
                        table.dropPrimaryKey();
                        return 0;
                    }
                    boolean foreign = false;
                    for (ForeignKey key : table.schema().foreignKeys) {
                        if (Names.same(key.name, drop.name())) {
                            foreign = true;
                            break;
                        }
                    }
                    if (foreign) {
                        table.dropForeignKey(drop.name());
                        return 0;
                    }
                    for (CheckConstraint check : table.schema().checks) {
                        if (Names.same(check.name, drop.name())) {
                            List<CheckConstraint> kept = new ArrayList<>();
                            for (CheckConstraint item : table.schema().checks) {
                                if (!Names.same(item.name, drop.name())) {
                                    kept.add(item);
                                }
                            }
                            table.installSchema(table.schema().withChecks(kept));
                            return 0;
                        }
                    }
                    UniqueKey unique = uniqueNamed(table.schema(), drop.name());
                    if (unique == null) {
                        throw new SeaException("constraint not found: " + drop.name());
                    }
                    guardUniqueReferenced(table, unique);
                    table.dropUnique(unique.name);
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case DropPrimaryKey drop -> {
                guardDdl(drop.table());
                rejectBorrowed(require(drop.table()));
                mutating(drop.table(), () -> {
                    TableStore table = require(drop.table());
                    guardPrimaryReferenced(table);
                    table.dropPrimaryKey();
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case DropForeignKey drop -> {
                guardDdl(drop.table());
                rejectBorrowed(require(drop.table()));
                mutating(drop.table(), () -> {
                    require(drop.table()).dropForeignKey(drop.name());
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case AddUnique add -> {
                guardDdl(add.table());
                rejectBorrowed(require(add.table()));
                mutating(add.table(), () -> {
                    TableStore table = require(add.table());
                    table.addUnique(buildUnique(table.schema(), table.namespace(), add.constraint(), add.columns()));
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case DropUnique drop -> {
                guardDdl(drop.table());
                rejectBorrowed(require(drop.table()));
                mutating(drop.table(), () -> {
                    TableStore table = require(drop.table());
                    UniqueKey unique = uniqueNamed(table.schema(), drop.name());
                    if (unique == null) {
                        throw new SeaException("unique key not found: " + drop.name());
                    }
                    guardUniqueReferenced(table, unique);
                    table.dropUnique(unique.name);
                    return 0;
                });
                yield SqlResult.update(0);
            }
            case Insert insert -> {
                int count = insert(insert);
                yield pendingWrite == null ? SqlResult.update(count) : pendingWrite;
            }
            case Update update -> {
                int count = update(update);
                yield pendingWrite == null ? SqlResult.update(count) : pendingWrite;
            }
            case Delete delete -> {
                int count = delete(delete);
                yield pendingWrite == null ? SqlResult.update(count) : pendingWrite;
            }
            case Truncate truncate -> SqlResult.update(truncate(truncate));
            case Merge merge -> SqlResult.update(merge(merge));
            case Analyze analyze -> {
                analyze(analyze);
                yield SqlResult.update(0);
            }
            case Explain explain -> explain(explain);
            case Comment comment -> {
                comment(comment);
                yield SqlResult.update(0);
            }
            case Grant grant -> {
                grant(grant);
                yield SqlResult.update(0);
            }
            case Revoke revoke -> {
                revoke(revoke);
                yield SqlResult.update(0);
            }
        };
    }

    private void create(CreateTable statement) {
        Relation rel = relation(statement.name());
        Names.checkName(rel.name(), "table");
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("reserved table name: " + rel.name());
        }
        if (findView(statement.name()) != null) {
            throw new SeaException("view already exists: " + shown(statement.name()));
        }
        String key = rel.key();
        if (tables.containsKey(key)) {
            if (statement.ifNotExists()) {
                return;
            }
            throw new SeaException("table already exists: " + shown(statement.name()));
        }
        Schema schema = compile(rel.schema(), rel.name(), statement.columns(), statement.primaryKey(), statement.uniques());
        if (statement.primaryConstraint() != null || statement.primaryIndex() != null || statement.primaryDescending()) {
            if (schema.primaryKey.length == 0) {
                throw new SeaException("USING INDEX requires a primary key");
            }
            String constraint = statement.primaryConstraint() == null ? schema.primaryConstraint : statement.primaryConstraint();
            String index = statement.primaryIndex() == null ? constraint : statement.primaryIndex();
            claim(rel.schema(), schema, constraint, index, true);
            schema = schema.namePrimary(constraint, index, statement.primaryDescending());
        }
        schema = schema.withChecks(tableChecks(schema, statement));
        for (ForeignClause clause : statement.foreignKeys()) {
            ForeignKey foreign = buildForeignKey(schema, rel.schema(), clause.constraint(), clause.columns(),
                    clause.index(), clause.descending(), clause.refTable(), clause.refColumns(),
                    clause.onUpdate(), clause.onDelete());
            schema = schema.withIndex(new IndexDef(foreign.indexName, foreign.columns, false, foreign.descending))
                    .withForeignKey(foreign);
        }
        ensureIndexesFree(rel.schema(), schema);
        // Earlier statements of this transaction are already in the log. Their pages
        // have to be stored before the catalog checkpoint moves past that SQL.
        if (currentTxn() != null && !suppressCapture && !storage.leaveForRecovery()) {
            checkpoint(true);
        }
        createStored(schema, rel.schema());
        OpenTxn txn = currentTxn();
        if (txn != null) {
            txn.created.put(require(statement.name()), Boolean.TRUE);
        }
        recordOwner(Relation.catalogName(rel.schema(), schema.name));
        // The owner row is written after the catalog. Store it now so a commit
        // survives the process ending without closing the connection.
        flushMatching(table -> Names.same(table.schema().name, SystemTables.PRIVILEGES));
    }

    private void drop(DropTable statement) {
        Relation rel = relation(statement.name());
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("system table cannot be removed: " + shown(statement.name()));
        }
        TableStore table = findTable(statement.name());
        if (table == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("table not found: " + shown(statement.name()));
        }
        guardDdl(statement.name());
        String storedName = Relation.catalogName(table.namespace(), table.schema().name);
        ensureNoViewDepends(storedName);
        guardReferenced(table);
        rejectBorrowed(table);
        String key = rel.key();
        OpenTxn txn = currentTxn();
        if (txn != null) {
            tables.remove(key);
            txn.dropped.put(key, table);
            forgetPrivileges(storedName);
            forgetTriggers(storedName);
            return;
        }
        tables.remove(key);
        try {
            publishLayout(table.base(), table.capacity());
        } catch (RuntimeException exception) {
            tables.put(key, table);
            throw exception;
        }
        table.close();
        forgetPrivileges(storedName);
        forgetTriggers(storedName);
    }

    private void createIndex(CreateIndex statement) {
        Names.checkName(statement.name(), "index");
        guardDdl(statement.table());
        TableStore table = require(statement.table());
        rejectBorrowed(table);
        if (reservedIndexName(statement.name())) {
            throw new SeaException("reserved index name: " + statement.name());
        }
        if (indexUsed(table.namespace(), statement.name())) {
            if (statement.ifNotExists()) {
                return;
            }
            throw new SeaException("index already exists: " + statement.name());
        }
        IndexDef definition = IndexFeatures.define(table.schema(), statement);
        mutating(statement.table(), () -> {
            require(statement.table()).createIndex(definition);
            return 0;
        });
    }

    private void dropIndex(DropIndex statement) {
        if (statement.table() != null) {
            require(statement.table());
        }
        TableStore table = locateIndex(statement);
        if (table == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("index not found: " + shown(statement.name()));
        }
        String indexName = Relation.parse(statement.name()).name();
        String stored = Relation.catalogName(table.namespace(), table.schema().name);
        guardDdl(stored);
        rejectBorrowed(table);
        if (table.schema().ownsIndex(indexName) || reservedIndexName(indexName)) {
            throw new SeaException("DROP INDEX does not remove a PRIMARY KEY, FOREIGN KEY, or UNIQUE declared on the table");
        }
        mutating(stored, () -> {
            require(stored).dropIndex(indexName);
            return 0;
        });
    }

    private SqlResult explain(Explain statement) {
        List<String> steps = new ArrayList<>();
        explainStmt(statement.statement(), steps);
        if (steps.isEmpty()) {
            steps.add("scan");
        }
        List<ColumnMeta> columns = List.of(new ColumnMeta("plan", DataType.VARCHAR));
        List<Row> rows = new ArrayList<>();
        for (String step : steps) {
            rows.add(new Row(columns, new Object[]{step}));
        }
        return SqlResult.query(columns, rows);
    }

    private void explainStmt(Stmt statement, List<String> steps) {
        switch (statement) {
            case Select select -> explainSelect(select, steps);
            case Insert insert -> {
                if (insert.source() instanceof Select select) {
                    explainSelect(select, steps);
                } else {
                    steps.add("scan");
                }
            }
            case Update update -> explainTable(update.table(), update.where(), steps);
            case Delete delete -> explainTable(delete.table(), delete.where(), steps);
            default -> steps.add("scan");
        }
    }

    private void explainSelect(Select statement, List<String> steps) {
        explainTable(statement.table(), statement.where(), steps);
        for (Join join : statement.joins()) {
            steps.add(joinHash(join) ? "hash" : "nested loop");
        }
        if (!statement.orderBy().isEmpty()) {
            steps.add("sort");
        }
        boolean aggregate = !statement.groupBy().isEmpty();
        if (!aggregate) {
            for (SelectItem item : statement.items()) {
                if (Evaluator.containsAggregate(item.expr())) {
                    aggregate = true;
                    break;
                }
            }
        }
        if (aggregate) {
            steps.add("aggregation");
        }
    }

    private void explainTable(String name, Expr where, List<String> steps) {
        TableStore table = name == null ? null : findTable(name);
        if (table == null) {
            steps.add("scan");
            return;
        }
        if (where == null) {
            steps.add("scan " + table.schema().name);
            return;
        }
        Evaluator evaluator = evaluator(table, null);
        EqualsProbe probe = probeEquals(table.schema(), evaluator, where);
        String index = probe.contradiction ? null
                : table.explainIndex(probe.values, where, probe.calls);
        steps.add(index == null ? "scan " + table.schema().name : index);
    }

    private boolean joinHash(Join join) {
        if (!(join.on() instanceof Binary binary) || binary.op() != BinaryOp.EQ) {
            return false;
        }
        TableStore right = findTable(join.relation());
        if (right == null) {
            return true;
        }
        ColumnRef column = binary.left() instanceof ColumnRef left ? left
                : binary.right() instanceof ColumnRef rightRef ? rightRef : null;
        if (column == null || right.schema().indexOf(column.name()) < 0) {
            return true;
        }
        Map<Integer, Object> equals = new LinkedHashMap<>();
        equals.put(right.schema().indexOf(column.name()), 0);
        return right.explainIndex(equals, null, Map.of()) == null;
    }

    private void comment(Comment statement) {
        String schema = statement.schema() == null || statement.schema().isBlank()
                ? activeSchema() : Names.fold(statement.schema());
        String name = Names.fold(statement.name());
        String column = statement.column() == null ? "" : Names.fold(statement.column());
        ensureRemarks();
        systemExecute("""
                DELETE FROM SEA$REMARKS
                WHERE SEA$KIND = ? AND SEA$SCHEMA_NAME = ? AND SEA$OBJECT_NAME = ? AND SEA$COLUMN_NAME = ?
                """, statement.kind(), schema, name, column);
        if (statement.text() == null) {
            return;
        }
        systemExecute("""
                INSERT INTO SEA$REMARKS (
                    SEA$KIND, SEA$SCHEMA_NAME, SEA$OBJECT_NAME, SEA$COLUMN_NAME, SEA$REMARK
                ) VALUES (?, ?, ?, ?, ?)
                """, statement.kind(), schema, name, column, statement.text());
    }

    private void ensureRemarks() {
        if (systemTable(SystemTables.REMARKS) != null) {
            return;
        }
        if (currentTxn() != null && !suppressCapture && !storage.leaveForRecovery()) {
            checkpoint(true);
        }
        createStored(SystemTables.remarks(), Relation.PUBLIC);
    }

    private void analyze(Analyze statement) {
        List<TableStore> targets = new ArrayList<>();
        if (statement.table() != null) {
            TableStore table = findTable(statement.table());
            if (table == null) {
                throw new SeaException("table not found: " + shown(statement.table()));
            }
            if (SystemTables.isSystem(table.schema().name)) {
                throw new SeaException("system table cannot be analyzed: " + shown(statement.table()));
            }
            targets.add(table);
        } else {
            for (TableStore table : tables.values()) {
                if (!SystemTables.isSystem(table.schema().name)) {
                    targets.add(table);
                }
            }
        }
        ensureStatistics();
        mutating(SystemTables.STATISTICS, () -> {
            for (TableStore table : targets) {
                writeStatistics(table);
            }
            return 0;
        });
    }

    private void ensureStatistics() {
        if (systemTable(SystemTables.STATISTICS) != null) {
            return;
        }
        if (currentTxn() != null && !suppressCapture && !storage.leaveForRecovery()) {
            checkpoint(true);
        }
        createStored(SystemTables.statistics(), Relation.PUBLIC);
    }

    private void writeStatistics(TableStore table) {
        String schemaName = table.namespace() == null ? Relation.PUBLIC : table.namespace();
        String relation = table.schema().name;
        long[] rows = new long[1];
        List<TableStore.IndexCensus> census = table.census(rows);
        systemExecute(
                "DELETE FROM SEA$STATISTICS WHERE SEA$SCHEMA_NAME = ? AND SEA$RELATION_NAME = ?",
                schemaName, relation);
        systemExecute("""
                INSERT INTO SEA$STATISTICS (
                    SEA$SCHEMA_NAME, SEA$RELATION_NAME, SEA$INDEX_NAME,
                    SEA$CARDINALITY, SEA$INDEX_ENTRIES, SEA$LEADING_MIN, SEA$LEADING_MAX
                ) VALUES (?, ?, '', ?, ?, NULL, NULL)
                """, schemaName, relation, rows[0], rows[0]);
        for (TableStore.IndexCensus index : census) {
            systemExecute("""
                    INSERT INTO SEA$STATISTICS (
                        SEA$SCHEMA_NAME, SEA$RELATION_NAME, SEA$INDEX_NAME,
                        SEA$CARDINALITY, SEA$INDEX_ENTRIES, SEA$LEADING_MIN, SEA$LEADING_MAX
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, schemaName, relation, index.name(), rows[0], index.entries(), index.min(), index.max());
        }
    }

    private Map<String, TableStore.IndexStat> statistics(TableStore table) {
        TableStore store = systemTable(SystemTables.STATISTICS);
        if (store == null) {
            return Map.of();
        }
        String schemaName = table.namespace() == null ? Relation.PUBLIC : table.namespace();
        int schemaCol = store.schema().indexOf("SEA$SCHEMA_NAME");
        int relationCol = store.schema().indexOf("SEA$RELATION_NAME");
        int indexCol = store.schema().indexOf("SEA$INDEX_NAME");
        int rowsCol = store.schema().indexOf("SEA$CARDINALITY");
        int entriesCol = store.schema().indexOf("SEA$INDEX_ENTRIES");
        int minCol = store.schema().indexOf("SEA$LEADING_MIN");
        int maxCol = store.schema().indexOf("SEA$LEADING_MAX");
        Map<String, TableStore.IndexStat> found = new HashMap<>();
        for (StoredRow row : store.scan()) {
            Object schema = store.value(row, schemaCol);
            Object relation = store.value(row, relationCol);
            if (!(schema instanceof String schemaText) || !Names.same(schemaText, schemaName)) {
                continue;
            }
            if (!(relation instanceof String relationText) || !Names.same(relationText, table.schema().name)) {
                continue;
            }
            Object index = store.value(row, indexCol);
            if (index == null || String.valueOf(index).isEmpty()) {
                continue;
            }
            Object rows = store.value(row, rowsCol);
            Object entries = store.value(row, entriesCol);
            if (!(rows instanceof Number rowCount) || !(entries instanceof Number entryCount)) {
                continue;
            }
            Object min = store.value(row, minCol);
            Object max = store.value(row, maxCol);
            found.put(Names.fold(String.valueOf(index)), new TableStore.IndexStat(rowCount.longValue(), entryCount.longValue(),
                    min == null ? null : String.valueOf(min), max == null ? null : String.valueOf(max)));
        }
        return found;
    }

    /**
     * The index name is unique in the schema, so {@code DROP INDEX name} finds it there.
     * {@code ON table} still limits the search to that table.
     */
    private TableStore locateIndex(DropIndex statement) {
        Relation parsed = Relation.parse(statement.name());
        if (statement.table() != null) {
            TableStore table = findTable(statement.table());
            if (table == null) {
                return null;
            }
            if (parsed.schema() != null && !Names.same(canonicalSchema(parsed.schema()), table.namespace())) {
                return null;
            }
            return table.hasIndex(parsed.name()) ? table : null;
        }
        Relation named = relation(statement.name());
        for (TableStore table : tables.values()) {
            if (Names.same(table.namespace(), named.schema()) && table.hasIndex(named.name())) {
                return table;
            }
        }
        return null;
    }

    private static boolean reservedIndexName(String name) {
        return Names.same(name, "PRIMARY") || Names.generatedUnique(name);
    }

    private void claim(String namespace, Schema schema, String constraint, String index, boolean replacingPrimary) {
        Names.checkName(constraint, "constraint");
        Names.checkName(index, "index");
        if (nameUsed(schema, constraint, replacingPrimary)) {
            throw new SeaException("constraint already exists: " + constraint);
        }
        if (!Names.same(constraint, index) && nameUsed(schema, index, replacingPrimary)) {
            throw new SeaException("index already exists: " + index);
        }
        ensureIndexFree(namespace, index);
    }

    private boolean nameUsed(Schema schema, String name, boolean ignorePrimary) {
        if (!ignorePrimary && schema.primaryKey.length > 0
                && (Names.same(schema.primaryConstraint, name) || Names.same(schema.primaryIndex, name))) {
            return true;
        }
        for (ForeignKey key : schema.foreignKeys) {
            if (Names.same(key.name, name) || Names.same(key.indexName, name)) {
                return true;
            }
        }
        for (IndexDef definition : schema.indexes) {
            if (Names.same(definition.name, name)) {
                return true;
            }
        }
        for (UniqueKey key : schema.uniques) {
            if (Names.same(key.name, name)) {
                return true;
            }
        }
        return Names.same(name, "PRIMARY");
    }

    private String freshForeignName(Schema schema, String namespace, int[] columns) {
        String base = "FK_" + schema.name;
        if (!nameTakenInSchema(schema, namespace, base)) {
            return base;
        }
        StringBuilder detailed = new StringBuilder(base);
        for (int column : columns) {
            detailed.append('_').append(schema.columns.get(column).name);
        }
        if (!nameTakenInSchema(schema, namespace, detailed.toString())) {
            return detailed.toString();
        }
        for (int sequence = 2; sequence < 1000; sequence++) {
            String candidate = base + "_" + sequence;
            if (!nameTakenInSchema(schema, namespace, candidate)) {
                return candidate;
            }
        }
        throw new SeaException("could not name the foreign key");
    }

    private boolean nameTakenInSchema(Schema schema, String namespace, String name) {
        return nameUsed(schema, name, false) || indexUsed(namespace, name);
    }

    private boolean indexUsed(String namespace, String name) {
        for (TableStore table : tables.values()) {
            if (Names.same(table.namespace(), namespace) && table.hasIndex(name)) {
                return true;
            }
        }
        return false;
    }

    private void ensureIndexFree(String namespace, String name) {
        if (indexUsed(namespace, name)) {
            throw new SeaException("index already exists: " + name);
        }
    }

    private void ensureIndexesFree(String namespace, Schema schema) {
        if (schema.primaryKey.length > 0) {
            ensureIndexFree(namespace, schema.primaryIndex);
        }
        for (UniqueKey key : schema.uniques) {
            ensureIndexFree(namespace, key.name);
        }
        for (IndexDef index : schema.indexes) {
            ensureIndexFree(namespace, index.name);
        }
    }

    private String freshUnique(Schema schema, String namespace) {
        for (int sequence = 1; sequence < 1000; sequence++) {
            String candidate = "UNQ" + sequence + "_" + schema.name;
            if (candidate.length() > Format.MAX_NAME) {
                break;
            }
            if (!nameTakenInSchema(schema, namespace, candidate)) {
                return candidate;
            }
        }
        throw new SeaException("could not name the unique key");
    }

    private ForeignKey buildForeignKey(Schema child, String childNamespace, String constraint, List<String> columnNames,
                                       String index, boolean descending, String refTable, List<String> refColumnNames,
                                       ReferentialAction onUpdate, ReferentialAction onDelete) {
        int[] columns = resolve(child.columns, columnNames, "foreign key");
        for (int column : columns) {
            if (child.columns.get(column).type == DataType.BLOB) {
                throw new SeaException("BLOB cannot be a key");
            }
        }
        Relation parentId = relation(refTable);
        boolean self = Names.same(parentId.schema(), childNamespace) && Names.same(parentId.name(), child.name);
        String referencedSchema;
        String referencedTable;
        String[] referencedColumns;
        DataType[] referencedTypes;
        if (self) {
            int[] refs = refColumnNames.isEmpty() ? child.primaryKey : resolve(child.columns, refColumnNames, "referenced key");
            if (refs.length == 0) {
                throw new SeaException("specify the referenced columns of " + child.name);
            }
            ensureReferencedKey(child, refs);
            referencedSchema = childNamespace;
            referencedTable = child.name;
            referencedColumns = new String[refs.length];
            referencedTypes = new DataType[refs.length];
            for (int i = 0; i < refs.length; i++) {
                referencedColumns[i] = child.columns.get(refs[i]).name;
                referencedTypes[i] = child.columns.get(refs[i]).type;
            }
        } else {
            TableStore parent = require(refTable);
            int[] refs = refColumnNames.isEmpty()
                    ? parent.schema().primaryKey
                    : resolve(parent.schema().columns, refColumnNames, "referenced key");
            if (refs.length == 0) {
                throw new SeaException("specify the referenced columns of " + parent.schema().name);
            }
            ensureReferencedKey(parent.schema(), refs);
            referencedSchema = parent.namespace();
            referencedTable = parent.schema().name;
            referencedColumns = new String[refs.length];
            referencedTypes = new DataType[refs.length];
            for (int i = 0; i < refs.length; i++) {
                referencedColumns[i] = parent.schema().columns.get(refs[i]).name;
                referencedTypes[i] = parent.schema().columns.get(refs[i]).type;
            }
        }
        if (columns.length != referencedColumns.length) {
            throw new SeaException("the foreign key and the referenced key have different numbers of columns");
        }
        for (int i = 0; i < columns.length; i++) {
            if (child.columns.get(columns[i]).type != referencedTypes[i]) {
                throw new SeaException("incompatible type in foreign key: " + child.columns.get(columns[i]).name);
            }
        }
        String constraintName = constraint == null || constraint.isBlank()
                ? freshForeignName(child, childNamespace, columns) : constraint;
        String indexName = index == null || index.isBlank() ? constraintName : index;
        claim(childNamespace, child, constraintName, indexName, false);
        ReferentialAction updateAction = onUpdate == null ? ReferentialAction.NO_ACTION : onUpdate;
        ReferentialAction deleteAction = onDelete == null ? ReferentialAction.NO_ACTION : onDelete;
        checkAction(child, columns, updateAction, "ON UPDATE");
        checkAction(child, columns, deleteAction, "ON DELETE");
        return new ForeignKey(constraintName, columns, referencedSchema, referencedTable, referencedColumns, indexName, descending,
                updateAction, deleteAction);
    }

    private static void checkAction(Schema child, int[] columns, ReferentialAction action, String clause) {
        if (action == ReferentialAction.SET_NULL) {
            for (int index : columns) {
                Column column = child.columns.get(index);
                if (column.notNull) {
                    throw new SeaException(clause + " SET NULL requires a nullable column: " + column.name);
                }
            }
        }
        if (action == ReferentialAction.SET_DEFAULT) {
            for (int index : columns) {
                Column column = child.columns.get(index);
                if (!column.hasDefault) {
                    throw new SeaException(clause + " SET DEFAULT requires a default value on " + column.name);
                }
                if (column.notNull && column.defaultValue == null) {
                    throw new SeaException(clause + " SET DEFAULT cannot be NULL on " + column.name);
                }
            }
        }
    }

    private static void ensureReferencedKey(Schema schema, int[] columns) {
        if (Arrays.equals(schema.primaryKey, columns)) {
            return;
        }
        for (UniqueKey unique : schema.uniques) {
            if (Arrays.equals(unique.columns, columns)) {
                return;
            }
        }
        throw new SeaException("the foreign key must reference the primary key or a UNIQUE key");
    }

    private void checkForeignRows(TableStore child, ForeignKey key) {
        TableStore parent = parentOf(key);
        int[] parentColumns = referencedIndexes(parent.schema(), key);
        for (StoredRow row : child.scan()) {
            Object[] values = logical(child, row);
            if (anyNull(values, key.columns)) {
                continue;
            }
            if (!keyExists(parent, parentColumns, values, key.columns)) {
                throw missingForeignRow(child, key, values);
            }
        }
    }

    private void enforceOutgoing(TableStore child, Object[] values) {
        enforceOutgoingExcept(child, values, null, -1L);
    }

    private void enforceOutgoingExcept(TableStore child, Object[] values, TableStore excluded, long excludedId) {
        for (ForeignKey key : child.schema().foreignKeys) {
            if (anyNull(values, key.columns)) {
                continue;
            }
            TableStore parent = parentOf(key);
            if (!keyExists(parent, referencedIndexes(parent.schema(), key), values, key.columns, excluded, excludedId)) {
                throw missingForeignRow(child, key, values);
            }
        }
    }

    private SeaException missingForeignRow(TableStore child, ForeignKey key, Object[] values) {
        List<String> columns = new ArrayList<>();
        List<String> comparedValues = new ArrayList<>();
        for (int column : key.columns) {
            columns.add(child.schema().columns.get(column).name);
            Object value = values[column];
            comparedValues.add(value instanceof String text
                    ? "'" + text.replace("'", "''") + "'" : String.valueOf(value));
        }
        return new SeaException("foreign key " + key.name + " does not find the row in " + referencedLabel(key)
                + "\nProblematic key value is (" + String.join(", ", columns) + ") = ("
                + String.join(", ", comparedValues) + ")"
                + "\nReferenced key is " + referencedLabel(key) + " ("
                + String.join(", ", key.referencedColumns) + ") = ("
                + String.join(", ", comparedValues) + ")");
    }

    private void blockReferentialUpdate(TableStore parent, Object[] oldValues, Object[] newValues, long exceptId) {
        for (ReferenceHit hit : incoming(parent, oldValues, exceptId)) {
            if (hit.key.onUpdate.restricts() && keyChanged(oldValues, newValues, hit.parentColumns)) {
                throw new SeaException("foreign key " + hit.key.name + " in " + hit.child.schema().name
                        + " still references this row");
            }
        }
    }

    private void cascadeReferentialUpdate(TableStore parent, Object[] oldValues, Object[] newValues, long exceptId) {
        List<Planned> planned = new ArrayList<>();
        collectUpdatePlans(parent, oldValues, newValues, exceptId, planned);
        for (Planned plan : planned) {
            applyPlan(plan);
        }
    }

    private void collectUpdatePlans(TableStore parent, Object[] oldValues, Object[] newValues, long exceptId,
                                    List<Planned> planned) {
        for (ReferenceHit hit : incoming(parent, oldValues, exceptId)) {
            if (hit.key.onUpdate.restricts() || !keyChanged(oldValues, newValues, hit.parentColumns)) {
                continue;
            }
            Object[] replacement = actionValues(hit, hit.key.onUpdate, newValues);
            for (StoredRow row : hit.rows) {
                planned.add(new Planned(hit.child, row.id, hit.key.columns, replacement, null, -1L));
            }
        }
    }

    private void applyReferentialDelete(TableStore parent, Object[] values, long exceptId) {
        List<ReferenceHit> hits = incoming(parent, values, exceptId);
        for (ReferenceHit hit : hits) {
            if (hit.key.onDelete.restricts()) {
                throw new SeaException("foreign key " + hit.key.name + " in " + hit.child.schema().name
                        + " still references this row");
            }
        }
        List<Planned> planned = new ArrayList<>();
        for (ReferenceHit hit : hits) {
            if (hit.key.onDelete == ReferentialAction.CASCADE) {
                for (StoredRow row : hit.rows) {
                    deleteOne(hit.child, row.id);
                }
                continue;
            }
            if (hit.key.onDelete != ReferentialAction.SET_NULL && hit.key.onDelete != ReferentialAction.SET_DEFAULT) {
                continue;
            }
            Object[] replacement = actionValues(hit, hit.key.onDelete, values);
            for (StoredRow row : hit.rows) {
                planned.add(new Planned(hit.child, row.id, hit.key.columns, replacement, parent, exceptId));
            }
        }
        for (Planned plan : planned) {
            applyPlan(plan);
        }
    }

    private void applyPlan(Planned plan) {
        if (rewritingRows == null) {
            rewritingRows = new IdentityHashMap<>();
        }
        Set<Long> busy = rewritingRows.computeIfAbsent(plan.child, key -> new HashSet<>());
        if (!busy.add(plan.rowId)) {
            return;
        }
        try {
            StoredRow current = plan.child.row(plan.rowId);
            if (current == null) {
                return;
            }
            Object[] old = logical(plan.child, current);
            Object[] neu = old.clone();
            for (int i = 0; i < plan.columns.length; i++) {
                neu[plan.columns[i]] = plan.values[i];
            }
            if (!changed(old, neu)) {
                return;
            }
            blockReferentialUpdate(plan.child, old, neu, current.id);
            note(plan.child);
            boolean routines = !SystemTables.isSystem(plan.child.schema().name);
            if (routines) {
                fireTriggers(plan.child, "BEFORE", "UPDATE", old, neu);
                current = plan.child.row(plan.rowId);
                if (current == null) {
                    return;
                }
            }
            Cell[] cells = current.copyCells();
            for (int i = 0; i < neu.length; i++) {
                if (!sameValue(old[i], neu[i])) {
                    cells[i] = TableStore.toCell(plan.child.schema().columns.get(i), neu[i]);
                }
            }
            plan.child.update(Map.of(current.id, cells));
            enforceOutgoingExcept(plan.child, neu, plan.excluded, plan.excludedId);
            if (routines) {
                fireTriggers(plan.child, "AFTER", "UPDATE", old, neu);
            }
            cascadeReferentialUpdate(plan.child, old, neu, current.id);
        } finally {
            busy.remove(plan.rowId);
        }
    }

    private boolean deleteOne(TableStore table, long id) {
        if (deletingRows == null) {
            deletingRows = new IdentityHashMap<>();
        }
        Set<Long> busy = deletingRows.computeIfAbsent(table, key -> new HashSet<>());
        if (!busy.add(id)) {
            return false;
        }
        try {
            StoredRow row = table.row(id);
            if (row == null) {
                return false;
            }
            note(table);
            Object[] old = logical(table, row);
            boolean routines = !SystemTables.isSystem(table.schema().name);
            if (routines) {
                fireTriggers(table, "BEFORE", "DELETE", old, null);
                row = table.row(id);
                if (row == null) {
                    return false;
                }
                old = logical(table, row);
            }
            applyReferentialDelete(table, old, id);
            boolean removed = table.delete(List.of(id)) > 0;
            if (routines && removed) {
                fireTriggers(table, "AFTER", "DELETE", old, null);
            }
            return removed;
        } finally {
            busy.remove(id);
        }
    }

    private List<ReferenceHit> incoming(TableStore parent, Object[] values, long exceptId) {
        List<ReferenceHit> hits = new ArrayList<>();
        for (TableStore child : tables.values()) {
            for (ForeignKey key : child.schema().foreignKeys) {
                if (!pointsAt(key, parent)) {
                    continue;
                }
                int[] parentColumns = referencedIndexes(parent.schema(), key);
                if (anyNull(values, parentColumns)) {
                    continue;
                }
                List<StoredRow> found = referencingRows(child, key.columns, values, parentColumns, parent, exceptId);
                if (!found.isEmpty()) {
                    hits.add(new ReferenceHit(child, key, found, parentColumns));
                }
            }
        }
        return hits;
    }

    private List<StoredRow> referencingRows(TableStore child, int[] childColumns, Object[] parentValues,
                                            int[] parentColumns, TableStore parent, long exceptId) {
        Map<Integer, Object> equals = new LinkedHashMap<>();
        for (int i = 0; i < childColumns.length; i++) {
            equals.put(childColumns[i], parentValues[parentColumns[i]]);
        }
        List<StoredRow> found = child.lookup(equals);
        if (found == null) {
            found = new ArrayList<>();
            for (StoredRow row : child.scan()) {
                Object[] current = logical(child, row);
                boolean same = true;
                for (int i = 0; i < childColumns.length; i++) {
                    if (!sameValue(current[childColumns[i]], parentValues[parentColumns[i]])) {
                        same = false;
                        break;
                    }
                }
                if (same) {
                    found.add(row);
                }
            }
        } else {
            found = new ArrayList<>(found);
        }
        if (child == parent) {
            found.removeIf(row -> row.id == exceptId);
        }
        return found;
    }

    private static Object[] actionValues(ReferenceHit hit, ReferentialAction action, Object[] parentValues) {
        Object[] values = new Object[hit.key.columns.length];
        for (int i = 0; i < hit.key.columns.length; i++) {
            values[i] = switch (action) {
                case CASCADE -> copyValue(parentValues[hit.parentColumns[i]]);
                case SET_NULL -> null;
                case SET_DEFAULT -> copyValue(hit.child.schema().columns.get(hit.key.columns[i]).defaultValue);
                default -> null;
            };
        }
        return values;
    }

    private static Object copyValue(Object value) {
        return value instanceof byte[] bytes ? bytes.clone() : value;
    }

    private boolean changed(Object[] oldValues, Object[] newValues) {
        for (int i = 0; i < oldValues.length; i++) {
            if (!sameValue(oldValues[i], newValues[i])) {
                return true;
            }
        }
        return false;
    }

    private static boolean keyChanged(Object[] oldValues, Object[] newValues, int[] columns) {
        for (int column : columns) {
            if (!sameValue(oldValues[column], newValues[column])) {
                return true;
            }
        }
        return false;
    }

    private void note(TableStore table) {
        track(table);
        if (statementSnapshots != null && !Names.same(table.schema().name, SystemTables.GENERATORS)) {
            statementSnapshots.computeIfAbsent(table, key -> key.savepoint());
        }
    }

    private record ReferenceHit(TableStore child, ForeignKey key, List<StoredRow> rows, int[] parentColumns) {
    }

    private record Planned(TableStore child, long rowId, int[] columns, Object[] values,
                           TableStore excluded, long excludedId) {
    }

    private boolean referencedValuesChanged(TableStore parent, Object[] oldValues, Object[] newValues) {
        for (TableStore child : tables.values()) {
            for (ForeignKey key : child.schema().foreignKeys) {
                if (!pointsAt(key, parent)) {
                    continue;
                }
                for (String column : key.referencedColumns) {
                    int index = parent.schema().indexOf(column);
                    if (index >= 0 && !sameValue(oldValues[index], newValues[index])) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean keyExists(TableStore parent, int[] parentColumns, Object[] childValues, int[] childColumns) {
        return keyExists(parent, parentColumns, childValues, childColumns, null, -1L);
    }

    private boolean keyExists(TableStore parent, int[] parentColumns, Object[] childValues, int[] childColumns,
                              TableStore excluded, long excludedId) {
        Map<Integer, Object> equals = new LinkedHashMap<>();
        for (int i = 0; i < parentColumns.length; i++) {
            equals.put(parentColumns[i], childValues[childColumns[i]]);
        }
        List<StoredRow> found = parent.lookup(equals);
        if (found != null) {
            for (StoredRow row : found) {
                if (parent == excluded && row.id == excludedId) {
                    continue;
                }
                return true;
            }
            return false;
        }
        for (StoredRow row : parent.scan()) {
            if (parent == excluded && row.id == excludedId) {
                continue;
            }
            Object[] values = logical(parent, row);
            boolean same = true;
            for (int i = 0; i < parentColumns.length; i++) {
                if (!sameValue(values[parentColumns[i]], childValues[childColumns[i]])) {
                    same = false;
                    break;
                }
            }
            if (same) {
                return true;
            }
        }
        return false;
    }

    private void guardPrimaryReferenced(TableStore table) {
        Schema schema = table.schema();
        if (schema.primaryKey.length == 0) {
            return;
        }
        for (TableStore other : tables.values()) {
            for (ForeignKey key : other.schema().foreignKeys) {
                if (!pointsAt(key, table) || !Arrays.equals(referencedIndexes(schema, key), schema.primaryKey)) {
                    continue;
                }
                boolean covered = false;
                for (UniqueKey unique : schema.uniques) {
                    if (Arrays.equals(unique.columns, schema.primaryKey)) {
                        covered = true;
                        break;
                    }
                }
                if (!covered) {
                    throw new SeaException("primary key is in use by foreign key " + key.name
                            + " in " + other.schema().name);
                }
            }
        }
    }

    private void guardUniqueReferenced(TableStore table, UniqueKey unique) {
        Schema schema = table.schema();
        for (TableStore other : tables.values()) {
            for (ForeignKey key : other.schema().foreignKeys) {
                if (!pointsAt(key, table) || !Arrays.equals(referencedIndexes(schema, key), unique.columns)) {
                    continue;
                }
                if (coveredWithout(schema, unique)) {
                    continue;
                }
                throw new SeaException("unique key is in use by foreign key " + key.name
                        + " in " + other.schema().name);
            }
        }
    }

    private static boolean coveredWithout(Schema schema, UniqueKey dropping) {
        if (Arrays.equals(schema.primaryKey, dropping.columns)) {
            return true;
        }
        for (UniqueKey other : schema.uniques) {
            if (!Names.same(other.name, dropping.name) && Arrays.equals(other.columns, dropping.columns)) {
                return true;
            }
        }
        return false;
    }

    private static UniqueKey uniqueNamed(Schema schema, String name) {
        for (UniqueKey key : schema.uniques) {
            if (Names.same(key.name, name)) {
                return key;
            }
        }
        return null;
    }

    private UniqueKey buildUnique(Schema schema, String namespace, String constraint, List<String> columnNames) {
        int[] columns = resolve(schema.columns, columnNames, "unique key");
        for (int column : columns) {
            if (schema.columns.get(column).type == DataType.BLOB) {
                throw new SeaException("BLOB cannot be a key");
            }
        }
        String name = constraint == null || constraint.isBlank() ? freshUnique(schema, namespace) : constraint;
        claim(namespace, schema, name, name, false);
        return new UniqueKey(name, columns);
    }

    private void guardReferenced(TableStore table) {
        for (TableStore other : tables.values()) {
            if (other == table) {
                continue;
            }
            for (ForeignKey key : other.schema().foreignKeys) {
                if (pointsAt(key, table)) {
                    throw new SeaException("table is in use by foreign key " + key.name + " in " + other.schema().name);
                }
            }
        }
    }

    private void guardReferencedColumn(TableStore table, String column) {
        for (TableStore other : tables.values()) {
            if (other == table) {
                continue;
            }
            for (ForeignKey key : other.schema().foreignKeys) {
                if (!pointsAt(key, table)) {
                    continue;
                }
                for (String referenced : key.referencedColumns) {
                    if (Names.same(referenced, column)) {
                        throw new SeaException("column is in use by foreign key " + key.name + " in " + other.schema().name);
                    }
                }
            }
        }
    }

    private TableStore parentOf(ForeignKey key) {
        TableStore parent = tables.get(Relation.key(key.referencedSchema, key.referencedTable));
        if (parent == null) {
            throw new SeaException("referenced table not found: " + referencedLabel(key));
        }
        return parent;
    }

    private static int[] referencedIndexes(Schema schema, ForeignKey key) {
        int[] indexes = new int[key.referencedColumns.length];
        for (int i = 0; i < key.referencedColumns.length; i++) {
            int found = schema.indexOf(key.referencedColumns[i]);
            if (found < 0) {
                throw new SeaException("referenced column not found: " + key.referencedColumns[i]);
            }
            indexes[i] = found;
        }
        return indexes;
    }

    private static boolean pointsAt(ForeignKey key, TableStore parent) {
        return Names.same(key.referencedSchema, parent.namespace())
                && Names.same(key.referencedTable, parent.schema().name);
    }

    private static String referencedLabel(ForeignKey key) {
        if (Names.same(key.referencedSchema, Relation.PUBLIC)) {
            return key.referencedTable;
        }
        return key.referencedSchema + "." + key.referencedTable;
    }

    private static boolean anyNull(Object[] values, int[] columns) {
        for (int column : columns) {
            if (values[column] == null) {
                return true;
            }
        }
        return false;
    }

    private int insert(Insert statement) {
        StoredView instead = findView(statement.table());
        if (instead != null) {
            return insteadInsert(statement, instead);
        }
        if (statement.source() != null) {
            throw new SeaException("INSERT ... SELECT is not supported");
        }
        boolean outer = writeDepth == 0;
        writeDepth++;
        try {
        return inSql(() -> mutating(statement.table(), () -> {
            guardMutation(statement.table(), "INSERT");
            TableStore table = require(statement.table());
            Schema schema = table.schema();
            guardWritten(statement.table(), "INSERT", statement.columns(), schema);
            if (statement.conflict() != null) {
                requireConflict(schema, statement.conflict());
            }
            List<Object[]> incoming = incomingRows(statement, schema);
            int count = 0;
            boolean routines = !SystemTables.isSystem(schema.name);
            List<Object[]> returned = new ArrayList<>();
            List<Object[]> generated = new ArrayList<>();
            for (Object[] values : incoming) {
                StoredRow existing = statement.conflict() == null ? null : conflictRow(table, statement.conflict(), values);
                if (existing != null) {
                    if (statement.doNothing()) {
                        continue;
                    }
                    Object[] old = logical(table, existing);
                    Object[] neu = old.clone();
                    Evaluator evaluator = evaluator(table, null);
                    evaluator.bind(existing);
                    evaluator.bindExcluded(schema, values);
                    for (Assignment assignment : statement.conflictSets()) {
                        int index = schema.indexOf(assignment.column());
                        if (index < 0) {
                            throw new SeaException("column not found: " + assignment.column());
                        }
                        neu[index] = prepared(schema, schema.columns.get(index), evaluator.eval(assignment.expr()));
                    }
                    if (routines) {
                        fireTriggers(table, "BEFORE", "UPDATE", old, neu);
                    }
                    applyGenerated(schema, neu);
                    enforceChecks(schema, neu);
                    replaceRow(table, existing, old, neu, routines);
                    returned.add(neu);
                    count++;
                    continue;
                }
                fillDefaults(schema, values);
                if (routines) {
                    fireTriggers(table, "BEFORE", "INSERT", null, values);
                }
                applyGenerated(schema, values);
                enforceChecks(schema, values);
                long id = table.insertOne(values);
                Object[] stored = logical(table, rowById(table, id));
                count++;
                enforceOutgoing(table, stored);
                if (routines) {
                    fireTriggers(table, "AFTER", "INSERT", null, stored);
                }
                returned.add(stored);
                generated.add(stored);
            }
            if (outer) {
                pendingWrite = writeResult(table, statement.returningAll(), statement.returning(), returned, generated, count);
            }
            return count;
        }));
        } finally {
            writeDepth--;
        }
    }

    private List<Object[]> incomingRows(Insert statement, Schema schema) {
        List<Object[]> incoming = new ArrayList<>();
        if (statement.source() != null) {
            SqlResult selected = runQuery(statement.source());
            int width = statement.columns().isEmpty() ? schema.columns.size() : statement.columns().size();
            if (selected.columns().size() != width) {
                throw new SeaException("INSERT column count does not match");
            }
            for (int i = 0; i < selected.columns().size(); i++) {
                DataType expected = statement.columns().isEmpty()
                        ? schema.columns.get(i).type
                        : schema.column(statement.columns().get(i)).type;
                if (selected.columns().get(i).type() != expected) {
                    throw new SeaException("INSERT column types do not match");
                }
            }
            for (Row row : selected.rows()) {
                incoming.add(place(schema, statement.columns(), rowValues(row)));
            }
            return incoming;
        }
        for (List<Expr> expressions : statement.rows()) {
            if (statement.columns().isEmpty() && expressions.size() != schema.columns.size()) {
                throw new SeaException("value count does not match the columns of " + schema.name);
            }
            if (!statement.columns().isEmpty() && expressions.size() != statement.columns().size()) {
                throw new SeaException("value count does not match the supplied columns");
            }
            Evaluator evaluator = evaluator(null, null);
            Object[] raw = new Object[expressions.size()];
            for (int i = 0; i < expressions.size(); i++) {
                raw[i] = evaluator.eval(expressions.get(i));
            }
            incoming.add(place(schema, statement.columns(), raw));
        }
        return incoming;
    }

    private Object[] place(Schema schema, List<String> columns, Object[] raw) {
        Object[] values = new Object[schema.columns.size()];
        Arrays.fill(values, Values.MISSING);
        if (columns == null || columns.isEmpty()) {
            for (int i = 0; i < raw.length; i++) {
                if (schema.columns.get(i).generated != null) {
                    throw new SeaException("cannot insert into generated column: " + schema.columns.get(i).name);
                }
                values[i] = prepared(schema, schema.columns.get(i), raw[i]);
            }
            return values;
        }
        Set<String> used = new HashSet<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            if (!used.add(Names.fold(name))) {
                throw new SeaException("duplicate column in INSERT: " + name);
            }
            int index = schema.indexOf(name);
            if (index < 0) {
                throw new SeaException("column not found: " + name);
            }
            if (schema.columns.get(index).generated != null) {
                throw new SeaException("cannot insert into generated column: " + name);
            }
            values[index] = prepared(schema, schema.columns.get(index), raw[i]);
        }
        return values;
    }

    private void requireConflict(Schema schema, List<String> columns) {
        int[] wanted = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int index = schema.indexOf(columns.get(i));
            if (index < 0) {
                throw new SeaException("column not found: " + columns.get(i));
            }
            wanted[i] = index;
        }
        if (sameColumns(schema.primaryKey, wanted)) {
            return;
        }
        for (UniqueKey key : schema.uniques) {
            if (sameColumns(key.columns, wanted)) {
                return;
            }
        }
        throw new SeaException("ON CONFLICT target is not a unique key");
    }

    private static boolean sameColumns(int[] left, int[] right) {
        if (left.length != right.length) {
            return false;
        }
        boolean[] used = new boolean[left.length];
        for (int value : right) {
            boolean hit = false;
            for (int i = 0; i < left.length; i++) {
                if (!used[i] && left[i] == value) {
                    used[i] = true;
                    hit = true;
                    break;
                }
            }
            if (!hit) {
                return false;
            }
        }
        return true;
    }

    private StoredRow conflictRow(TableStore table, List<String> columns, Object[] values) {
        Map<Integer, Object> equals = new LinkedHashMap<>();
        Schema schema = table.schema();
        for (String name : columns) {
            int index = schema.indexOf(name);
            Object value = values[index];
            if (value == null || value == Values.MISSING) {
                return null;
            }
            equals.put(index, value);
        }
        List<StoredRow> found = table.lookup(equals);
        if (found == null) {
            found = new ArrayList<>();
            for (StoredRow row : table.scan()) {
                boolean match = true;
                for (Map.Entry<Integer, Object> entry : equals.entrySet()) {
                    Object current = table.value(row, entry.getKey());
                    if (current == null || Values.compare(current, entry.getValue()) != 0) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    found.add(row);
                }
            }
        }
        if (found.size() > 1) {
            throw new SeaException("ON CONFLICT matched more than one row");
        }
        return found.isEmpty() ? null : found.get(0);
    }

    private void replaceRow(TableStore table, StoredRow row, Object[] old, Object[] neu, boolean routines) {
        Schema schema = table.schema();
        Cell[] cells = row.copyCells();
        for (int i = 0; i < neu.length; i++) {
            if (!sameValue(old[i], neu[i])) {
                cells[i] = TableStore.toCell(schema.columns.get(i), neu[i]);
            }
        }
        Map<Long, Cell[]> replacements = new LinkedHashMap<>();
        replacements.put(row.id, cells);
        table.update(replacements);
        if (referencedValuesChanged(table, old, neu)) {
            blockReferentialUpdate(table, old, neu, row.id);
        }
        enforceOutgoing(table, neu);
        if (routines) {
            fireTriggers(table, "AFTER", "UPDATE", old, neu);
        }
    }

    private SqlResult writeResult(TableStore table, boolean all, List<SelectItem> returning, List<Object[]> rows,
                                  List<Object[]> generated, int count) {
        Schema schema = table.schema();
        List<Column> keys = new ArrayList<>();
        for (Column column : schema.columns) {
            if (column.autoIncrement) {
                keys.add(column);
            }
        }
        List<ColumnMeta> keyColumns = new ArrayList<>();
        List<Row> keyRows = new ArrayList<>();
        if (!keys.isEmpty()) {
            if (keys.size() == 1) {
                keyColumns.add(new ColumnMeta("GENERATED_KEY", keys.get(0).type));
            } else {
                for (Column column : keys) {
                    keyColumns.add(new ColumnMeta(column.name, column.type));
                }
            }
            for (Object[] row : generated) {
                Object[] values = new Object[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    values[i] = row[schema.indexOf(keys.get(i).name)];
                }
                keyRows.add(new Row(keyColumns, values));
            }
        }
        if (!all && returning == null) {
            return keyColumns.isEmpty() ? null : SqlResult.update(count, keyColumns, keyRows);
        }
        List<SelectItem> items = all ? star(schema) : returning;
        List<ColumnMeta> columns = new ArrayList<>();
        for (SelectItem item : items) {
            columns.add(new ColumnMeta(item.label(), infer(item.expr(), schema)));
        }
        Evaluator evaluator = evaluator(table, null);
        evaluator.useFrames(List.of(new Evaluator.Frame(schema, schema.name, schema.name, table.namespace(), null, 0, schema.columns.size())));
        List<Row> out = new ArrayList<>();
        for (Object[] values : rows) {
            evaluator.bindJoined(values);
            Object[] projected = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                projected[i] = materialize(evaluator.eval(items.get(i).expr()));
            }
            out.add(new Row(columns, projected));
        }
        return SqlResult.query(columns, out, keyColumns, keyRows);
    }

    private int update(Update statement) {
        StoredView instead = findView(statement.table());
        if (instead != null) {
            return insteadUpdate(statement, instead);
        }
        return inSql(() -> mutating(statement.table(), () -> {
            guardMutation(statement.table(), "UPDATE");
            TableStore table = require(statement.table());
            Schema schema = table.schema();
            List<String> written = new ArrayList<>();
            for (Assignment assignment : statement.sets()) {
                written.add(assignment.column());
            }
            guardWritten(statement.table(), "UPDATE", written, schema);
            guardColumnUse(schema, whereScope(statement.table(), "UPDATE"), List.of(), statement.where());
            Set<String> used = new HashSet<>();
            for (Assignment assignment : statement.sets()) {
                if (!used.add(Names.fold(assignment.column()))) {
                    throw new SeaException("duplicate column in UPDATE: " + assignment.column());
                }
                if (schema.indexOf(assignment.column()) < 0) {
                    throw new SeaException("column not found: " + assignment.column());
                }
                if (schema.column(assignment.column()).generated != null) {
                    throw new SeaException("cannot update generated column: " + assignment.column());
                }
            }
            Evaluator evaluator = evaluator(table, null);
            List<StoredRow> matched = match(table, evaluator, statement.where());
            lockRows(table, matched);
            Map<Long, Cell[]> replacements = new LinkedHashMap<>();
            List<Object[]> before = new ArrayList<>();
            List<Object[]> after = new ArrayList<>();
            boolean routines = !SystemTables.isSystem(schema.name);
            boolean ownDeletes = deletingRows == null;
            boolean ownRewrites = rewritingRows == null;
            if (ownDeletes) {
                deletingRows = new IdentityHashMap<>();
            }
            if (ownRewrites) {
                rewritingRows = new IdentityHashMap<>();
            }
            try {
            for (StoredRow row : matched) {
                evaluator.bind(row);
                Object[] old = logical(table, row);
                Object[] neu = old.clone();
                for (Assignment assignment : statement.sets()) {
                    int index = schema.indexOf(assignment.column());
                    Column column = schema.columns.get(index);
                    neu[index] = prepared(schema, column, evaluator.eval(assignment.expr()));
                }
                if (routines) {
                    fireTriggers(table, "BEFORE", "UPDATE", old, neu);
                }
                applyGenerated(schema, neu);
                enforceChecks(schema, neu);
                Cell[] cells = row.copyCells();
                for (int i = 0; i < neu.length; i++) {
                    if (sameValue(old[i], neu[i])) {
                        continue;
                    }
                    cells[i] = TableStore.toCell(schema.columns.get(i), neu[i]);
                }
                replacements.put(row.id, cells);
                before.add(old);
                after.add(neu);
                if (referencedValuesChanged(table, old, neu)) {
                    blockReferentialUpdate(table, old, neu, row.id);
                }
            }
            if (Names.same(schema.name, SystemTables.USERS)) {
                guardLastAdmin(table, List.of(), replacements);
            }
            table.update(replacements);
            List<Planned> planned = new ArrayList<>();
            for (int i = 0; i < matched.size(); i++) {
                collectUpdatePlans(table, before.get(i), after.get(i), matched.get(i).id, planned);
            }
            for (Planned plan : planned) {
                applyPlan(plan);
            }
            for (Object[] neu : after) {
                enforceOutgoing(table, neu);
            }
            if (routines) {
                for (int i = 0; i < matched.size(); i++) {
                    fireTriggers(table, "AFTER", "UPDATE", before.get(i), after.get(i));
                }
            }
            if (statement.returningAll() || statement.returning() != null) {
                pendingWrite = writeResult(table, statement.returningAll(), statement.returning(), after, List.of(), matched.size());
            }
            return matched.size();
            } finally {
                if (ownDeletes) {
                    deletingRows = null;
                }
                if (ownRewrites) {
                    rewritingRows = null;
                }
            }
        }));
    }

    private int delete(Delete statement) {
        StoredView instead = findView(statement.table());
        if (instead != null) {
            return insteadDelete(statement, instead);
        }
        return inSql(() -> mutating(statement.table(), () -> {
            guardMutation(statement.table(), "DELETE");
            TableStore table = require(statement.table());
            Evaluator evaluator = evaluator(table, null);
            List<StoredRow> matched = match(table, evaluator, statement.where());
            lockRows(table, matched);
            List<Long> ids = new ArrayList<>();
            List<Object[]> removed = new ArrayList<>();
            for (StoredRow row : matched) {
                ids.add(row.id);
                removed.add(logical(table, row));
            }
            if (Names.same(table.schema().name, SystemTables.USERS)) {
                guardLastAdmin(table, ids, null);
            }
            boolean ownDeletes = deletingRows == null;
            boolean ownRewrites = rewritingRows == null;
            if (ownDeletes) {
                deletingRows = new IdentityHashMap<>();
            }
            if (ownRewrites) {
                rewritingRows = new IdentityHashMap<>();
            }
            try {
                int count = 0;
                for (Long id : ids) {
                    if (table.row(id) == null) {
                        count++;
                        continue;
                    }
                    if (deleteOne(table, id)) {
                        count++;
                    }
                }
                if (statement.returningAll() || statement.returning() != null) {
                    pendingWrite = writeResult(table, statement.returningAll(), statement.returning(), removed, List.of(), count);
                }
                return count;
            } finally {
                if (ownDeletes) {
                    deletingRows = null;
                }
                if (ownRewrites) {
                    rewritingRows = null;
                }
            }
        }));
    }

    private int truncate(Truncate statement) {
        rejectView(statement.table());
        return inSql(() -> mutating(statement.table(), () -> {
            guardMutation(statement.table(), "DELETE");
            if ((actor == null || !actor.admin) && !owns(statement.table())) {
                throw new SeaException("TRUNCATE requires the owner or a DBA");
            }
            TableStore table = require(statement.table());
            String parentSchema = table.namespace();
            for (TableStore other : tables.values()) {
                if (other == table) {
                    continue;
                }
                for (ForeignKey key : other.schema().foreignKeys) {
                    if (!Names.same(key.referencedTable, table.schema().name)) {
                        continue;
                    }
                    if (key.referencedSchema != null && !key.referencedSchema.isEmpty()
                            && !Names.same(key.referencedSchema, parentSchema)) {
                        continue;
                    }
                    for (StoredRow row : other.scan()) {
                        for (int column : key.columns) {
                            if (other.value(row, column) != null) {
                                throw new SeaException("TRUNCATE blocked by child rows");
                            }
                        }
                    }
                }
            }
            return table.deleteAll();
        }));
    }

    private int merge(Merge statement) {
        rejectView(statement.target());
        return inSql(() -> mutating(statement.target(), () -> {
            guardMutation(statement.target(), "UPDATE");
            if (statement.insertValues() != null) {
                guardMutation(statement.target(), "INSERT");
            }
            TableStore target = require(statement.target());
            SqlResult source = statement.sourceQuery() != null
                    ? runQuery(statement.sourceQuery())
                    : runQuery(new Select(false, true, List.of(), statement.source(), statement.sourceAlias(),
                    List.of(), null, List.of(), null, List.of(), null, null));
            Schema targetSchema = target.schema();
            List<Column> sourceColumns = new ArrayList<>();
            for (ColumnMeta meta : source.columns()) {
                int length = meta.type() == DataType.CHAR || meta.type() == DataType.VARCHAR ? 256 : 0;
                sourceColumns.add(new Column(meta.name(), meta.type(), length, 0, false, false, 0, false, null));
            }
            String sourceName = statement.sourceAlias() != null ? statement.sourceAlias()
                    : statement.source() == null ? "source" : Relation.parse(statement.source()).name();
            Schema sourceSchema = new Schema(sourceName, sourceColumns, new int[0], List.of(), List.of());
            Evaluator evaluator = evaluator(null, null);
            evaluator.useFrames(List.of(
                    new Evaluator.Frame(targetSchema, targetSchema.name, statement.target(), target.namespace(), statement.targetAlias(),
                            0, targetSchema.columns.size()),
                    new Evaluator.Frame(sourceSchema, sourceName, statement.source(), target.namespace(), statement.sourceAlias(),
                            targetSchema.columns.size(), sourceColumns.size())));
            List<Long> targetIds = new ArrayList<>();
            for (StoredRow row : target.scan()) {
                targetIds.add(row.id);
            }
            int count = 0;
            boolean routines = !SystemTables.isSystem(targetSchema.name);
            int width = targetSchema.columns.size();
            for (Row sourceRow : source.rows()) {
                Object[] sourceValues = rowValues(sourceRow);
                List<StoredRow> hits = new ArrayList<>();
                for (Long id : targetIds) {
                    StoredRow row = target.row(id);
                    if (row == null) {
                        continue;
                    }
                    Object[] joined = new Object[width + sourceValues.length];
                    Object[] current = logical(target, row);
                    System.arraycopy(current, 0, joined, 0, width);
                    System.arraycopy(sourceValues, 0, joined, width, sourceValues.length);
                    evaluator.bindJoined(joined);
                    if (evaluator.truth(statement.on())) {
                        hits.add(row);
                    }
                }
                if (hits.size() > 1) {
                    throw new SeaException("MERGE matched more than one target row");
                }
                if (hits.size() == 1 && statement.sets() != null) {
                    StoredRow row = hits.get(0);
                    Object[] old = logical(target, row);
                    Object[] joined = new Object[width + sourceValues.length];
                    System.arraycopy(old, 0, joined, 0, width);
                    System.arraycopy(sourceValues, 0, joined, width, sourceValues.length);
                    evaluator.bindJoined(joined);
                    if (statement.matchedWhen() != null && !evaluator.truth(statement.matchedWhen())) {
                        continue;
                    }
                    Object[] neu = old.clone();
                    for (Assignment assignment : statement.sets()) {
                        int index = targetSchema.indexOf(assignment.column());
                        if (index < 0) {
                            throw new SeaException("column not found: " + assignment.column());
                        }
                        neu[index] = prepared(targetSchema, targetSchema.columns.get(index), evaluator.eval(assignment.expr()));
                    }
                    if (routines) {
                        fireTriggers(target, "BEFORE", "UPDATE", old, neu);
                    }
                    replaceRow(target, row, old, neu, routines);
                    count++;
                } else if (hits.isEmpty() && statement.insertValues() != null) {
                    Object[] joined = new Object[width + sourceValues.length];
                    System.arraycopy(sourceValues, 0, joined, width, sourceValues.length);
                    evaluator.bindJoined(joined);
                    if (statement.notMatchedWhen() != null && !evaluator.truth(statement.notMatchedWhen())) {
                        continue;
                    }
                    Object[] raw = new Object[statement.insertValues().size()];
                    for (int i = 0; i < raw.length; i++) {
                        raw[i] = evaluator.eval(statement.insertValues().get(i));
                    }
                    List<String> columns = statement.insertColumns() == null ? List.of() : statement.insertColumns();
                    Object[] values = place(targetSchema, columns, raw);
                    if (routines) {
                        fireTriggers(target, "BEFORE", "INSERT", null, values);
                    }
                    long id = target.insertOne(values);
                    Object[] stored = logical(target, rowById(target, id));
                    enforceOutgoing(target, stored);
                    if (routines) {
                        fireTriggers(target, "AFTER", "INSERT", null, stored);
                    }
                    count++;
                }
            }
            return count;
        }));
    }

    private SqlResult query(Select statement) {
        if (activeCte(statement.table()) || !statement.joins().isEmpty()) {
            return querySources(statement);
        }
        if (statement.table() == null) {
            return queryWithoutTable(statement);
        }
        if (findView(statement.table()) != null) {
            return queryView(statement);
        }
        guardAccess(statement.table(), "SELECT");
        TableStore table = require(statement.table());
        if (repeatable() && inTransaction && !SystemTables.isSystem(table.schema().name)) {
            pinRead(table);
        }
        Schema schema = table.schema();
        Set<String> allowed = columnScope(statement.table(), "SELECT");
        List<SelectItem> items = statement.star() ? star(schema, allowed)
                : expandStars(statement.items(), schema, schema.name, table.namespace(), statement.table(), statement.alias());
        guardColumnUse(schema, allowed, items, statement.where());
        if (items.isEmpty()) {
            throw new SeaException("SELECT needs at least one expression");
        }
        boolean aggregate = false;
        boolean scalar = false;
        for (SelectItem item : items) {
            if (Evaluator.containsAggregate(item.expr())) {
                aggregate = true;
            } else {
                scalar = true;
            }
        }
        if (statement.groupBy().isEmpty() && aggregate && scalar) {
            throw new SeaException("cannot mix aggregates and columns without GROUP BY");
        }
        Evaluator evaluator = evaluator(table, statement.alias());
        List<StoredRow> coveredRows = null;
        if (statement.groupBy().isEmpty() && statement.having() == null) {
            coveredRows = coverQuery(table, schema, items, statement.where(), evaluator);
        }
        final List<StoredRow> matched = coveredRows == null
                ? match(table, evaluator, statement.where()) : coveredRows;
        List<ColumnMeta> columns = new ArrayList<>();
        for (SelectItem item : items) {
            columns.add(columnMeta(item, schema, table.namespace(), schema.name));
        }
        if (!statement.groupBy().isEmpty() || statement.having() != null) {
            String tableName = schema.name;
            String namespace = table.namespace();
            String alias = statement.alias();
            return grouped(statement.star(), statement.distinct(), statement.groupBy(), statement.having(),
                    statement.orderBy(), statement.limit(), statement.offset(), items, columns, evaluator, new GroupSource() {
                @Override
                public int size() {
                    return matched.size();
                }

                @Override
                public void bind(int index) {
                    evaluator.bind(matched.get(index));
                }

                @Override
                public Projected projected(int index, Object[] values) {
                    return new Projected(matched.get(index), values, null);
                }

                @Override
                public String identify(ColumnRef column) {
                    return identifyColumn(column, schema, tableName, namespace, alias);
                }

                @Override
                public boolean hasColumn(String name) {
                    return schema.indexOf(name) >= 0;
                }
            });
        }
        if (streamable(statement, aggregate, items)) {
            return openStream(columns, matched, items, evaluator, statement.limit(), statement.offset());
        }
        List<Projected> projected = new ArrayList<>();
        if (aggregate) {
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evaluator.evalAggregate(items.get(i).expr(), matched));
            }
            projected.add(new Projected(null, values, null));
        } else {
            for (StoredRow row : matched) {
                evaluator.bind(row);
                Object[] values = new Object[items.size()];
                for (int i = 0; i < items.size(); i++) {
                    values[i] = materialize(evaluator.eval(items.get(i).expr()));
                }
                projected.add(new Projected(row, values, null));
            }
            applyWindows(projected, items, statement.orderBy(), evaluator);
            if (statement.distinct()) {
                projected = distinct(projected);
            }
            if (!statement.orderBy().isEmpty()) {
                sort(projected, statement, items, evaluator);
            }
            projected = page(projected, statement.limit(), statement.offset());
        }
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(columns, row.values));
        }
        return SqlResult.query(columns, rows);
    }

    private static final class PendingQuery {
        private final List<ColumnMeta> columns;
        private final List<StoredRow> matched;
        private final List<SelectItem> items;
        private final Evaluator evaluator;
        private int at;
        private final int end;

        private PendingQuery(List<ColumnMeta> columns, List<StoredRow> matched, List<SelectItem> items,
                             Evaluator evaluator, int at, int end) {
            this.columns = columns;
            this.matched = matched;
            this.items = items;
            this.evaluator = evaluator;
            this.at = at;
            this.end = end;
        }

        private boolean done() {
            return at >= end;
        }

        private int remaining() {
            return Math.max(0, end - at);
        }
    }

    private boolean streamable(Select statement, boolean aggregate, List<SelectItem> items) {
        if (requestedFetch <= 0 || captureDepth != 1 || aggregate || statement.distinct() || !statement.orderBy().isEmpty()) {
            return false;
        }
        for (SelectItem item : items) {
            if (item.expr() instanceof WindowExpr) {
                return false;
            }
        }
        return true;
    }

    private SqlResult openStream(List<ColumnMeta> columns, List<StoredRow> matched, List<SelectItem> items,
                                 Evaluator evaluator, Long limit, Long offset) {
        int start = offset == null ? 0 : Math.max(0, offset.intValue());
        int end = limit == null ? matched.size() : Math.min(matched.size(), start + Math.max(0, limit.intValue()));
        PendingQuery pending = new PendingQuery(columns, matched, items, evaluator, Math.min(start, matched.size()), Math.max(start, end));
        SqlResult block = pull(pending, requestedFetch);
        if (pending.done()) {
            return block;
        }
        long id = ++nextCursor;
        openCursors.computeIfAbsent(actor, key -> new HashMap<>()).put(id, pending);
        return block.withCursor(id);
    }

    private SqlResult pull(PendingQuery pending, int count) {
        int take = count <= 0 ? pending.remaining() : Math.min(count, pending.remaining());
        List<Row> rows = new ArrayList<>(Math.max(take, 0));
        for (int n = 0; n < take; n++) {
            StoredRow source = pending.matched.get(pending.at++);
            pending.evaluator.bind(source);
            Object[] values = new Object[pending.items.size()];
            for (int i = 0; i < pending.items.size(); i++) {
                values[i] = materialize(pending.evaluator.eval(pending.items.get(i).expr()));
            }
            rows.add(new Row(pending.columns, values));
        }
        return SqlResult.query(pending.columns, rows);
    }

    SqlResult fetchCursor(Session session, long id, int count) {
        java.util.Map<Long, PendingQuery> held = openCursors.get(session);
        PendingQuery pending = held == null ? null : held.get(id);
        if (pending == null) {
            throw new SeaException("cursor is not open");
        }
        SqlResult block = pull(pending, count);
        if (pending.done()) {
            held.remove(id);
            return block;
        }
        return block.withCursor(id);
    }

    private void closeCursors(Session session) {
        if (session != null) {
            openCursors.remove(session);
        }
    }

    private SqlResult queryWithoutTable(Select statement) {
        if (statement.star()) {
            throw new SeaException("SELECT * needs a table");
        }
        List<SelectItem> items = statement.items();
        for (SelectItem item : items) {
            if (item.expr() instanceof QualifiedStar star) {
                throw new SeaException("unknown qualifier: " + star.qualifier());
            }
        }
        if (items.isEmpty()) {
            throw new SeaException("SELECT needs at least one expression");
        }
        boolean aggregate = false;
        boolean scalar = false;
        for (SelectItem item : items) {
            if (Evaluator.containsAggregate(item.expr())) {
                aggregate = true;
            } else {
                scalar = true;
            }
        }
        if (statement.groupBy().isEmpty() && aggregate && scalar) {
            throw new SeaException("cannot mix aggregates and columns without GROUP BY");
        }
        Schema schema = new Schema("", List.of(), new int[0], List.of(), List.of());
        Evaluator evaluator = evaluator(null, null);
        boolean keep = statement.where() == null || evaluator.truth(statement.where());
        List<ColumnMeta> columns = new ArrayList<>();
        for (SelectItem item : items) {
            columns.add(new ColumnMeta(item.label(), infer(item.expr(), schema)));
        }
        StoredRow row = new StoredRow(0, new Cell[0], -1, 0, true);
        List<StoredRow> matched = keep ? List.of(row) : List.of();
        if (statement.having() != null) {
            return grouped(statement.star(), statement.distinct(), statement.groupBy(), statement.having(),
                    statement.orderBy(), statement.limit(), statement.offset(), items, columns, evaluator, new GroupSource() {
                @Override
                public int size() {
                    return matched.size();
                }

                @Override
                public void bind(int index) {
                    evaluator.bind(matched.get(index));
                }

                @Override
                public Projected projected(int index, Object[] values) {
                    return new Projected(matched.get(index), values, null);
                }

                @Override
                public String identify(ColumnRef column) {
                    throw new SeaException("column " + column.name() + " cannot be used in this statement");
                }

                @Override
                public boolean hasColumn(String name) {
                    return false;
                }
            });
        }
        List<Projected> projected = new ArrayList<>();
        if (aggregate) {
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evaluator.evalAggregate(items.get(i).expr(), matched));
            }
            projected.add(new Projected(null, values, null));
        } else if (keep) {
            evaluator.bind(row);
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evaluator.eval(items.get(i).expr()));
            }
            projected.add(new Projected(row, values, null));
            if (statement.distinct()) {
                projected = distinct(projected);
            }
            if (!statement.orderBy().isEmpty()) {
                sort(projected, statement, items, evaluator);
            }
            projected = page(projected, statement.limit(), statement.offset());
        }
        List<Row> rows = new ArrayList<>();
        for (Projected projectedRow : projected) {
            rows.add(new Row(columns, projectedRow.values));
        }
        return SqlResult.query(columns, rows);
    }

    private SqlResult queryView(Select statement) {
        if (viewDepth >= 16) {
            throw new SeaException("view exceeded 16 levels");
        }
        viewDepth++;
        try {
            guardAccess(statement.table(), "SELECT");
            StoredView view = findView(statement.table());
            if (view == null) {
                throw new SeaException("view not found: " + statement.table());
            }
            Session previous = actor;
            actor = ownerSession(view.owner());
            Map<String, Held> savedScope = cteScope;
            Set<String> savedDefining = cteDefining;
            cteScope = null;
            cteDefining = new HashSet<>();
            SqlResult produced;
            try {
                produced = complexView(view.source())
                        ? runQuery(SqlParser.parseViewQuery(view.source()))
                        : query(preparedView(view));
            } finally {
                actor = previous;
                cteScope = savedScope;
                cteDefining = savedDefining;
            }
            if (produced.columns().size() != view.columns().size()) {
                throw new SeaException("the view " + view.name() + " does not match the stored SELECT");
            }
            Schema schema = new Schema(view.name(), viewColumns(view, produced.columns()), new int[0], List.of(), List.of());
            return projectView(statement, schema, produced, view.schema(), view.name());
        } finally {
            viewDepth--;
        }
    }

    private SqlResult projectView(Select statement, Schema schema, SqlResult produced, String namespace, String viewName) {
        List<SelectItem> items = statement.star() ? star(schema)
                : expandStars(statement.items(), schema, viewName, namespace, statement.table(), statement.alias());
        if (items.isEmpty()) {
            throw new SeaException("SELECT needs at least one expression");
        }
        boolean aggregate = false;
        boolean scalar = false;
        for (SelectItem item : items) {
            if (Evaluator.containsAggregate(item.expr())) {
                aggregate = true;
            } else {
                scalar = true;
            }
        }
        if (statement.groupBy().isEmpty() && aggregate && scalar) {
            throw new SeaException("cannot mix aggregates and columns without GROUP BY");
        }
        Evaluator evaluator = evaluator(null, statement.alias());
        evaluator.useView(schema, namespace + "." + viewName);
        List<Object[]> matched = new ArrayList<>();
        for (Row row : produced.rows()) {
            Object[] source = new Object[produced.columns().size()];
            for (int i = 0; i < source.length; i++) {
                source[i] = row.get(i);
            }
            evaluator.bindView(source);
            if (statement.where() == null || evaluator.truth(statement.where())) {
                matched.add(source);
            }
        }
        List<ColumnMeta> columns = new ArrayList<>();
        for (SelectItem item : items) {
            columns.add(columnMeta(item, schema, namespace, viewName));
        }
        if (!statement.groupBy().isEmpty() || statement.having() != null) {
            String qualifier = namespace + "." + viewName;
            return grouped(statement.star(), statement.distinct(), statement.groupBy(), statement.having(),
                    statement.orderBy(), statement.limit(), statement.offset(), items, columns, evaluator, new GroupSource() {
                @Override
                public int size() {
                    return matched.size();
                }

                @Override
                public void bind(int index) {
                    evaluator.bindView(matched.get(index));
                }

                @Override
                public Projected projected(int index, Object[] values) {
                    return new Projected(null, values, matched.get(index));
                }

                @Override
                public String identify(ColumnRef column) {
                    return identifyView(column, schema, statement.alias(), qualifier);
                }

                @Override
                public boolean hasColumn(String name) {
                    return schema.indexOf(name) >= 0;
                }
            });
        }
        List<Projected> projected = new ArrayList<>();
        if (aggregate) {
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evaluator.evalAggregateValues(items.get(i).expr(), matched));
            }
            projected.add(new Projected(null, values, null));
        } else {
            for (Object[] source : matched) {
                evaluator.bindView(source);
                Object[] values = new Object[items.size()];
                for (int i = 0; i < items.size(); i++) {
                    values[i] = materialize(evaluator.eval(items.get(i).expr()));
                }
                projected.add(new Projected(null, values, source));
            }
            if (statement.distinct()) {
                projected = distinct(projected);
            }
            if (!statement.orderBy().isEmpty()) {
                sort(projected, statement, items, evaluator);
            }
            projected = page(projected, statement.limit(), statement.offset());
        }
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(columns, row.values));
        }
        return SqlResult.query(columns, rows);
    }

    private void sort(List<Projected> projected, Select statement, List<SelectItem> items, Evaluator evaluator) {
        sort(projected, statement.orderBy(), statement.distinct(), items, evaluator);
    }

    private void sort(List<Projected> projected, List<OrderItem> orderBy, boolean distinct, List<SelectItem> items, Evaluator evaluator) {
        projected.sort((left, right) -> {
            for (OrderItem order : orderBy) {
                Object lv = orderValue(left, order, items, distinct, evaluator);
                Object rv = orderValue(right, order, items, distinct, evaluator);
                int cmp;
                if (lv == null && rv == null) {
                    cmp = 0;
                } else if (lv == null) {
                    cmp = 1;
                } else if (rv == null) {
                    cmp = -1;
                } else {
                    cmp = Values.compare(lv, rv);
                }
                if (order.descending()) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });
    }

    private Object orderValue(Projected projected, OrderItem order, List<SelectItem> items, boolean distinct, Evaluator evaluator) {
        if (order.expr() instanceof WindowExpr) {
            Map<Expr, Object> side = orderWindows.get(projected);
            if (side != null && side.containsKey(order.expr())) {
                return side.get(order.expr());
            }
        }
        Integer index = selectIndex(order.expr(), items);
        if (index != null) {
            return projected.values[index];
        }
        if (distinct) {
            throw new SeaException("ORDER BY with DISTINCT must use columns from the SELECT list");
        }
        if (Evaluator.containsAggregate(order.expr())) {
            throw new SeaException("ORDER BY aggregate must appear in the SELECT list");
        }
        if (evaluator.joined()) {
            if (projected.source == null) {
                throw new SeaException("invalid ORDER BY");
            }
            evaluator.bindJoined(projected.source);
            return evaluator.eval(order.expr());
        }
        if (projected.row == null) {
            if (projected.source == null) {
                throw new SeaException("invalid ORDER BY");
            }
            evaluator.bindView(projected.source);
            return evaluator.eval(order.expr());
        }
        evaluator.bind(projected.row);
        return evaluator.eval(order.expr());
    }

    private Integer selectIndex(Expr expr, List<SelectItem> items) {
        Long ordinal = ordinal(expr);
        if (ordinal != null) {
            if (ordinal < 1 || ordinal > items.size()) {
                throw new SeaException("ORDER BY " + ordinal + " is outside the SELECT list");
            }
            return ordinal.intValue() - 1;
        }
        if (expr instanceof ColumnRef column && column.qualifier() == null) {
            for (int i = 0; i < items.size(); i++) {
                SelectItem item = items.get(i);
                if (Names.same(item.label(), column.name())
                        || (item.alias() != null && Names.same(item.alias(), column.name()))) {
                    return i;
                }
                if (item.expr() instanceof ColumnRef ref && ref.qualifier() == null && Names.same(ref.name(), column.name())) {
                    return i;
                }
            }
        }
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).expr().equals(expr)) {
                return i;
            }
        }
        return null;
    }

    private static Long ordinal(Expr expr) {
        if (!(expr instanceof Literal literal)) {
            return null;
        }
        if (literal.value() instanceof Integer number) {
            return number.longValue();
        }
        if (literal.value() instanceof Long number) {
            return number;
        }
        return null;
    }

    private List<Projected> distinct(List<Projected> projected) {
        List<Projected> unique = new ArrayList<>();
        Set<IndexKey> seen = new HashSet<>();
        for (Projected row : projected) {
            if (seen.add(new IndexKey(row.values))) {
                unique.add(row);
            }
        }
        return unique;
    }

    private List<Projected> page(List<Projected> projected, Long limit, Long offset) {
        long skip = offset == null ? 0 : offset;
        if (skip < 0 || (limit != null && limit < 0)) {
            throw new SeaException("LIMIT and OFFSET cannot be negative");
        }
        if (skip >= projected.size()) {
            return List.of();
        }
        int from = (int) skip;
        int to = projected.size();
        if (limit != null) {
            long end = from + limit;
            to = (int) Math.min(projected.size(), end);
        }
        return new ArrayList<>(projected.subList(from, to));
    }

    private SqlResult runQuery(Query query) {
        return switch (query) {
            case WithQuery with -> queryWith(with);
            case UnionQuery union -> queryUnion(union);
            case SelectProcedure procedure -> queryProcedure(procedure);
            case Select select -> query(select);
        };
    }

    private Object scalarSubquery(Query query, Evaluator outer) {
        SqlResult result = subquery(query, outer);
        if (result.columns().size() != 1) {
            throw new SeaException("scalar subquery must return one column");
        }
        if (result.size() > 1) {
            throw new SeaException("scalar subquery returned more than one row");
        }
        if (result.size() == 0) {
            return null;
        }
        return result.rows().get(0).get(0);
    }

    private Object inSubquery(Expr expr, Query query, boolean negated, Evaluator outer) {
        Object value = outer.eval(expr);
        SqlResult result = subquery(query, outer);
        if (result.columns().size() != 1) {
            throw new SeaException("IN subquery must return one column");
        }
        boolean found = false;
        boolean unknown = value == null;
        for (Row row : result.rows()) {
            Object item = row.get(0);
            if (item == null || value == null) {
                unknown = true;
                continue;
            }
            if (Values.compare(value, item) == 0) {
                found = true;
            }
        }
        if (found) {
            return !negated;
        }
        if (unknown) {
            return null;
        }
        return negated;
    }

    private Object existsSubquery(Query query, Evaluator outer) {
        SqlResult result = subquery(query, outer);
        return !result.rows().isEmpty();
    }

    private SqlResult subquery(Query query, Evaluator outer) {
        if (++subqueryDepth > 32) {
            subqueryDepth--;
            throw new SeaException("subquery exceeded 32 nested levels");
        }
        Evaluator previous = correlation;
        outer.link = previous;
        correlation = outer;
        try {
            return runQuery(correlate(query));
        } finally {
            outer.link = null;
            correlation = previous;
            subqueryDepth--;
        }
    }

    private Query correlate(Query query) {
        return switch (query) {
            case Select select -> correlateSelect(select);
            case SelectProcedure procedure -> correlateProcedure(procedure);
            case UnionQuery union -> new UnionQuery(correlate(union.left()), union.op(), union.all(), correlate(union.right()),
                    union.orderBy(), union.limit(), union.offset());
            case WithQuery with -> new WithQuery(correlateCtes(with.ctes()), correlate(with.body()));
        };
    }

    private List<Cte> correlateCtes(List<Cte> ctes) {
        List<Cte> copy = new ArrayList<>();
        for (Cte cte : ctes) {
            copy.add(new Cte(cte.name(), cte.columns(), correlate(cte.query())));
        }
        return copy;
    }

    private Select correlateSelect(Select select) {
        return new Select(select.distinct(), select.star(), correlateItems(select.items(), select),
                select.table(), select.alias(), correlateJoins(select.joins(), select),
                correlateExpr(select.where(), select), correlateList(select.groupBy(), select),
                correlateExpr(select.having(), select), correlateOrder(select.orderBy(), select),
                select.limit(), select.offset());
    }

    private SelectProcedure correlateProcedure(SelectProcedure procedure) {
        List<Expr> arguments = new ArrayList<>();
        for (Expr argument : procedure.arguments()) {
            arguments.add(correlateExpr(argument, procedure));
        }
        return new SelectProcedure(procedure.distinct(), procedure.star(), correlateItems(procedure.items(), procedure),
                procedure.procedure(), arguments, procedure.alias(), correlateExpr(procedure.where(), procedure),
                correlateList(procedure.groupBy(), procedure), correlateExpr(procedure.having(), procedure),
                correlateOrder(procedure.orderBy(), procedure), procedure.limit(), procedure.offset());
    }

    private List<Join> correlateJoins(List<Join> joins, Query owner) {
        List<Join> copy = new ArrayList<>();
        for (Join join : joins) {
            copy.add(new Join(join.kind(), join.relation(), join.alias(), correlateExpr(join.on(), owner),
                    join.natural(), join.using()));
        }
        return copy;
    }

    private List<SelectItem> correlateItems(List<SelectItem> items, Query owner) {
        List<SelectItem> copy = new ArrayList<>();
        for (SelectItem item : items) {
            copy.add(new SelectItem(correlateExpr(item.expr(), owner), item.alias(), item.label()));
        }
        return copy;
    }

    private List<Expr> correlateList(List<Expr> expressions, Query owner) {
        List<Expr> copy = new ArrayList<>();
        for (Expr expr : expressions) {
            copy.add(correlateExpr(expr, owner));
        }
        return copy;
    }

    private List<OrderItem> correlateOrder(List<OrderItem> order, Query owner) {
        List<OrderItem> copy = new ArrayList<>();
        for (OrderItem item : order) {
            copy.add(new OrderItem(correlateExpr(item.expr(), owner), item.descending()));
        }
        return copy;
    }

    private Expr correlateExpr(Expr expr, Query owner) {
        if (expr == null) {
            return null;
        }
        return switch (expr) {
            case ColumnRef column -> correlateColumn(column, owner);
            case Unary unary -> new Unary(unary.op(), correlateExpr(unary.expr(), owner));
            case Binary binary -> new Binary(binary.op(), correlateExpr(binary.left(), owner), correlateExpr(binary.right(), owner));
            case IsNull isNull -> new IsNull(correlateExpr(isNull.expr(), owner), isNull.negated());
            case Like like -> new Like(correlateExpr(like.expr(), owner), correlateExpr(like.pattern(), owner),
                    correlateExpr(like.escape(), owner), like.negated());
            case InList inList -> new InList(correlateExpr(inList.expr(), owner), correlateList(inList.values(), owner), inList.negated());
            case Between between -> new Between(correlateExpr(between.expr(), owner), correlateExpr(between.low(), owner),
                    correlateExpr(between.high(), owner), between.negated());
            case Call call -> new Call(call.name(), call.distinct(), call.star(), correlateList(call.args(), owner));
            case CaseExpr caseExpr -> {
                List<WhenBranch> branches = new ArrayList<>();
                for (WhenBranch branch : caseExpr.branches()) {
                    branches.add(new WhenBranch(correlateExpr(branch.when(), owner), correlateExpr(branch.then(), owner)));
                }
                yield new CaseExpr(correlateExpr(caseExpr.operand(), owner), branches, correlateExpr(caseExpr.otherwise(), owner));
            }
            case CastExpr cast -> new CastExpr(correlateExpr(cast.expr(), owner), cast.type(), cast.length(), cast.scale());
            case InSubquery inSubquery -> new InSubquery(correlateExpr(inSubquery.expr(), owner), inSubquery.query(), inSubquery.negated());
            case ScalarSubquery ignored -> expr;
            case ExistsExpr ignored -> expr;
            default -> expr;
        };
    }

    private Expr correlateColumn(ColumnRef column, Query owner) {
        if (innerColumn(owner, column)) {
            return column;
        }
        for (Evaluator cursor = correlation; cursor != null; cursor = cursor.link) {
            Object value = cursor.readOwned(column);
            if (value != Values.MISSING) {
                return new Literal(value);
            }
        }
        return column;
    }

    private boolean innerColumn(Query owner, ColumnRef column) {
        return switch (owner) {
            case Select select -> innerSelect(select, column);
            case SelectProcedure procedure -> column.qualifier() == null
                    || (procedure.alias() != null && Names.same(procedure.alias(), column.qualifier()))
                    || Names.same(procedure.procedure(), column.qualifier());
            case UnionQuery union -> innerColumn(union.left(), column) || innerColumn(union.right(), column);
            case WithQuery with -> innerColumn(with.body(), column);
        };
    }

    private boolean innerSelect(Select select, ColumnRef column) {
        if (column.qualifier() != null) {
            if (matchesRelation(select.table(), select.alias(), column.qualifier())) {
                return true;
            }
            for (Join join : select.joins()) {
                if (matchesRelation(join.relation(), join.alias(), column.qualifier())) {
                    return true;
                }
            }
            return false;
        }
        if (relationHasColumn(select.table(), column.name())) {
            return true;
        }
        for (Join join : select.joins()) {
            if (relationHasColumn(join.relation(), column.name())) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesRelation(String relation, String alias, String qualifier) {
        if (alias != null && Names.same(alias, qualifier)) {
            return true;
        }
        if (relation == null) {
            return false;
        }
        if (Names.same(relation, qualifier)) {
            return true;
        }
        return Names.same(Relation.parse(relation).name(), qualifier);
    }

    private boolean relationHasColumn(String relation, String column) {
        if (relation == null || column == null) {
            return false;
        }
        if (activeCte(relation)) {
            Held held = cteScope.get(Names.fold(Relation.parse(relation).name()));
            if (held == null) {
                return false;
            }
            for (Evaluator.Frame frame : held.frames()) {
                if (frame.schema.indexOf(column) >= 0) {
                    return true;
                }
            }
            return false;
        }
        StoredView view = findView(relation);
        if (view != null) {
            for (String name : view.columns()) {
                if (Names.same(name, column)) {
                    return true;
                }
            }
            return false;
        }
        TableStore table = findTable(relation);
        return table != null && table.schema().indexOf(column) >= 0;
    }

    private SqlResult queryWith(WithQuery with) {
        Map<String, Held> saved = cteScope;
        Map<String, Held> scope = new LinkedHashMap<>();
        if (saved != null) {
            scope.putAll(saved);
        }
        cteScope = scope;
        try {
            for (Cte cte : with.ctes()) {
                String key = Names.fold(cte.name());
                if (cteDefining.contains(key)) {
                    throw new SeaException("CTE cannot reference itself: " + cte.name());
                }
                cteDefining.add(key);
                SqlResult result;
                try {
                    result = runQuery(cte.query());
                } finally {
                    cteDefining.remove(key);
                }
                if (!cte.columns().isEmpty() && cte.columns().size() != result.columns().size()) {
                    throw new SeaException("CTE column count does not match the query");
                }
                scope.put(key, holdRows(cte.name(), null, cteColumns(cte, result), rowsOf(result)));
            }
            return runQuery(with.body());
        } finally {
            cteScope = saved;
        }
    }

    private List<ColumnMeta> cteColumns(Cte cte, SqlResult result) {
        if (cte.columns().isEmpty()) {
            return result.columns();
        }
        List<ColumnMeta> columns = new ArrayList<>();
        for (int i = 0; i < cte.columns().size(); i++) {
            ColumnMeta meta = result.columns().get(i);
            columns.add(new ColumnMeta(cte.columns().get(i), meta.type(), meta.schema(), meta.table()));
        }
        return columns;
    }

    private SqlResult queryUnion(UnionQuery union) {
        SqlResult left = runQuery(union.left());
        SqlResult right = runQuery(union.right());
        String op = union.op().name();
        if (left.columns().size() != right.columns().size()) {
            throw new SeaException(op + " column count does not match");
        }
        for (int i = 0; i < left.columns().size(); i++) {
            if (left.columns().get(i).type() != right.columns().get(i).type()) {
                throw new SeaException(op + " column types do not match");
            }
        }
        List<SelectItem> items = new ArrayList<>();
        for (ColumnMeta column : left.columns()) {
            items.add(new SelectItem(new ColumnRef(null, column.name()), null, column.name()));
        }
        List<Projected> projected = combineSets(left, right, union.op(), union.all());
        if (!union.orderBy().isEmpty()) {
            sortUnion(projected, union.orderBy(), items);
        }
        projected = page(projected, union.limit(), union.offset());
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(left.columns(), row.values));
        }
        return SqlResult.query(left.columns(), rows);
    }

    private List<Projected> combineSets(SqlResult left, SqlResult right, SetOp op, boolean all) {
        List<Projected> projected = new ArrayList<>();
        if (op == SetOp.UNION) {
            Set<IndexKey> seen = new HashSet<>();
            addUnion(projected, left, seen, all);
            addUnion(projected, right, seen, all);
            return projected;
        }
        Map<IndexKey, Integer> rightCounts = new LinkedHashMap<>();
        for (Row row : right.rows()) {
            IndexKey key = new IndexKey(rowValues(row));
            rightCounts.merge(key, 1, Integer::sum);
        }
        Set<IndexKey> seen = new HashSet<>();
        List<Object[]> leftOrder = new ArrayList<>();
        for (Row row : left.rows()) {
            leftOrder.add(rowValues(row));
        }
        if (op == SetOp.EXCEPT) {
            for (Object[] values : leftOrder) {
                IndexKey key = new IndexKey(values);
                int have = rightCounts.getOrDefault(key, 0);
                if (all) {
                    if (have > 0) {
                        rightCounts.put(key, have - 1);
                        continue;
                    }
                    projected.add(new Projected(null, values, null));
                } else if (have == 0 && seen.add(key)) {
                    projected.add(new Projected(null, values, null));
                }
            }
            return projected;
        }
        for (Object[] values : leftOrder) {
            IndexKey key = new IndexKey(values);
            int have = rightCounts.getOrDefault(key, 0);
            if (have <= 0) {
                continue;
            }
            if (all) {
                rightCounts.put(key, have - 1);
                projected.add(new Projected(null, values, null));
            } else if (seen.add(key)) {
                projected.add(new Projected(null, values, null));
            }
        }
        return projected;
    }

    private static Object[] rowValues(Row row) {
        Object[] values = new Object[row.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = row.get(i);
        }
        return values;
    }

    private void addUnion(List<Projected> projected, SqlResult result, Set<IndexKey> seen, boolean all) {
        for (Row row : result.rows()) {
            Object[] values = new Object[row.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = row.get(i);
            }
            if (!all && !seen.add(new IndexKey(values))) {
                continue;
            }
            projected.add(new Projected(null, values, null));
        }
    }

    private void sortUnion(List<Projected> projected, List<OrderItem> order, List<SelectItem> items) {
        projected.sort((left, right) -> {
            for (OrderItem item : order) {
                Integer index = selectIndex(item.expr(), items);
                if (index == null) {
                    throw new SeaException("ORDER BY must use columns from the SELECT list");
                }
                int cmp = compareValues(left.values[index], right.values[index]);
                if (item.descending()) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });
    }

    private boolean activeCte(String name) {
        if (name == null || name.indexOf('.') >= 0) {
            return false;
        }
        String key = Names.fold(name);
        if (cteDefining.contains(key)) {
            return true;
        }
        return cteScope != null && cteScope.containsKey(key);
    }

    private SqlResult querySources(Select statement) {
        List<Expr> whereParts = conjuncts(statement.where());
        Probe left = openProbe(statement.table(), statement.alias());
        Held current = null;
        List<Evaluator.Frame> leftFrames = left.frames();
        joinKeys = new HashSet<>();
        for (Join join : statement.joins()) {
            Probe right = openProbe(join.relation(), join.alias());
            if (join.on() == null && (join.natural() || !join.using().isEmpty())) {
                Expr on = equiJoin(leftFrames, right.frames(), join.using(), joinKeys);
                join = new Join(join.kind(), join.relation(), join.alias(), on, join.natural(), join.using());
            }
            if (join.kind() == JoinKind.RIGHT) {
                Probe inner = current == null ? left : new Probe(current.frames(), current.rows(), null, null);
                Held driving = materialize(right, filtersOn(whereParts, right.frames()));
                Held joined = joinInputs(driving, inner, join.on(), List.of(), true, false);
                current = flip(joined, inner.frames(), right.frames());
                leftFrames = current.frames();
                continue;
            }
            if (current == null) {
                current = materialize(left, filtersOn(whereParts, left.frames()));
            } else {
                current = filterHeld(current, filtersOn(whereParts, current.frames()));
            }
            if (join.kind() == JoinKind.CROSS) {
                Held rightRows = materialize(right, filtersOn(whereParts, right.frames()));
                current = joinRows(current, rightRows, null, false);
                leftFrames = current.frames();
                continue;
            }
            boolean full = join.kind() == JoinKind.FULL;
            boolean outer = full || join.kind() == JoinKind.LEFT;
            current = joinInputs(current, right, join.on(), outer ? List.of() : whereParts, outer, full);
            leftFrames = current.frames();
        }
        if (current == null) {
            current = materialize(left, filtersOn(whereParts, left.frames()));
        }
        try {
            return projectHeld(statement, current);
        } finally {
            joinKeys = Set.of();
        }
    }

    private Expr equiJoin(List<Evaluator.Frame> left, List<Evaluator.Frame> right, List<String> using, Set<String> shared) {
        List<String> wanted = using == null ? List.of() : using;
        Expr on = null;
        Set<String> matched = new HashSet<>();
        for (Evaluator.Frame rightFrame : right) {
            for (Column column : rightFrame.schema.columns) {
                if (!wanted.isEmpty() && wanted.stream().noneMatch(name -> Names.same(name, column.name))) {
                    continue;
                }
                Evaluator.Frame owner = null;
                for (Evaluator.Frame frame : left) {
                    if (frame.schema.indexOf(column.name) >= 0) {
                        owner = frame;
                        break;
                    }
                }
                if (owner == null) {
                    continue;
                }
                matched.add(Names.fold(column.name));
                shared.add(Names.fold(column.name));
                String leftName = owner.alias != null ? owner.alias : owner.name;
                String rightName = rightFrame.alias != null ? rightFrame.alias : rightFrame.name;
                Expr eq = new Binary(BinaryOp.EQ, new ColumnRef(leftName, column.name), new ColumnRef(rightName, column.name));
                on = on == null ? eq : new Binary(BinaryOp.AND, on, eq);
            }
        }
        for (String name : wanted) {
            if (!matched.contains(Names.fold(name))) {
                throw new SeaException("USING column is not shared: " + name);
            }
        }
        if (on == null) {
            throw new SeaException("JOIN has no common column");
        }
        return on;
    }

    private SqlResult projectHeld(Select statement, Held held) {
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(held.frames());
        List<Object[]> matched = new ArrayList<>();
        for (Object[] row : held.rows()) {
            evaluator.bindJoined(row);
            if (statement.where() == null || evaluator.truth(statement.where())) {
                matched.add(row);
            }
        }
        List<SelectItem> items = new ArrayList<>();
        List<ColumnMeta> columns = new ArrayList<>();
        if (statement.star()) {
            Set<String> emittedKeys = new HashSet<>();
            for (Evaluator.Frame frame : held.frames()) {
                String qualifier = frame.alias != null ? frame.alias : frame.name;
                String namespace = frame.namespace == null ? "" : frame.namespace;
                for (Column column : frame.schema.columns) {
                    String key = Names.fold(column.name);
                    if (joinKeys.contains(key) && !emittedKeys.add(key)) {
                        continue;
                    }
                    items.add(new SelectItem(new ColumnRef(qualifier, column.name), null, column.name));
                    columns.add(new ColumnMeta(column.name, column.type, namespace, frame.name));
                }
            }
        } else {
            inferFrames = held.frames();
            try {
                for (SelectItem item : statement.items()) {
                    if (item.expr() instanceof QualifiedStar star) {
                        for (Evaluator.Frame frame : framesForStar(held.frames(), star.qualifier())) {
                            String namespace = frame.namespace == null ? "" : frame.namespace;
                            for (Column column : frame.schema.columns) {
                                items.add(new SelectItem(new ColumnRef(star.qualifier(), column.name), null, column.name));
                                columns.add(new ColumnMeta(column.name, column.type, namespace, frame.name));
                            }
                        }
                        continue;
                    }
                    items.add(item);
                    if (item.expr() instanceof ColumnRef column) {
                        Evaluator.Frame frame = Evaluator.Frame.find(held.frames(), column.qualifier(), column.name());
                        String namespace = frame.namespace == null ? "" : frame.namespace;
                        columns.add(new ColumnMeta(item.label(), frame.schema.column(column.name()).type, namespace, frame.name));
                    } else {
                        columns.add(new ColumnMeta(item.label(), infer(item.expr(), held.frames().get(0).schema)));
                    }
                }
            } finally {
                inferFrames = null;
            }
        }
        if (items.isEmpty()) {
            throw new SeaException("SELECT needs at least one expression");
        }
        boolean aggregate = false;
        for (SelectItem item : items) {
            if (Evaluator.containsAggregate(item.expr())) {
                aggregate = true;
            }
        }
        if (!statement.groupBy().isEmpty() || statement.having() != null || aggregate) {
            return grouped(statement.star(), statement.distinct(), statement.groupBy(), statement.having(),
                    statement.orderBy(), statement.limit(), statement.offset(), items, columns, evaluator, new GroupSource() {
                @Override
                public int size() {
                    return matched.size();
                }

                @Override
                public void bind(int index) {
                    evaluator.bindJoined(matched.get(index));
                }

                @Override
                public Projected projected(int index, Object[] values) {
                    return new Projected(null, values, matched.get(index));
                }

                @Override
                public String identify(ColumnRef column) {
                    return Evaluator.Frame.identify(held.frames(), column);
                }

                @Override
                public boolean hasColumn(String name) {
                    return Evaluator.Frame.hasColumn(held.frames(), name);
                }
            });
        }
        List<Projected> projected = new ArrayList<>();
        for (Object[] row : matched) {
            evaluator.bindJoined(row);
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evaluator.eval(items.get(i).expr()));
            }
            projected.add(new Projected(null, values, row));
        }
        applyWindows(projected, items, statement.orderBy(), evaluator);
        if (statement.distinct()) {
            projected = distinct(projected);
        }
        if (!statement.orderBy().isEmpty()) {
            sort(projected, statement, items, evaluator);
        }
        projected = page(projected, statement.limit(), statement.offset());
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(columns, row.values));
        }
        return SqlResult.query(columns, rows);
    }

    private Probe openProbe(String written, String alias) {
        if (written != null && written.indexOf('.') < 0) {
            String key = Names.fold(written);
            if (cteDefining.contains(key)) {
                throw new SeaException("CTE cannot reference itself: " + written);
            }
            Held cte = cteScope == null ? null : cteScope.get(key);
            if (cte != null) {
                List<Evaluator.Frame> frames = new ArrayList<>();
                for (Evaluator.Frame frame : cte.frames()) {
                    frames.add(frame.withAlias(alias));
                }
                return new Probe(frames, cte.rows(), null, alias);
            }
        }
        if (findView(written) != null) {
            Held view = loadView(written, alias);
            return new Probe(view.frames(), view.rows(), null, alias);
        }
        guardAccess(written, "SELECT");
        TableStore table = require(written);
        if (repeatable() && inTransaction && !SystemTables.isSystem(table.schema().name)) {
            pinRead(table);
        }
        Schema schema = table.schema();
        Evaluator.Frame frame = new Evaluator.Frame(schema, schema.name, written, table.namespace(), alias, 0, schema.columns.size());
        return new Probe(List.of(frame), null, table, alias);
    }

    private Held materialize(Probe probe, List<Expr> filters) {
        Expr filter = andAll(filters);
        if (probe.table != null) {
            Evaluator evaluator = evaluator(probe.table, probe.alias);
            List<Object[]> rows = new ArrayList<>();
            for (StoredRow row : match(probe.table, evaluator, filter)) {
                rows.add(cells(probe.table, row));
            }
            return new Held(probe.frames, rows);
        }
        if (filter == null) {
            return new Held(probe.frames, probe.rows);
        }
        return filterHeld(new Held(probe.frames, probe.rows), filters);
    }

    private Held filterHeld(Held held, List<Expr> filters) {
        Expr filter = andAll(filters);
        if (filter == null) {
            return held;
        }
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(held.frames());
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : held.rows()) {
            evaluator.bindJoined(row);
            if (evaluator.truth(filter)) {
                rows.add(row);
            }
        }
        return new Held(held.frames(), rows);
    }

    private Object[] cells(TableStore table, StoredRow row) {
        Schema schema = table.schema();
        Object[] values = new Object[schema.columns.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = materialize(table.value(row, i));
        }
        return values;
    }

    private Held loadView(String written, String alias) {
        Map<String, Held> savedScope = cteScope;
        Set<String> savedDefining = cteDefining;
        cteScope = null;
        cteDefining = new HashSet<>();
        SqlResult produced;
        try {
            produced = query(new Select(false, true, List.of(), written, null, List.of(), null, List.of(), null, List.of(), null, null));
        } finally {
            cteScope = savedScope;
            cteDefining = savedDefining;
        }
        return holdRows(written, alias, produced.columns(), rowsOf(produced));
    }

    private Held holdRows(String written, String alias, List<ColumnMeta> metas, List<Object[]> rows) {
        Relation relation = Relation.parse(written);
        String namespace = relation.schema() == null ? "" : relation.schema();
        List<Column> columns = new ArrayList<>();
        for (ColumnMeta meta : metas) {
            DataType type = meta.type();
            int length = type == DataType.VARCHAR || type == DataType.CHAR ? 256 : 0;
            columns.add(new Column(meta.name(), type, length, 0, false, false, 0L, false, null));
            if (meta.schema() != null && !meta.schema().isEmpty()) {
                namespace = meta.schema();
            }
        }
        Schema schema = new Schema(relation.name(), columns, new int[0], List.of(), List.of());
        Evaluator.Frame frame = new Evaluator.Frame(schema, relation.name(), written, namespace, alias, 0, columns.size());
        return new Held(List.of(frame), rows);
    }

    private List<Object[]> rowsOf(SqlResult result) {
        List<Object[]> rows = new ArrayList<>();
        for (Row row : result.rows()) {
            Object[] values = new Object[row.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = row.get(i);
            }
            rows.add(values);
        }
        return rows;
    }

    private Held joinInputs(Held left, Probe right, Expr on, List<Expr> whereParts, boolean keepLeft, boolean keepRight) {
        List<Expr> onParts = conjuncts(on);
        List<Evaluator.Frame> rightFrames = right.frames();
        List<Expr> rightFilters = new ArrayList<>();
        for (Expr part : onParts) {
            if (onlyReferences(part, rightFrames)) {
                rightFilters.add(part);
            }
        }
        List<Expr> extra = new ArrayList<>();
        if (!keepLeft && !keepRight) {
            List<Evaluator.Frame> both = combine(left.frames(), rightFrames);
            for (Expr part : whereParts) {
                if (onlyReferences(part, rightFrames)) {
                    rightFilters.add(part);
                } else if (references(part, left.frames()) && references(part, rightFrames) && onlyReferences(part, both)) {
                    extra.add(part);
                }
            }
        }
        List<Equi> pairs = equiPairs(onParts, left.frames(), rightFrames);
        pairs.addAll(equiPairs(extra, left.frames(), rightFrames));
        if (pairs.isEmpty()) {
            Held rightRows = materialize(right, rightFilters);
            List<Expr> conditionParts = new ArrayList<>();
            if (on != null) {
                conditionParts.add(on);
            }
            conditionParts.addAll(extra);
            Expr condition = andAll(conditionParts);
            if (keepRight) {
                return fullJoin(left, rightRows, condition);
            }
            return joinRows(left, rightRows, condition, keepLeft);
        }
        Fixed fixed = fixedEquals(rightFilters, rightFrames);
        if (fixed.contradiction) {
            return unmatched(left, rightFrames, keepLeft);
        }
        Set<Integer> seekColumns = new HashSet<>();
        for (Equi pair : pairs) {
            seekColumns.add(pair.rightIndex());
        }
        seekColumns.addAll(fixed.bound().keySet());
        if (right.table != null && right.table.canSeek(seekColumns)) {
            return indexJoin(left, right, pairs, fixed.bound(), on, extra, rightFilters, keepLeft, keepRight);
        }
        return hashJoin(left, right, pairs, rightFilters, on, extra, keepLeft, keepRight);
    }

    private Held unmatched(Held left, List<Evaluator.Frame> rightFrames, boolean keepLeft) {
        List<Evaluator.Frame> frames = combine(left.frames(), rightFrames);
        if (!keepLeft) {
            return new Held(frames, List.of());
        }
        Object[] rightNulls = new Object[width(rightFrames)];
        List<Object[]> rows = new ArrayList<>();
        for (Object[] leftRow : left.rows()) {
            rows.add(concat(leftRow, rightNulls));
        }
        return new Held(frames, rows);
    }

    private Held indexJoin(Held left, Probe right, List<Equi> pairs, Map<Integer, Object> fixed, Expr on, List<Expr> extra,
                           List<Expr> rightFilters, boolean keepLeft, boolean keepRight) {
        List<Evaluator.Frame> frames = combine(left.frames(), right.frames());
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(frames);
        List<Object[]> rows = new ArrayList<>();
        Object[] rightNulls = new Object[width(right.frames())];
        Set<Long> matched = keepRight ? new HashSet<>() : null;
        List<StoredRow> population = keepRight
                ? match(right.table, evaluator(right.table, right.alias), andAll(rightFilters))
                : List.of();
        for (Object[] leftRow : left.rows()) {
            Map<Integer, Object> key = seekKey(leftRow, pairs, fixed);
            boolean any = false;
            if (key != null) {
                List<StoredRow> found = right.table.lookup(key);
                if (found != null) {
                    for (StoredRow stored : found) {
                        Object[] combined = concat(leftRow, cells(right.table, stored));
                        if (!accept(evaluator, combined, on, extra)) {
                            continue;
                        }
                        rows.add(combined);
                        any = true;
                        if (matched != null) {
                            matched.add(stored.id);
                        }
                    }
                }
            }
            if (!any && keepLeft) {
                rows.add(concat(leftRow, rightNulls));
            }
        }
        if (keepRight) {
            Object[] leftNulls = new Object[width(left.frames())];
            for (StoredRow stored : population) {
                if (!matched.contains(stored.id)) {
                    rows.add(concat(leftNulls, cells(right.table, stored)));
                }
            }
        }
        return new Held(frames, rows);
    }

    private Held hashJoin(Held left, Probe right, List<Equi> pairs, List<Expr> rightFilters, Expr on, List<Expr> extra,
                          boolean keepLeft, boolean keepRight) {
        Held rightHeld = materialize(right, rightFilters);
        List<Evaluator.Frame> frames = combine(left.frames(), rightHeld.frames());
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(frames);
        Map<IndexKey, List<Integer>> buckets = new HashMap<>();
        List<Object[]> rightRows = rightHeld.rows();
        for (int i = 0; i < rightRows.size(); i++) {
            Object[] parts = hashParts(rightRows.get(i), pairs, false);
            if (parts == null) {
                continue;
            }
            buckets.computeIfAbsent(new IndexKey(parts), key -> new ArrayList<>()).add(i);
        }
        boolean[] hit = keepRight ? new boolean[rightRows.size()] : null;
        List<Object[]> rows = new ArrayList<>();
        Object[] rightNulls = new Object[width(rightHeld.frames())];
        for (Object[] leftRow : left.rows()) {
            Object[] parts = hashParts(leftRow, pairs, true);
            boolean any = false;
            List<Integer> hits = parts == null ? null : buckets.get(new IndexKey(parts));
            if (hits != null) {
                for (int index : hits) {
                    Object[] combined = concat(leftRow, rightRows.get(index));
                    if (!accept(evaluator, combined, on, extra)) {
                        continue;
                    }
                    rows.add(combined);
                    any = true;
                    if (hit != null) {
                        hit[index] = true;
                    }
                }
            }
            if (!any && keepLeft) {
                rows.add(concat(leftRow, rightNulls));
            }
        }
        if (keepRight) {
            Object[] leftNulls = new Object[width(left.frames())];
            for (int i = 0; i < rightRows.size(); i++) {
                if (!hit[i]) {
                    rows.add(concat(leftNulls, rightRows.get(i)));
                }
            }
        }
        return new Held(frames, rows);
    }

    private Map<Integer, Object> seekKey(Object[] leftRow, List<Equi> pairs, Map<Integer, Object> fixed) {
        Map<Integer, Object> key = new HashMap<>(fixed);
        for (Equi pair : pairs) {
            Object raw = leftRow[pair.left().offset + pair.leftIndex()];
            if (raw == null) {
                return null;
            }
            Column column = pair.right().schema.columns.get(pair.rightIndex());
            Object coerced = Values.coerce(column.type, column.length, column.scale, raw);
            if (coerced == null || Values.compare(coerced, raw) != 0) {
                return null;
            }
            Object previous = key.get(pair.rightIndex());
            if (previous != null && Values.compare(previous, coerced) != 0) {
                return null;
            }
            key.put(pair.rightIndex(), coerced);
        }
        return key.isEmpty() ? null : key;
    }

    private Object[] hashParts(Object[] row, List<Equi> pairs, boolean leftSide) {
        Object[] parts = new Object[pairs.size()];
        for (int i = 0; i < pairs.size(); i++) {
            Equi pair = pairs.get(i);
            Object raw = leftSide
                    ? row[pair.left().offset + pair.leftIndex()]
                    : row[pair.right().offset + pair.rightIndex()];
            if (raw == null) {
                return null;
            }
            Column column = pair.right().schema.columns.get(pair.rightIndex());
            Object coerced = Values.coerce(column.type, column.length, column.scale, raw);
            if (coerced == null || Values.compare(coerced, raw) != 0) {
                return null;
            }
            parts[i] = coerced;
        }
        return parts;
    }

    private boolean accept(Evaluator evaluator, Object[] row, Expr on, List<Expr> extra) {
        evaluator.bindJoined(row);
        if (on != null && !evaluator.truth(on)) {
            return false;
        }
        for (Expr expr : extra) {
            if (!evaluator.truth(expr)) {
                return false;
            }
        }
        return true;
    }

    private List<Equi> equiPairs(List<Expr> parts, List<Evaluator.Frame> left, List<Evaluator.Frame> right) {
        List<Equi> pairs = new ArrayList<>();
        for (Expr expr : parts) {
            if (!(expr instanceof Binary binary) || binary.op() != BinaryOp.EQ) {
                continue;
            }
            if (!(binary.left() instanceof ColumnRef leftRef) || !(binary.right() instanceof ColumnRef rightRef)) {
                continue;
            }
            Evaluator.Frame leftOnLeft = tryFrame(left, leftRef);
            Evaluator.Frame leftOnRight = tryFrame(right, leftRef);
            Evaluator.Frame rightOnLeft = tryFrame(left, rightRef);
            Evaluator.Frame rightOnRight = tryFrame(right, rightRef);
            Evaluator.Frame leftFrame;
            Evaluator.Frame rightFrame;
            ColumnRef leftColumn;
            ColumnRef rightColumn;
            if (leftOnLeft != null && leftOnRight == null && rightOnRight != null && rightOnLeft == null) {
                leftFrame = leftOnLeft;
                rightFrame = rightOnRight;
                leftColumn = leftRef;
                rightColumn = rightRef;
            } else if (rightOnLeft != null && rightOnRight == null && leftOnRight != null && leftOnLeft == null) {
                leftFrame = rightOnLeft;
                rightFrame = leftOnRight;
                leftColumn = rightRef;
                rightColumn = leftRef;
            } else {
                continue;
            }
            int leftIndex = leftFrame.schema.indexOf(leftColumn.name());
            int rightIndex = rightFrame.schema.indexOf(rightColumn.name());
            if (leftIndex < 0 || rightIndex < 0) {
                continue;
            }
            pairs.add(new Equi(leftFrame, leftIndex, rightFrame, rightIndex));
        }
        return pairs;
    }

    private Fixed fixedEquals(List<Expr> filters, List<Evaluator.Frame> frames) {
        Map<Integer, Object> equals = new HashMap<>();
        for (Expr expr : filters) {
            if (!(expr instanceof Binary binary) || binary.op() != BinaryOp.EQ) {
                continue;
            }
            ColumnRef column;
            Expr valueExpr;
            if (binary.left() instanceof ColumnRef left && constant(binary.right()) != Values.MISSING) {
                column = left;
                valueExpr = binary.right();
            } else if (binary.right() instanceof ColumnRef right && constant(binary.left()) != Values.MISSING) {
                column = right;
                valueExpr = binary.left();
            } else {
                continue;
            }
            Evaluator.Frame frame = tryFrame(frames, column);
            if (frame == null) {
                continue;
            }
            int index = frame.schema.indexOf(column.name());
            if (index < 0) {
                continue;
            }
            Object raw = constant(valueExpr);
            if (raw == null) {
                return new Fixed(Map.of(), true);
            }
            Column definition = frame.schema.columns.get(index);
            Object coerced;
            try {
                coerced = Values.coerce(definition.type, definition.length, definition.scale, raw);
                if (Values.compare(coerced, raw) != 0) {
                    continue;
                }
            } catch (SeaException exception) {
                continue;
            }
            Object previous = equals.get(index);
            if (previous != null && Values.compare(previous, coerced) != 0) {
                return new Fixed(Map.of(), true);
            }
            equals.put(index, coerced);
        }
        return new Fixed(equals, false);
    }

    private List<Expr> conjuncts(Expr expr) {
        List<Expr> parts = new ArrayList<>();
        flattenAnd(expr, parts);
        return parts;
    }

    private void flattenAnd(Expr expr, List<Expr> parts) {
        if (expr instanceof Binary binary && binary.op() == BinaryOp.AND) {
            flattenAnd(binary.left(), parts);
            flattenAnd(binary.right(), parts);
            return;
        }
        if (expr != null) {
            parts.add(expr);
        }
    }

    private Expr andAll(List<Expr> parts) {
        Expr combined = null;
        for (Expr part : parts) {
            combined = combined == null ? part : new Binary(BinaryOp.AND, combined, part);
        }
        return combined;
    }

    private List<Expr> filtersOn(List<Expr> parts, List<Evaluator.Frame> frames) {
        List<Expr> filters = new ArrayList<>();
        for (Expr part : parts) {
            if (onlyReferences(part, frames)) {
                filters.add(part);
            }
        }
        return filters;
    }

    private boolean onlyReferences(Expr expr, List<Evaluator.Frame> frames) {
        for (ColumnRef column : columnRefs(expr)) {
            if (tryFrame(frames, column) == null) {
                return false;
            }
        }
        return true;
    }

    private boolean references(Expr expr, List<Evaluator.Frame> frames) {
        for (ColumnRef column : columnRefs(expr)) {
            if (tryFrame(frames, column) != null) {
                return true;
            }
        }
        return false;
    }

    private Evaluator.Frame tryFrame(List<Evaluator.Frame> frames, ColumnRef column) {
        try {
            return Evaluator.Frame.find(frames, column.qualifier(), column.name());
        } catch (SeaException exception) {
            return null;
        }
    }

    private List<ColumnRef> columnRefs(Expr expr) {
        List<ColumnRef> refs = new ArrayList<>();
        collectColumns(expr, refs);
        return refs;
    }

    private void collectColumns(Expr expr, List<ColumnRef> refs) {
        if (expr instanceof ColumnRef column) {
            refs.add(column);
        } else if (expr instanceof Unary unary) {
            collectColumns(unary.expr(), refs);
        } else if (expr instanceof Binary binary) {
            collectColumns(binary.left(), refs);
            collectColumns(binary.right(), refs);
        } else if (expr instanceof IsNull isNull) {
            collectColumns(isNull.expr(), refs);
        } else if (expr instanceof Like like) {
            collectColumns(like.expr(), refs);
            collectColumns(like.pattern(), refs);
            if (like.escape() != null) {
                collectColumns(like.escape(), refs);
            }
        } else if (expr instanceof InList inList) {
            collectColumns(inList.expr(), refs);
            for (Expr value : inList.values()) {
                collectColumns(value, refs);
            }
        } else if (expr instanceof Between between) {
            collectColumns(between.expr(), refs);
            collectColumns(between.low(), refs);
            collectColumns(between.high(), refs);
        } else if (expr instanceof Call call) {
            for (Expr arg : call.args()) {
                collectColumns(arg, refs);
            }
        }
    }

    private Held joinRows(Held left, Held right, Expr on, boolean keepLeft) {
        List<Evaluator.Frame> frames = combine(left.frames(), right.frames());
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(frames);
        List<Object[]> rows = new ArrayList<>();
        Object[] rightNulls = new Object[width(right.frames())];
        for (Object[] leftRow : left.rows()) {
            boolean any = false;
            for (Object[] rightRow : right.rows()) {
                Object[] combined = concat(leftRow, rightRow);
                if (on == null || matches(evaluator, combined, on)) {
                    rows.add(combined);
                    any = true;
                }
            }
            if (!any && keepLeft) {
                rows.add(concat(leftRow, rightNulls));
            }
        }
        return new Held(frames, rows);
    }

    private Held flip(Held joined, List<Evaluator.Frame> leftFrames, List<Evaluator.Frame> rightFrames) {
        int leftWidth = width(leftFrames);
        int rightWidth = width(rightFrames);
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : joined.rows()) {
            Object[] ordered = new Object[row.length];
            System.arraycopy(row, rightWidth, ordered, 0, leftWidth);
            System.arraycopy(row, 0, ordered, leftWidth, rightWidth);
            rows.add(ordered);
        }
        return new Held(combine(leftFrames, rightFrames), rows);
    }

    private Held fullJoin(Held left, Held right, Expr on) {
        List<Evaluator.Frame> frames = combine(left.frames(), right.frames());
        Evaluator evaluator = evaluator(null, null);
        evaluator.useFrames(frames);
        List<Object[]> rows = new ArrayList<>();
        boolean[] matched = new boolean[right.rows().size()];
        Object[] rightNulls = new Object[width(right.frames())];
        Object[] leftNulls = new Object[width(left.frames())];
        for (Object[] leftRow : left.rows()) {
            boolean any = false;
            for (int index = 0; index < right.rows().size(); index++) {
                Object[] combined = concat(leftRow, right.rows().get(index));
                if (matches(evaluator, combined, on)) {
                    rows.add(combined);
                    matched[index] = true;
                    any = true;
                }
            }
            if (!any) {
                rows.add(concat(leftRow, rightNulls));
            }
        }
        for (int index = 0; index < right.rows().size(); index++) {
            if (!matched[index]) {
                rows.add(concat(leftNulls, right.rows().get(index)));
            }
        }
        return new Held(frames, rows);
    }

    private boolean matches(Evaluator evaluator, Object[] row, Expr on) {
        evaluator.bindJoined(row);
        return evaluator.truth(on);
    }

    private List<Evaluator.Frame> combine(List<Evaluator.Frame> left, List<Evaluator.Frame> right) {
        List<Evaluator.Frame> frames = new ArrayList<>(left);
        int shift = width(left);
        for (Evaluator.Frame frame : right) {
            frames.add(frame.shift(shift));
        }
        return frames;
    }

    private static int width(List<Evaluator.Frame> frames) {
        int width = 0;
        for (Evaluator.Frame frame : frames) {
            width += frame.width;
        }
        return width;
    }

    private static Object[] concat(Object[] left, Object[] right) {
        Object[] combined = new Object[left.length + right.length];
        System.arraycopy(left, 0, combined, 0, left.length);
        System.arraycopy(right, 0, combined, left.length, right.length);
        return combined;
    }

    private SqlResult grouped(boolean star, boolean distinct, List<Expr> groupBy, Expr having, List<OrderItem> orderBy,
                              Long limit, Long offset, List<SelectItem> items, List<ColumnMeta> columns,
                              Evaluator evaluator, GroupSource source) {
        if (star && !groupBy.isEmpty()) {
            throw new SeaException("SELECT * cannot be combined with GROUP BY");
        }
        if (groupBy.isEmpty()) {
            boolean aggregate = false;
            boolean scalar = false;
            for (SelectItem item : items) {
                if (Evaluator.containsAggregate(item.expr())) {
                    aggregate = true;
                } else {
                    scalar = true;
                }
            }
            if (aggregate && scalar) {
                throw new SeaException("cannot mix aggregates and columns without GROUP BY");
            }
        }
        List<Expr> keys = groupKeys(groupBy, items, source);
        for (SelectItem item : items) {
            checkGrouped(item.expr(), keys, source);
        }
        if (having != null) {
            checkGrouped(having, keys, source);
        }
        Map<IndexKey, List<Integer>> groups = new LinkedHashMap<>();
        if (keys.isEmpty()) {
            List<Integer> all = new ArrayList<>();
            for (int index = 0; index < source.size(); index++) {
                all.add(index);
            }
            groups.put(new IndexKey(new Object[0]), all);
        } else {
            for (int index = 0; index < source.size(); index++) {
                source.bind(index);
                Object[] keyValues = new Object[keys.size()];
                for (int key = 0; key < keys.size(); key++) {
                    keyValues[key] = materialize(evaluator.eval(keys.get(key)));
                }
                groups.computeIfAbsent(new IndexKey(keyValues), ignored -> new ArrayList<>()).add(index);
            }
        }
        List<Projected> projected = new ArrayList<>();
        for (List<Integer> members : groups.values()) {
            if (having != null && !evaluator.truthGrouped(having, members, source::bind)) {
                continue;
            }
            Object[] values = new Object[items.size()];
            for (int index = 0; index < items.size(); index++) {
                values[index] = materialize(evaluator.evalGrouped(items.get(index).expr(), members, source::bind));
            }
            if (members.isEmpty()) {
                projected.add(new Projected(null, values, null));
            } else {
                projected.add(source.projected(members.get(0), values));
            }
        }
        applyWindows(projected, items, orderBy, evaluator);
        if (distinct) {
            projected = distinct(projected);
        }
        if (!orderBy.isEmpty()) {
            sort(projected, orderBy, distinct, items, evaluator);
        }
        projected = page(projected, limit, offset);
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(columns, row.values));
        }
        return SqlResult.query(columns, rows);
    }

    private List<Expr> groupKeys(List<Expr> groupBy, List<SelectItem> items, GroupSource source) {
        List<Expr> keys = new ArrayList<>();
        for (Expr key : groupBy) {
            Long ordinal = ordinal(key);
            if (ordinal != null) {
                if (ordinal < 1 || ordinal > items.size()) {
                    throw new SeaException("GROUP BY " + ordinal + " is outside the SELECT list");
                }
                keys.add(items.get(ordinal.intValue() - 1).expr());
                continue;
            }
            if (key instanceof ColumnRef column && column.qualifier() == null && !source.hasColumn(column.name())) {
                Integer alias = aliasIndex(column.name(), items);
                if (alias != null) {
                    keys.add(items.get(alias).expr());
                    continue;
                }
            }
            keys.add(key);
        }
        for (Expr key : keys) {
            if (Evaluator.containsAggregate(key)) {
                throw new SeaException("GROUP BY cannot contain an aggregate");
            }
        }
        return keys;
    }

    private Integer aliasIndex(String name, List<SelectItem> items) {
        for (int index = 0; index < items.size(); index++) {
            SelectItem item = items.get(index);
            if (Names.same(item.label(), name) || (item.alias() != null && Names.same(item.alias(), name))) {
                return index;
            }
        }
        return null;
    }

    private void checkGrouped(Expr expr, List<Expr> keys, GroupSource source) {
        if (matchesKey(expr, keys, source)) {
            return;
        }
        switch (expr) {
            case Call call when Evaluator.aggregate(call.name()) -> {
            }
            case Call call -> {
                for (Expr argument : call.args()) {
                    checkGrouped(argument, keys, source);
                }
            }
            case ColumnRef column -> {
                String id = source.identify(column);
                if (!groupedColumn(id, keys, source)) {
                    throw new SeaException("column " + column.name() + " must appear in the GROUP BY");
                }
            }
            case Unary unary -> checkGrouped(unary.expr(), keys, source);
            case Binary binary -> {
                checkGrouped(binary.left(), keys, source);
                checkGrouped(binary.right(), keys, source);
            }
            case IsNull isNull -> checkGrouped(isNull.expr(), keys, source);
            case Like like -> {
                checkGrouped(like.expr(), keys, source);
                checkGrouped(like.pattern(), keys, source);
                if (like.escape() != null) {
                    checkGrouped(like.escape(), keys, source);
                }
            }
            case InList inList -> {
                checkGrouped(inList.expr(), keys, source);
                for (Expr value : inList.values()) {
                    checkGrouped(value, keys, source);
                }
            }
            case Between between -> {
                checkGrouped(between.expr(), keys, source);
                checkGrouped(between.low(), keys, source);
                checkGrouped(between.high(), keys, source);
            }
            case CaseExpr caseExpr -> {
                if (caseExpr.operand() != null) {
                    checkGrouped(caseExpr.operand(), keys, source);
                }
                for (WhenBranch branch : caseExpr.branches()) {
                    checkGrouped(branch.when(), keys, source);
                    checkGrouped(branch.then(), keys, source);
                }
                if (caseExpr.otherwise() != null) {
                    checkGrouped(caseExpr.otherwise(), keys, source);
                }
            }
            case WindowExpr window -> {
                for (Expr arg : window.call().args()) {
                    checkGrouped(arg, keys, source);
                }
                for (Expr part : window.partition()) {
                    checkGrouped(part, keys, source);
                }
                for (OrderItem item : window.orderBy()) {
                    checkGrouped(item.expr(), keys, source);
                }
            }
            case CastExpr cast -> checkGrouped(cast.expr(), keys, source);
            case InSubquery inSubquery -> checkGrouped(inSubquery.expr(), keys, source);
            case ScalarSubquery ignored -> {
            }
            case ExistsExpr ignored -> {
            }
            default -> {
            }
        }
    }

    private boolean matchesKey(Expr expr, List<Expr> keys, GroupSource source) {
        for (Expr key : keys) {
            if (expr.equals(key)) {
                return true;
            }
            if (expr instanceof ColumnRef column && key instanceof ColumnRef grouped
                    && source.identify(column).equals(source.identify(grouped))) {
                return true;
            }
        }
        return false;
    }

    private boolean groupedColumn(String id, List<Expr> keys, GroupSource source) {
        for (Expr key : keys) {
            if (key instanceof ColumnRef column && source.identify(column).equals(id)) {
                return true;
            }
        }
        return false;
    }

    private String identifyColumn(ColumnRef column, Schema schema, String tableName, String namespace, String alias) {
        if (column.qualifier() != null
                && !Names.same(column.qualifier(), tableName)
                && !Names.same(column.qualifier(), namespace + "." + tableName)
                && (alias == null || !Names.same(column.qualifier(), alias))) {
            throw new SeaException("unknown qualifier: " + column.qualifier());
        }
        int index = schema.indexOf(column.name());
        if (index < 0) {
            throw new SeaException("column not found: " + column.name());
        }
        return tableName + ":" + index;
    }

    private String identifyView(ColumnRef column, Schema schema, String alias, String qualifier) {
        if (column.qualifier() != null
                && !Names.same(column.qualifier(), schema.name)
                && !Names.same(column.qualifier(), qualifier)
                && (alias == null || !Names.same(column.qualifier(), alias))) {
            throw new SeaException("unknown qualifier: " + column.qualifier());
        }
        int index = schema.indexOf(column.name());
        if (index < 0) {
            throw new SeaException("column not found: " + column.name());
        }
        return schema.name + ":" + index;
    }

    private List<StoredRow> coverQuery(TableStore table, Schema schema, List<SelectItem> items, Expr where,
                                       Evaluator evaluator) {
        if (where == null) {
            return null;
        }
        Set<Integer> needed = new HashSet<>();
        for (SelectItem item : items) {
            if (!columnsOf(item.expr(), schema, needed)) {
                return null;
            }
        }
        if (!columnsOf(where, schema, needed) || needed.isEmpty()) {
            return null;
        }
        EqualsProbe probe = probeEquals(schema, evaluator, where);
        if (probe.contradiction || probe.orColumn != null || probe.rangeColumn != null || probe.likePrefix != null) {
            return null;
        }
        return table.cover(probe.values, where, probe.calls, needed);
    }

    private boolean columnsOf(Expr expr, Schema schema, Set<Integer> needed) {
        if (expr == null || expr instanceof Literal || expr instanceof Param) {
            return true;
        }
        if (expr instanceof ColumnRef column) {
            int index = schema.indexOf(column.name());
            if (index < 0) {
                return false;
            }
            needed.add(index);
            return true;
        }
        if (expr instanceof Unary unary) {
            return columnsOf(unary.expr(), schema, needed);
        }
        if (expr instanceof Binary binary) {
            return columnsOf(binary.left(), schema, needed) && columnsOf(binary.right(), schema, needed);
        }
        if (expr instanceof IsNull isNull) {
            return columnsOf(isNull.expr(), schema, needed);
        }
        if (expr instanceof Like like) {
            return columnsOf(like.expr(), schema, needed) && columnsOf(like.pattern(), schema, needed)
                    && columnsOf(like.escape(), schema, needed);
        }
        if (expr instanceof Between between) {
            return columnsOf(between.expr(), schema, needed) && columnsOf(between.low(), schema, needed)
                    && columnsOf(between.high(), schema, needed);
        }
        if (expr instanceof InList list) {
            if (!columnsOf(list.expr(), schema, needed)) {
                return false;
            }
            for (Expr value : list.values()) {
                if (!columnsOf(value, schema, needed)) {
                    return false;
                }
            }
            return true;
        }
        if (expr instanceof Call call) {
            for (Expr arg : call.args()) {
                if (!columnsOf(arg, schema, needed)) {
                    return false;
                }
            }
            return true;
        }
        if (expr instanceof CastExpr cast) {
            return columnsOf(cast.expr(), schema, needed);
        }
        return false;
    }

    private List<StoredRow> match(TableStore table, Evaluator evaluator, Expr where) {
        List<StoredRow> matched = new ArrayList<>();
        Iterable<StoredRow> source = table.scan();
        TableStore.Snapshot pinned = pinnedRead(table);
        TableStore.Snapshot dirty = pinned == null ? uncommitted(table) : null;
        if (pinned != null) {
            source = pinned.rows.values();
        } else if (dirty != null) {
            source = dirty.rows.values();
        } else if (where != null) {
            EqualsProbe probe = probeEquals(table.schema(), evaluator, where);
            if (probe.contradiction) {
                return matched;
            }
            List<StoredRow> indexed = null;
            if (probe.orColumn != null) {
                indexed = new ArrayList<>();
                Set<Long> seen = new HashSet<>();
                for (Object value : probe.orValues) {
                    Map<Integer, Object> one = new LinkedHashMap<>();
                    one.put(probe.orColumn, value);
                    List<StoredRow> part = table.lookup(one, where, probe.calls);
                    if (part == null) {
                        indexed = null;
                        break;
                    }
                    for (StoredRow row : part) {
                        if (seen.add(row.id)) {
                            indexed.add(row);
                        }
                    }
                }
            } else if (probe.rangeColumn != null) {
                indexed = table.lookupRange(probe.values, probe.rangeColumn, probe.low, probe.lowInclusive,
                        probe.high, probe.highInclusive, probe.likePrefix, where, probe.calls, statistics(table));
            }
            if (indexed == null) {
                indexed = table.lookup(probe.values, where, probe.calls);
            }
            if (indexed != null) {
                source = indexed;
            }
        }
        for (StoredRow row : source) {
            Pace.check();
            evaluator.bind(row);
            if (where == null || evaluator.truth(where)) {
                matched.add(row);
            }
        }
        return matched;
    }

    private EqualsProbe probeEquals(Schema schema, Evaluator evaluator, Expr where) {
        EqualsProbe probe = new EqualsProbe();
        if (where instanceof Binary binary && binary.op() == BinaryOp.OR && collectOr(schema, evaluator, where, probe)) {
            return probe;
        }
        collectEquals(schema, evaluator, where, probe);
        return probe;
    }

    private boolean collectOr(Schema schema, Evaluator evaluator, Expr expr, EqualsProbe probe) {
        List<Binary> terms = new ArrayList<>();
        if (!flattenOr(expr, terms)) {
            return false;
        }
        Integer column = null;
        List<Object> values = new ArrayList<>();
        for (Binary term : terms) {
            if (term.op() != BinaryOp.EQ) {
                return false;
            }
            ColumnRef ref;
            Expr valueExpr;
            if (term.left() instanceof ColumnRef left && constant(term.right()) != Values.MISSING) {
                ref = left;
                valueExpr = term.right();
            } else if (term.right() instanceof ColumnRef right && constant(term.left()) != Values.MISSING) {
                ref = right;
                valueExpr = term.left();
            } else {
                return false;
            }
            if (!evaluator.owns(ref)) {
                return false;
            }
            int index = schema.indexOf(ref.name());
            if (index < 0) {
                return false;
            }
            if (column == null) {
                column = index;
            } else if (column != index) {
                return false;
            }
            Object raw = constant(valueExpr);
            if (raw == null) {
                continue;
            }
            try {
                values.add(Values.coerce(schema.columns.get(index).type, schema.columns.get(index).length,
                        schema.columns.get(index).scale, raw));
            } catch (SeaException exception) {
                return false;
            }
        }
        if (column == null || values.isEmpty()) {
            return false;
        }
        probe.orColumn = column;
        probe.orValues = values;
        return true;
    }

    private boolean flattenOr(Expr expr, List<Binary> terms) {
        if (!(expr instanceof Binary binary)) {
            return false;
        }
        if (binary.op() == BinaryOp.OR) {
            return flattenOr(binary.left(), terms) && flattenOr(binary.right(), terms);
        }
        terms.add(binary);
        return true;
    }

    private void collectEquals(Schema schema, Evaluator evaluator, Expr expr, EqualsProbe probe) {
        if (probe.contradiction) {
            return;
        }
        if (expr instanceof Binary binary && binary.op() == BinaryOp.AND) {
            collectEquals(schema, evaluator, binary.left(), probe);
            collectEquals(schema, evaluator, binary.right(), probe);
            return;
        }
        if (expr instanceof Between between && !between.negated()) {
            noteBetween(schema, evaluator, between, probe);
            return;
        }
        if (expr instanceof Like like && !like.negated()) {
            noteLike(schema, evaluator, like, probe);
            return;
        }
        if (!(expr instanceof Binary binary) || binary.op() != BinaryOp.EQ) {
            if (expr instanceof Binary comparison) {
                noteRange(schema, evaluator, comparison, probe);
            }
            return;
        }
        ColumnRef column;
        Expr valueExpr;
        if (binary.left() instanceof Call call && constant(binary.right()) != Values.MISSING) {
            noteCall(call, constant(binary.right()), probe);
            return;
        }
        if (binary.right() instanceof Call call && constant(binary.left()) != Values.MISSING) {
            noteCall(call, constant(binary.left()), probe);
            return;
        }
        if (binary.left() instanceof ColumnRef left && constant(binary.right()) != Values.MISSING) {
            column = left;
            valueExpr = binary.right();
        } else if (binary.right() instanceof ColumnRef right && constant(binary.left()) != Values.MISSING) {
            column = right;
            valueExpr = binary.left();
        } else {
            return;
        }
        if (!evaluator.owns(column)) {
            return;
        }
        int index = schema.indexOf(column.name());
        if (index < 0) {
            return;
        }
        Object raw = constant(valueExpr);
        if (raw == null) {
            probe.contradiction = true;
            return;
        }
        Column definition = schema.columns.get(index);
        Object coerced;
        try {
            coerced = Values.coerce(definition.type, definition.length, definition.scale, raw);
            if (Values.compare(coerced, raw) != 0) {
                return;
            }
        } catch (SeaException exception) {
            return;
        }
        Object previous = probe.values.get(index);
        if (previous != null) {
            try {
                if (Values.compare(previous, coerced) != 0) {
                    probe.contradiction = true;
                }
            } catch (SeaException exception) {
                probe.contradiction = true;
            }
            return;
        }
        probe.values.put(index, coerced);
    }

    private void noteCall(Call call, Object raw, EqualsProbe probe) {
        String key = IndexFeatures.canonical(call);
        if (key == null || raw == null) {
            return;
        }
        probe.calls.put(key, raw);
    }

    private void noteRange(Schema schema, Evaluator evaluator, Binary binary, EqualsProbe probe) {
        boolean lower = binary.op() == BinaryOp.GT || binary.op() == BinaryOp.GE;
        boolean upper = binary.op() == BinaryOp.LT || binary.op() == BinaryOp.LE;
        if (!lower && !upper) {
            return;
        }
        ColumnRef column;
        Expr bound;
        if (binary.left() instanceof ColumnRef left && constant(binary.right()) != Values.MISSING) {
            column = left;
            bound = binary.right();
        } else {
            return;
        }
        int index = rangeColumn(schema, evaluator, column);
        if (index < 0 || probe.values.containsKey(index)) {
            return;
        }
        Object value = coercedConstant(schema, index, constant(bound));
        if (value == null) {
            return;
        }
        if (probe.rangeColumn != null && probe.rangeColumn != index) {
            return;
        }
        probe.rangeColumn = index;
        if (lower) {
            probe.low = value;
            probe.lowInclusive = binary.op() == BinaryOp.GE;
        } else {
            probe.high = value;
            probe.highInclusive = binary.op() == BinaryOp.LE;
        }
    }

    private void noteBetween(Schema schema, Evaluator evaluator, Between between, EqualsProbe probe) {
        if (!(between.expr() instanceof ColumnRef column)) {
            return;
        }
        int index = rangeColumn(schema, evaluator, column);
        if (index < 0 || probe.rangeColumn != null || probe.values.containsKey(index)) {
            return;
        }
        Object low = coercedConstant(schema, index, constant(between.low()));
        Object high = coercedConstant(schema, index, constant(between.high()));
        if (low == null || high == null) {
            return;
        }
        probe.rangeColumn = index;
        probe.low = low;
        probe.high = high;
        probe.lowInclusive = true;
        probe.highInclusive = true;
    }

    private void noteLike(Schema schema, Evaluator evaluator, Like like, EqualsProbe probe) {
        if (like.escape() != null || !(like.expr() instanceof ColumnRef column)) {
            return;
        }
        Object pattern = constant(like.pattern());
        if (!(pattern instanceof String text) || text.length() < 2 || !text.endsWith("%")) {
            return;
        }
        String prefix = text.substring(0, text.length() - 1);
        if (prefix.indexOf('%') >= 0 || prefix.indexOf('_') >= 0) {
            return;
        }
        int index = rangeColumn(schema, evaluator, column);
        if (index < 0 || probe.rangeColumn != null || probe.values.containsKey(index)) {
            return;
        }
        DataType type = schema.columns.get(index).type;
        if (type != DataType.CHAR && type != DataType.VARCHAR) {
            return;
        }
        probe.rangeColumn = index;
        probe.likePrefix = prefix;
    }

    private int rangeColumn(Schema schema, Evaluator evaluator, ColumnRef column) {
        if (!evaluator.owns(column)) {
            return -1;
        }
        return schema.indexOf(column.name());
    }

    private Object coercedConstant(Schema schema, int index, Object raw) {
        if (raw == null || raw == Values.MISSING) {
            return null;
        }
        Column column = schema.columns.get(index);
        try {
            return Values.coerce(column.type, column.length, column.scale, raw);
        } catch (SeaException exception) {
            return null;
        }
    }

    private Object constant(Expr expr) {
        if (expr instanceof Literal literal) {
            return literal.value();
        }
        if (expr instanceof Param param) {
            return params[param.index()];
        }
        return Values.MISSING;
    }

    private List<SelectItem> star(Schema schema) {
        return star(schema, null);
    }

    private List<SelectItem> star(Schema schema, Set<String> allowed) {
        List<SelectItem> items = new ArrayList<>();
        for (Column column : schema.columns) {
            if (allowed != null && !allowed.contains(Names.fold(column.name))) {
                continue;
            }
            items.add(new SelectItem(new ColumnRef(null, column.name), null, column.name));
        }
        return items;
    }

    private List<SelectItem> expandStars(List<SelectItem> items, Schema schema, String tableName, String namespace,
                                          String written, String alias) {
        boolean qualified = false;
        for (SelectItem item : items) {
            if (item.expr() instanceof QualifiedStar) {
                qualified = true;
                break;
            }
        }
        if (!qualified) {
            return items;
        }
        List<SelectItem> expanded = new ArrayList<>();
        for (SelectItem item : items) {
            if (!(item.expr() instanceof QualifiedStar star)) {
                expanded.add(item);
                continue;
            }
            if (!matchesQualifier(star.qualifier(), tableName, namespace, written, alias)) {
                throw new SeaException("unknown qualifier: " + star.qualifier());
            }
            expanded.addAll(star(schema));
        }
        return expanded;
    }

    private static boolean matchesQualifier(String qualifier, String tableName, String namespace, String written, String alias) {
        if (tableName != null && Names.same(qualifier, tableName)) {
            return true;
        }
        if (written != null && Names.same(qualifier, written)) {
            return true;
        }
        if (alias != null && Names.same(qualifier, alias)) {
            return true;
        }
        return namespace != null && !namespace.isEmpty() && tableName != null
                && Names.same(qualifier, namespace + "." + tableName);
    }

    private List<Evaluator.Frame> framesForStar(List<Evaluator.Frame> frames, String qualifier) {
        List<Evaluator.Frame> matched = new ArrayList<>();
        for (Evaluator.Frame frame : frames) {
            if (frame.matches(qualifier)) {
                matched.add(frame);
            }
        }
        if (matched.isEmpty()) {
            throw new SeaException("unknown qualifier: " + qualifier);
        }
        if (matched.size() == 1) {
            return matched;
        }
        for (Evaluator.Frame frame : matched) {
            if (frame.alias == null || !Names.same(frame.alias, qualifier)) {
                throw new SeaException("ambiguous qualifier: " + qualifier);
            }
        }
        return matched;
    }

    private DataType infer(Expr expr, Schema schema) {
        return switch (expr) {
            case Literal literal -> literal.value() == null ? DataType.VARCHAR : dataTypeOf(literal.value());
            case Param param -> param.index() < params.length && params[param.index()] != null
                    ? dataTypeOf(params[param.index()]) : DataType.VARCHAR;
            case ColumnRef column -> inferFrames == null
                    ? schema.column(column.name()).type
                    : Evaluator.Frame.type(inferFrames, column);
            case QualifiedStar ignored -> throw new SeaException("* cannot be used in this expression");
            case VarRef variable -> scope != null ? scope.typeOf(variable.name()) : DataType.VARCHAR;
            case NextValue ignored -> DataType.BIGINT;
            case ContextRef context -> contextType(context);
            case Call call when Evaluator.aggregate(call.name()) -> aggregateType(call, schema);
            case Call call -> scalarType(call, schema);
            case WindowExpr window -> windowType(window, schema);
            case CaseExpr caseExpr -> caseType(caseExpr, schema);
            case CastExpr cast -> cast.type();
            case ScalarSubquery subquery -> subqueryType(subquery.query(), schema);
            case InSubquery ignored -> DataType.BOOLEAN;
            case ExistsExpr ignored -> DataType.BOOLEAN;
            case IsNull ignored -> DataType.BOOLEAN;
            case Like ignored -> DataType.BOOLEAN;
            case InList ignored -> DataType.BOOLEAN;
            case Between ignored -> DataType.BOOLEAN;
            case Unary unary when unary.op() == UnaryOp.NOT -> DataType.BOOLEAN;
            case Unary unary -> infer(unary.expr(), schema);
            case Binary binary when binary.op() == BinaryOp.OR || binary.op() == BinaryOp.AND
                    || binary.op() == BinaryOp.EQ || binary.op() == BinaryOp.NE
                    || binary.op() == BinaryOp.LT || binary.op() == BinaryOp.GT
                    || binary.op() == BinaryOp.LE || binary.op() == BinaryOp.GE -> DataType.BOOLEAN;
            case Binary binary when binary.op() == BinaryOp.CONCAT -> DataType.VARCHAR;
            case Binary binary -> wider(infer(binary.left(), schema), infer(binary.right(), schema));
        };
    }

    private DataType scalarType(Call call, Schema schema) {
        String name = call.name().toUpperCase(java.util.Locale.ROOT);
        return switch (name) {
            case "LENGTH", "CHAR_LENGTH", "POSITION", "DATEDIFF", "GEN_ID" -> DataType.BIGINT;
            case "JSON_EXTRACT", "JSON_TYPE" -> DataType.VARCHAR;
            case "EXTRACT", "SIGN" -> DataType.INTEGER;
            case "LOWER", "UPPER", "SUBSTRING", "TRIM", "LTRIM", "RTRIM", "REPLACE" -> DataType.VARCHAR;
            case "COALESCE" -> call.args().isEmpty() ? DataType.VARCHAR : infer(call.args().get(0), schema);
            case "ABS", "CEIL", "CEILING", "FLOOR", "ROUND" -> call.args().isEmpty()
                    ? DataType.INTEGER : infer(call.args().get(0), schema);
            case "MOD" -> call.args().size() < 2
                    ? DataType.INTEGER : wider(infer(call.args().get(0), schema), infer(call.args().get(1), schema));
            case "DATEADD" -> call.args().size() < 3 ? DataType.TIMESTAMP : dateAddType(call, schema);
            default -> DataType.VARCHAR;
        };
    }

    private DataType dateAddType(Call call, Schema schema) {
        DataType base = infer(call.args().get(2), schema);
        if (!(call.args().get(0) instanceof Literal literal) || !(literal.value() instanceof String part)) {
            return base;
        }
        String folded = part.toUpperCase(java.util.Locale.ROOT);
        if (base == DataType.DATE && (folded.equals("HOUR") || folded.equals("MINUTE") || folded.equals("SECOND"))) {
            return DataType.TIMESTAMP;
        }
        return base;
    }

    private DataType subqueryType(Query query, Schema schema) {
        return switch (query) {
            case Select select when select.star() && select.table() != null -> {
                TableStore table = findTable(select.table());
                yield table == null || table.schema().columns.isEmpty()
                        ? DataType.VARCHAR : table.schema().columns.get(0).type;
            }
            case Select select when !select.items().isEmpty() -> {
                Expr expr = select.items().get(0).expr();
                if (expr instanceof ColumnRef column && select.table() != null) {
                    TableStore table = findTable(select.table());
                    if (table != null && table.schema().indexOf(column.name()) >= 0) {
                        yield table.schema().column(column.name()).type;
                    }
                }
                yield expr instanceof ColumnRef ? DataType.VARCHAR : infer(expr, schema);
            }
            case UnionQuery union -> subqueryType(union.left(), schema);
            case WithQuery with -> subqueryType(with.body(), schema);
            default -> DataType.VARCHAR;
        };
    }

    private DataType caseType(CaseExpr expr, Schema schema) {
        DataType found = null;
        for (WhenBranch branch : expr.branches()) {
            found = sameBranch(found, branch.then(), schema);
        }
        if (expr.otherwise() != null) {
            found = sameBranch(found, expr.otherwise(), schema);
        }
        return found == null ? DataType.VARCHAR : found;
    }

    private DataType sameBranch(DataType found, Expr expr, Schema schema) {
        if (expr instanceof Literal literal && literal.value() == null) {
            return found;
        }
        DataType type = infer(expr, schema);
        if (found == null || found == type) {
            return type;
        }
        throw new SeaException("CASE branch types do not match");
    }

    private DataType aggregateType(Call call, Schema schema) {
        String name = call.name().toUpperCase(java.util.Locale.ROOT);
        if (name.equals("COUNT")) {
            return DataType.BIGINT;
        }
        if (call.args().isEmpty()) {
            return DataType.INTEGER;
        }
        DataType argument = infer(call.args().get(0), schema);
        if (name.equals("AVG")) {
            return argument == DataType.DECIMAL ? DataType.DECIMAL : DataType.DOUBLE;
        }
        if (name.equals("SUM")) {
            if (argument == DataType.DECIMAL) {
                return DataType.DECIMAL;
            }
            if (argument == DataType.FLOAT || argument == DataType.DOUBLE) {
                return DataType.DOUBLE;
            }
            if (argument == DataType.BIGINT) {
                return DataType.BIGINT;
            }
            if (argument == DataType.SMALLINT || argument == DataType.INTEGER || argument == DataType.BIGINT) {
                return DataType.BIGINT;
            }
        }
        return argument;
    }

    private static DataType wider(DataType left, DataType right) {
        if (left == DataType.DOUBLE || right == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (left == DataType.FLOAT || right == DataType.FLOAT) {
            return DataType.FLOAT;
        }
        if (left == DataType.DECIMAL || right == DataType.DECIMAL) {
            return DataType.DECIMAL;
        }
        if (left == DataType.BIGINT || right == DataType.BIGINT) {
            return DataType.BIGINT;
        }
        return DataType.INTEGER;
    }

    private static DataType dataTypeOf(Object value) {
        if (value instanceof byte[]) {
            return DataType.BLOB;
        }
        if (value instanceof BigDecimal) {
            return DataType.DECIMAL;
        }
        if (value instanceof Float) {
            return DataType.FLOAT;
        }
        if (value instanceof Double) {
            return DataType.DOUBLE;
        }
        if (value instanceof Integer) {
            return DataType.INTEGER;
        }
        if (value instanceof Long) {
            return DataType.BIGINT;
        }
        if (value instanceof Short) {
            return DataType.SMALLINT;
        }
        if (value instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (value instanceof LocalDate) {
            return DataType.DATE;
        }
        if (value instanceof LocalTime) {
            return DataType.TIME;
        }
        if (value instanceof LocalDateTime) {
            return DataType.TIMESTAMP;
        }
        if (value instanceof java.util.UUID) {
            return DataType.UUID;
        }
        if (value instanceof SqlInterval) {
            return DataType.INTERVAL;
        }
        if (value instanceof java.time.OffsetDateTime) {
            return DataType.TIMESTAMPTZ;
        }
        if (value instanceof String) {
            return DataType.VARCHAR;
        }
        return DataType.INTEGER;
    }

    private Object materialize(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes.clone();
        }
        return value;
    }

    private Object coerced(Column column, Object value) {
        if (value == null) {
            return null;
        }
        return Values.coerce(column.type, column.length, column.scale, value);
    }

    private int mutating(String tableName, java.util.function.IntSupplier action) {
        TableStore table = require(tableName);
        if (actor != null && actor.isolation == 1 && !SystemTables.isSystem(table.schema().name)) {
            rejectBorrowed(table);
        }
        track(table);
        boolean boundary = statementDepth == 0 && procedureDepth == 0;
        int renameBase = renameUndo.size();
        if (boundary) {
            statementSnapshots = new IdentityHashMap<>();
        }
        if (statementSnapshots != null && !Names.same(table.schema().name, SystemTables.GENERATORS)) {
            statementSnapshots.computeIfAbsent(table, key -> key.savepoint());
        }
        statementDepth++;
        try {
            int affected = action.getAsInt();
            if (boundary && !inTransaction) {
                flush();
            }
            return affected;
        } catch (RuntimeException exception) {
            if (boundary) {
                restoreStatement();
                undoRenames(renameBase);
            }
            throw exception;
        } finally {
            statementDepth--;
            if (boundary) {
                statementSnapshots = null;
                if (!inTransaction) {
                    renameUndo.clear();
                }
            }
        }
    }

    private void flush() {
        List<TransactionLog.Applied> transactions = new ArrayList<>(pending);
        pending.clear();
        flushMatching(table -> true, transactions);
    }

    private boolean flushMatching(Predicate<TableStore> include) {
        return flushMatching(include, List.of(), false);
    }

    private boolean flushMatching(Predicate<TableStore> include, List<TransactionLog.Applied> transactions) {
        return flushMatching(include, transactions, false);
    }

    private boolean flushMatching(Predicate<TableStore> include, List<TransactionLog.Applied> transactions, boolean pagesOnly) {
        List<TableStore> dirty = new ArrayList<>();
        for (TableStore table : tables.values()) {
            if (table.isDirty() && include.test(table)) {
                dirty.add(table);
            }
        }
        if (dirty.isEmpty()) {
            if (!catalogDirty || pagesOnly) {
                storage.ensureScrubbed();
                return false;
            }
            storage.ensureScrubbed();
            storage.beginCommit();
            try {
                storage.armScrub();
                DatabaseFile.Allocator draft = storage.draft();
                draft.free(storage.catalogOffset(), storage.catalogSpan());
                storage.publish(snapshotTables(), draft);
                storage.scrubPending();
                storage.finishCommit(transactions, commitMarker, rollbackMarker);
                catalogDirty = false;
                return true;
            } catch (RuntimeException exception) {
                storage.abortCommit();
                if (storage.leaveForRecovery() || exception instanceof SeaException) {
                    throw exception;
                }
                throw new SeaException("failed to commit changes: " + exception.getMessage(), exception);
            }
        }
        storage.ensureScrubbed();
        storage.beginCommit();
        Map<TableStore, Long> oldBase = new HashMap<>();
        Map<TableStore, Long> oldCapacity = new HashMap<>();
        Map<TableStore, TableStore.Plan> plans = new HashMap<>();
        Set<TableStore> relocated = new HashSet<>();
        DatabaseFile.Allocator draft = storage.draft();
        // The catalog sits at the end of the file. Releasing it first lets a
        // growing table extend in place instead of copying every page.
        draft.free(storage.catalogOffset(), storage.catalogSpan());
        List<DatabaseFile.StoredTable> entries = new ArrayList<>();
        boolean published = false;
        boolean reclaimed = false;
        long committedSize = storage.fileSize();
        try {
            storage.armScrub();
            for (TableStore table : tables.values()) {
                if (!dirty.contains(table)) {
                    entries.add(stored(table, null));
                    continue;
                }
                boolean shrink = shouldShrink(table);
                long storedSize = table.logicalSize();
                if (!pagesOnly) {
                    // Commit must persist every live row, including inserts made
                    // after the background checkpoint froze its image.
                    table.abortEncode();
                }
                TableStore.Plan prepared = shrink ? null : table.takePrepared();
                boolean merged = prepared != null && table.finishEncode(prepared);
                if (prepared != null && !merged) {
                    snapshotLsn = false;
                }
                // Rebuilding the table here holds the insert. An open transaction
                // leaves the rows dirty and tries again; commit still writes them.
                TableStore.Plan plan = merged ? prepared : (pagesOnly ? null : (shrink ? table.preparePacked() : table.prepare()));
                if (plan == null) {
                    entries.add(stored(table, null));
                    continue;
                }
                long wanted = DatabaseFile.capacityFor(plan.fileSize, storage.pageSize());
                if (shrink && wanted >= table.capacity()) {
                    shrink = false;
                    plan = table.prepare();
                    wanted = DatabaseFile.capacityFor(plan.fileSize, storage.pageSize());
                }
                plans.put(table, plan);
                oldBase.put(table, table.base());
                oldCapacity.put(table, table.capacity());
                table.armScrub();
                boolean grow = plan.fileSize > table.capacity();
                if (grow && !shrink && draft.extendTail(table.base(), table.capacity(), wanted)) {
                    long extra = wanted - table.capacity();
                    storage.zero(table.base() + table.capacity(), extra);
                    table.retarget(table.base(), wanted);
                    table.writePayload(plan);
                    table.flip(plan);
                    relocated.add(table);
                    entries.add(new DatabaseFile.StoredTable(
                            Relation.catalogName(table.namespace(), table.schema().name), table.base(), wanted, null));
                } else if (grow || shrink) {
                    long capacity = wanted;
                    long base = draft.allocate(capacity);
                    storage.zero(base, capacity);
                    long previousBase = table.base();
                    long previousCapacity = table.capacity();
                    if (grow) {
                        if (merged && plan.relocation != null
                                && plan.sourceBase == previousBase && plan.sourceSize == storedSize) {
                            for (TableStore.Write chunk : plan.relocation) {
                                storage.writeBytes(base + chunk.offset, chunk.bytes);
                            }
                        } else {
                            storage.copy(previousBase, base, storedSize);
                        }
                    }
                    storage.retire(previousBase, previousCapacity);
                    draft.free(previousBase, previousCapacity);
                    table.retarget(base, capacity);
                    if (!grow) {
                        table.plantHeader();
                        reclaimed = true;
                    }
                    table.writePayload(plan);
                    table.flip(plan);
                    relocated.add(table);
                    entries.add(new DatabaseFile.StoredTable(
                            Relation.catalogName(table.namespace(), table.schema().name), base, capacity, null));
                } else {
                    table.writePayload(plan);
                    entries.add(stored(table, new DatabaseFile.PendingFlip(
                            plan.generation, plan.metaOffset, plan.metaLength, plan.targetSlot)));
                }
            }
            storage.publish(entries, draft);
            storage.scrubPending();
            for (TableStore table : dirty) {
                TableStore.Plan plan = plans.get(table);
                if (plan == null) {
                    continue;
                }
                if (!relocated.contains(table)) {
                    table.flip(plan);
                }
                table.scrub(plan);
                if (!table.consumeMerged()) {
                    table.install(plan);
                }
            }
            DatabaseFile.Allocator clean = storage.draft();
            clean.free(storage.catalogOffset(), storage.catalogSpan());
            storage.publish(snapshotTables(), clean);
            storage.scrubPending();
            if (pagesOnly) {
                storage.finishPages();
            } else {
                storage.finishCommit(transactions, commitMarker, rollbackMarker);
                catalogDirty = false;
            }
            published = true;
        } catch (RuntimeException exception) {
            storage.abortCommit();
            if (storage.leaveForRecovery()) {
                throw exception;
            }
            if (published) {
                try {
                    storage.recoverPending();
                    storage.ensureScrubbed();
                    for (TableStore table : dirty) {
                        TableStore.Plan plan = plans.get(table);
                        if (plan != null) {
                            table.scrub(plan);
                        }
                    }
                } catch (RuntimeException ignored) {
                    // the next open zeroes the space left pending
                }
                for (TableStore table : dirty) {
                    TableStore.Plan plan = plans.get(table);
                    if (plan != null) {
                        table.install(plan);
                    }
                }
                return true;
            }
            for (TableStore table : dirty) {
                if (oldBase.containsKey(table)) {
                    table.retarget(oldBase.get(table), oldCapacity.get(table));
                }
                table.truncateToCommitted();
                table.scrubIfDirty();
            }
            storage.ensureScrubbed();
            storage.truncate(committedSize);
            if (exception instanceof SeaException nex) {
                throw nex;
            }
            throw new SeaException("failed to commit changes: " + exception.getMessage(), exception);
        }
        if (reclaimed) {
            compactFile();
        }
        return true;
    }

    private boolean shouldShrink(TableStore table) {
        // A rewrite is smaller than half the file only when free space is most of it.
        // That check uses the free list, so a checkpoint does not copy every row.
        if (table.occupiedBytes() * 2 >= table.logicalSize()) {
            return false;
        }
        long wanted = DatabaseFile.capacityFor(table.packedBytes(), storage.pageSize());
        // An exact half remains: the next insert fills that page and packing would return the same split.
        return wanted < table.capacity() && wanted * 2 < table.capacity();
    }

    private void compactFile() {
        int guard = tables.size() * tables.size() + 1;
        while (guard-- > 0) {
            TableStore candidate = null;
            long destination = -1;
            List<TableStore> ordered = new ArrayList<>(tables.values());
            ordered.sort(Comparator.comparingLong(TableStore::base));
            for (Extent hole : storage.freeExtents()) {
                for (TableStore table : ordered) {
                    long holeEnd = hole.offset + hole.length;
                    if (table.base() == holeEnd && table.capacity() <= hole.length) {
                        candidate = table;
                        destination = hole.offset;
                        break;
                    }
                }
                if (candidate != null) {
                    break;
                }
            }
            if (candidate == null) {
                return;
            }
            long from = candidate.base();
            long capacity = candidate.capacity();
            List<DatabaseFile.StoredTable> entries = new ArrayList<>();
            for (TableStore table : tables.values()) {
                if (table == candidate) {
                    entries.add(new DatabaseFile.StoredTable(
                            Relation.catalogName(table.namespace(), table.schema().name), destination, capacity, null));
                } else {
                    entries.add(stored(table, null));
                }
            }
            storage.relocate(from, destination, capacity, entries);
            candidate.retarget(destination, capacity);
        }
        throw new SeaException("internal failure while compacting the database");
    }

    private void rollback() {
        pending.clear();
        reloadAll();
        inTransaction = false;
        transactionSession = null;
        transactionId = null;
        installed = null;
    }

    private void reloadAll() {
        for (TableStore table : tables.values()) {
            table.reload();
        }
    }

    private Schema compile(String namespace, String name, List<ColDef> definitions, List<String> primary, List<UniqueClause> uniques) {
        List<Column> columns = new ArrayList<>();
        List<String> columnPrimary = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (ColDef definition : definitions) {
            Names.checkName(definition.name(), "column");
            if (!seen.add(Names.fold(definition.name()))) {
                throw new SeaException("duplicate column: " + definition.name());
            }
            if (definition.primaryKey()) {
                columnPrimary.add(definition.name());
            }
            columns.add(column(definition));
        }
        if (columnPrimary.size() > 1) {
            throw new SeaException("composite primary key must be declared as PRIMARY KEY (a, b)");
        }
        if (!columnPrimary.isEmpty() && !primary.isEmpty()) {
            throw new SeaException("duplicate primary key");
        }
        int[] primaryIndexes = resolve(columns, primary.isEmpty() ? columnPrimary : primary, "primary key");
        for (int index : primaryIndexes) {
            Column column = columns.get(index);
            if (!column.notNull) {
                columns.set(index, column.asNotNull());
            }
        }
        Schema schema = new Schema(name, columns, primaryIndexes, List.of(), List.of());
        for (int i = 0; i < definitions.size(); i++) {
            if (definitions.get(i).unique() && !definitions.get(i).primaryKey()) {
                if (columns.get(i).type == DataType.BLOB) {
                    throw new SeaException("BLOB cannot be a key");
                }
                schema = schema.withUnique(new UniqueKey(freshUnique(schema, namespace), new int[]{i}));
            }
        }
        for (UniqueClause clause : uniques) {
            schema = schema.withUnique(buildUnique(schema, namespace, clause.constraint(), clause.columns()));
        }
        return schema;
    }

    private Column column(ColDef definition) {
        Names.checkName(definition.name(), "column");
        DataType type = definition.type();
        int length = definition.length();
        int scale = definition.scale();
        String domainName = definition.domain();
        StoredDomain domain = null;
        if (domainName != null) {
            domain = requireDomain(domainName);
            type = domain.type;
            length = domain.length;
            scale = domain.scale;
        }
        if (definition.generated() != null && (definition.primaryKey() || definition.unique())) {
            throw new SeaException("generated column cannot be a key: " + definition.name());
        }
        String generated = definition.generated();
        if (generated != null) {
            rejectRowExpr(SqlParser.parseExpression(generated));
        }
        String defaultExpr = definition.defaultSource();
        if (defaultExpr == null && domain != null && domain.defaultSource != null && !definition.hasDefault()) {
            defaultExpr = domain.defaultSource;
        }
        if (defaultExpr != null) {
            rejectRowExpr(SqlParser.parseExpression(defaultExpr));
        }
        Object def = null;
        boolean hasDefault = definition.hasDefault() || defaultExpr != null;
        if (definition.hasDefault() && definition.defaultValue() != null && defaultExpr == null) {
            def = Values.coerce(type, length, scale, definition.defaultValue());
        }
        return new Column(definition.name(), type, length, scale, definition.notNull() || definition.primaryKey(),
                definition.autoIncrement(), definition.autoIncrement() ? 1L : 0L, hasDefault, def,
                definition.collation(), domainName, generated, defaultExpr);
    }

    private List<CheckConstraint> tableChecks(Schema schema, CreateTable statement) {
        List<CheckConstraint> checks = new ArrayList<>();
        Set<String> used = new HashSet<>();
        int sequence = 1;
        for (ColDef definition : statement.columns()) {
            if (definition.checkSource() == null) {
                continue;
            }
            String name = definition.checkName() == null ? "CK_" + schema.name + "_" + sequence++ : definition.checkName();
            addCheckSpec(checks, used, name, definition.name(), definition.checkSource());
        }
        for (CheckSpec spec : statement.checks()) {
            String name = spec.name() == null || spec.name().isBlank() ? "CK_" + schema.name + "_" + sequence++ : spec.name();
            addCheckSpec(checks, used, name, spec.column(), spec.expression());
        }
        return checks;
    }

    private void addCheckSpec(List<CheckConstraint> checks, Set<String> used, String name, String column, String expression) {
        Names.checkName(name, "constraint");
        if (!used.add(Names.fold(name))) {
            throw new SeaException("constraint already exists: " + name);
        }
        rejectRowExpr(SqlParser.parseExpression(expression));
        checks.add(new CheckConstraint(name, column, expression));
    }

    private void alterColumnType(AlterColumnType statement) {
        guardDdl(statement.table());
        rejectBorrowed(require(statement.table()));
        mutating(statement.table(), () -> {
            TableStore table = require(statement.table());
            int index = columnIndex(table.schema(), statement.column());
            Column current = table.schema().columns.get(index);
            if (current.generated != null) {
                throw new SeaException("generated column cannot change type: " + current.name);
            }
            table.installSchema(table.schema().withColumn(index, current.retyped(statement.type(), statement.length(), statement.scale())));
            return 0;
        });
    }

    private void alterColumnNull(AlterColumnNull statement) {
        guardDdl(statement.table());
        rejectBorrowed(require(statement.table()));
        mutating(statement.table(), () -> {
            TableStore table = require(statement.table());
            Schema schema = table.schema();
            int index = columnIndex(schema, statement.column());
            if (!statement.notNull()) {
                for (int key : schema.primaryKey) {
                    if (key == index) {
                        throw new SeaException("primary key column cannot drop NOT NULL");
                    }
                }
            }
            if (statement.notNull()) {
                for (StoredRow row : table.scan()) {
                    if (table.value(row, index) == null) {
                        throw new SeaException("column contains NULL: " + statement.column());
                    }
                }
            }
            table.installSchema(schema.withColumn(index, schema.columns.get(index).withNullability(statement.notNull())));
            return 0;
        });
    }

    private void alterColumnDefault(AlterColumnDefault statement) {
        guardDdl(statement.table());
        rejectBorrowed(require(statement.table()));
        mutating(statement.table(), () -> {
            TableStore table = require(statement.table());
            int index = columnIndex(table.schema(), statement.column());
            Column current = table.schema().columns.get(index);
            Column next;
            if (statement.expression() == null) {
                next = current.withDefault(false, null, null);
            } else {
                Expr expr = SqlParser.parseExpression(statement.expression());
                if (expr instanceof Literal literal) {
                    Object value = literal.value() == null ? null
                            : Values.coerce(current.type, current.length, current.scale, literal.value());
                    next = current.withDefault(true, value, null);
                } else {
                    rejectRowExpr(expr);
                    next = current.withDefault(true, null, statement.expression());
                }
            }
            table.installSchema(table.schema().withColumn(index, next));
            return 0;
        });
    }

    private void renameColumn(RenameColumn statement) {
        guardDdl(statement.table());
        rejectBorrowed(require(statement.table()));
        mutating(statement.table(), () -> {
            TableStore table = require(statement.table());
            int index = columnIndex(table.schema(), statement.column());
            Names.checkName(statement.name(), "column");
            if (table.schema().indexOf(statement.name()) >= 0) {
                throw new SeaException("column already exists: " + statement.name());
            }
            String catalog = Relation.catalogName(table.namespace(), table.schema().name);
            table.installSchema(table.schema().withColumn(index, table.schema().columns.get(index).renamed(statement.name())));
            rewriteQuoted(catalog, table.schema().name, statement.column(), statement.name());
            retargetReferencedColumns(table.namespace(), table.schema().name, statement.column(), statement.name());
            return 0;
        });
    }

    private void renameTable(RenameTable statement) {
        TableStore table = require(statement.table());
        guardDdl(statement.table());
        rejectBorrowed(table);
        Relation next = relation(statement.name());
        if (!Names.same(next.schema(), table.namespace())) {
            throw new SeaException("RENAME cannot change the schema");
        }
        Names.checkName(next.name(), "table");
        if (SystemTables.isSystem(next.name()) || tables.containsKey(next.key()) || findView(statement.name()) != null) {
            throw new SeaException("table already exists: " + shown(statement.name()));
        }
        mutating(statement.table(), () -> {
            String oldName = table.schema().name;
            String oldKey = Relation.key(table.namespace(), oldName);
            String oldCatalog = Relation.catalogName(table.namespace(), oldName);
            String newCatalog = Relation.catalogName(next.schema(), next.name());
            tables.remove(oldKey);
            tables.put(next.key(), table);
            renameUndo.add(() -> {
                tables.remove(next.key());
                tables.put(oldKey, table);
            });
            table.installSchema(table.schema().renamed(next.name()));
            rewriteQuoted(oldCatalog, oldName, oldName, next.name());
            retargetReferencedTables(table.namespace(), oldName, next.name());
            systemExecute("UPDATE SEA$PRIVILEGES SET SEA$TABLE_NAME = ? WHERE SEA$TABLE_NAME = ?", newCatalog, oldCatalog);
            systemExecute("UPDATE SEA$VIEWS SET SEA$RELATION_NAME = ? WHERE SEA$RELATION_NAME = ?", newCatalog, oldCatalog);
            systemExecute("UPDATE SEA$TRIGGERS SET SEA$RELATION_NAME = ? WHERE SEA$RELATION_NAME = ?", newCatalog, oldCatalog);
            return 0;
        });
    }

    private void addCheck(AddCheck statement) {
        guardDdl(statement.table());
        rejectBorrowed(require(statement.table()));
        mutating(statement.table(), () -> {
            TableStore table = require(statement.table());
            Schema schema = table.schema();
            String name = statement.name() == null || statement.name().isBlank()
                    ? "CK_" + schema.name + "_" + (schema.checks.size() + 1) : statement.name();
            for (CheckConstraint check : schema.checks) {
                if (Names.same(check.name, name)) {
                    throw new SeaException("constraint already exists: " + name);
                }
            }
            Expr expr = SqlParser.parseExpression(statement.expression());
            rejectRowExpr(expr);
            for (StoredRow row : table.scan()) {
                Object result = evalRow(schema, logical(table, row), expr);
                if (Boolean.FALSE.equals(result)) {
                    throw new SeaException("check constraint failed: " + name);
                }
            }
            List<CheckConstraint> checks = new ArrayList<>(schema.checks);
            checks.add(new CheckConstraint(name, statement.column(), statement.expression()));
            table.installSchema(schema.withChecks(checks));
            return 0;
        });
    }

    private void rewriteQuoted(String catalog, String tableName, String from, String to) {
        rewriteText(SystemTables.VIEWS, "SEA$VIEW_NAME", "SEA$VIEW_SOURCE", "SEA$RELATION_NAME", "SEA$COLUMNS",
                catalog, tableName, from, to);
        rewriteText(SystemTables.TRIGGERS, "SEA$TRIGGER_NAME", "SEA$TRIGGER_SOURCE", "SEA$RELATION_NAME", null,
                catalog, tableName, from, to);
    }

    private void rewriteText(String catalogTable, String keyColumn, String sourceColumn, String relationColumn,
                             String columnsColumn, String catalog, String tableName, String from, String to) {
        TableStore store = systemTable(catalogTable);
        if (store == null) {
            return;
        }
        for (StoredRow row : new ArrayList<>(store.scan())) {
            String relation = text(store, row, relationColumn);
            String source = text(store, row, sourceColumn);
            if (source == null) {
                continue;
            }
            if (!Names.same(relation, catalog) && !SqlText.cites(source, tableName)) {
                continue;
            }
            String updated = SqlText.rename(source, from, to);
            if (!updated.equals(source)) {
                systemExecute("UPDATE " + catalogTable + " SET " + sourceColumn + " = ? WHERE " + keyColumn + " = ?",
                        updated, text(store, row, keyColumn));
            }
            if (columnsColumn != null) {
                String columns = text(store, row, columnsColumn);
                if (columns != null) {
                    String[] parts = columns.split(VIEW_COLUMN_SEPARATOR, -1);
                    boolean changed = false;
                    for (int i = 0; i < parts.length; i++) {
                        if (Names.same(parts[i], from)) {
                            parts[i] = to;
                            changed = true;
                        }
                    }
                    if (changed) {
                        systemExecute("UPDATE " + catalogTable + " SET " + columnsColumn + " = ? WHERE " + keyColumn + " = ?",
                                String.join(VIEW_COLUMN_SEPARATOR, parts), text(store, row, keyColumn));
                    }
                }
            }
        }
    }

    private void retargetReferencedTables(String namespace, String from, String to) {
        for (TableStore other : new ArrayList<>(tables.values())) {
            boolean changed = false;
            List<ForeignKey> keys = new ArrayList<>();
            for (ForeignKey key : other.schema().foreignKeys) {
                if (Names.same(key.referencedSchema, namespace) && Names.same(key.referencedTable, from)) {
                    changed = true;
                    keys.add(new ForeignKey(key.name, key.columns, key.referencedSchema, to, key.referencedColumns,
                            key.indexName, key.descending, key.onUpdate, key.onDelete));
                } else {
                    keys.add(key);
                }
            }
            if (changed) {
                other.installSchema(other.schema().withForeignKeys(keys));
            }
        }
    }

    private void retargetReferencedColumns(String namespace, String table, String from, String to) {
        for (TableStore other : new ArrayList<>(tables.values())) {
            boolean changed = false;
            List<ForeignKey> keys = new ArrayList<>();
            for (ForeignKey key : other.schema().foreignKeys) {
                if (Names.same(key.referencedSchema, namespace) && Names.same(key.referencedTable, table)) {
                    String[] columns = key.referencedColumns.clone();
                    for (int i = 0; i < columns.length; i++) {
                        if (Names.same(columns[i], from)) {
                            columns[i] = to;
                            changed = true;
                        }
                    }
                    keys.add(new ForeignKey(key.name, key.columns, key.referencedSchema, key.referencedTable, columns,
                            key.indexName, key.descending, key.onUpdate, key.onDelete));
                } else {
                    keys.add(key);
                }
            }
            if (changed) {
                other.installSchema(other.schema().withForeignKeys(keys));
            }
        }
    }

    private int columnIndex(Schema schema, String name) {
        int index = schema.indexOf(name);
        if (index < 0) {
            throw new SeaException("column not found: " + name);
        }
        return index;
    }

    private void fillDefaults(Schema schema, Object[] values) {
        for (int i = 0; i < schema.columns.size(); i++) {
            Column column = schema.columns.get(i);
            if (values[i] != Values.MISSING || column.defaultExpr == null) {
                continue;
            }
            Object raw = evalRow(schema, values, SqlParser.parseExpression(column.defaultExpr));
            values[i] = prepared(schema, column, raw);
        }
    }

    private void applyGenerated(Schema schema, Object[] values) {
        for (int i = 0; i < schema.columns.size(); i++) {
            Column column = schema.columns.get(i);
            if (column.generated == null) {
                continue;
            }
            Object raw = evalRow(schema, values, SqlParser.parseExpression(column.generated));
            values[i] = prepared(schema, column, raw);
        }
    }

    private void enforceChecks(Schema schema, Object[] values) {
        for (CheckConstraint check : schema.checks) {
            Object result = evalRow(schema, values, SqlParser.parseExpression(check.expression));
            if (Boolean.FALSE.equals(result)) {
                throw new SeaException("check constraint failed: " + check.name);
            }
        }
        for (Column column : schema.columns) {
            if (column.domain == null) {
                continue;
            }
            StoredDomain domain = findDomain(column.domain);
            if (domain == null || domain.check == null || domain.check.isBlank()) {
                continue;
            }
            int index = schema.indexOf(column.name);
            Object value = values[index] == Values.MISSING ? null : values[index];
            Schema probe = new Schema("VALUE", List.of(new Column("VALUE", column.type, column.length, column.scale,
                    false, false, 0L, false, null)), new int[0], List.of(), List.of());
            Object result = evalRow(probe, new Object[]{value}, SqlParser.parseExpression(domain.check));
            if (Boolean.FALSE.equals(result)) {
                throw new SeaException("check constraint failed: " + column.domain);
            }
        }
    }

    private Object evalRow(Schema schema, Object[] values, Expr expr) {
        Evaluator evaluator = evaluator(null, null);
        evaluator.useView(schema, schema.name);
        Object[] visible = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            visible[i] = values[i] == Values.MISSING ? null : values[i];
        }
        evaluator.bindView(visible);
        return evaluator.eval(expr);
    }

    private void rejectRowExpr(Expr expr) {
        if (expr == null) {
            return;
        }
        switch (expr) {
            case ScalarSubquery ignored -> throw new SeaException("expression cannot use a subquery");
            case InSubquery ignored -> throw new SeaException("expression cannot use a subquery");
            case ExistsExpr ignored -> throw new SeaException("expression cannot use a subquery");
            case WindowExpr ignored -> throw new SeaException("expression cannot use a window");
            case Call call -> {
                if (Evaluator.aggregate(call.name())) {
                    throw new SeaException("expression cannot use an aggregate");
                }
                for (Expr arg : call.args()) {
                    rejectRowExpr(arg);
                }
            }
            case Unary unary -> rejectRowExpr(unary.expr());
            case Binary binary -> {
                rejectRowExpr(binary.left());
                rejectRowExpr(binary.right());
            }
            case IsNull isNull -> rejectRowExpr(isNull.expr());
            case Like like -> {
                rejectRowExpr(like.expr());
                rejectRowExpr(like.pattern());
                rejectRowExpr(like.escape());
            }
            case InList inList -> {
                rejectRowExpr(inList.expr());
                for (Expr value : inList.values()) {
                    rejectRowExpr(value);
                }
            }
            case Between between -> {
                rejectRowExpr(between.expr());
                rejectRowExpr(between.low());
                rejectRowExpr(between.high());
            }
            case CaseExpr caseExpr -> {
                rejectRowExpr(caseExpr.operand());
                for (WhenBranch branch : caseExpr.branches()) {
                    rejectRowExpr(branch.when());
                    rejectRowExpr(branch.then());
                }
                rejectRowExpr(caseExpr.otherwise());
            }
            case CastExpr cast -> rejectRowExpr(cast.expr());
            default -> {
            }
        }
    }

    private void createDomain(CreateDomain statement) {
        ensureCatalog(SystemTables.DOMAINS, SystemTables.domains());
        Relation rel = relation(statement.name());
        Names.checkName(rel.name(), "domain");
        if (findDomain(statement.name()) != null) {
            throw new SeaException("domain already exists: " + shown(statement.name()));
        }
        if (statement.checkSource() != null) {
            rejectRowExpr(SqlParser.parseExpression(statement.checkSource()));
        }
        if (statement.defaultSource() != null) {
            rejectRowExpr(SqlParser.parseExpression(statement.defaultSource()));
        }
        systemExecute("""
                INSERT INTO SEA$DOMAINS (
                    SEA$SCHEMA_NAME, SEA$DOMAIN_NAME, SEA$FIELD_TYPE, SEA$FIELD_LENGTH, SEA$FIELD_SCALE,
                    SEA$DEFAULT_SOURCE, SEA$CHECK_SOURCE, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, rel.schema(), rel.name(), statement.type().name(), statement.length(), statement.scale(),
                statement.defaultSource(), statement.checkSource(), ownerName());
    }

    private void dropDomain(DropDomain statement) {
        StoredDomain domain = findDomain(statement.name());
        if (domain == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("domain not found: " + shown(statement.name()));
        }
        for (TableStore table : tables.values()) {
            for (Column column : table.schema().columns) {
                if (column.domain != null && Names.same(column.domain, domain.name)) {
                    throw new SeaException("domain is in use: " + domain.name);
                }
            }
        }
        systemExecute("DELETE FROM SEA$DOMAINS WHERE SEA$SCHEMA_NAME = ? AND SEA$DOMAIN_NAME = ?",
                domain.schema, domain.name);
    }

    private StoredDomain requireDomain(String name) {
        StoredDomain domain = findDomain(name);
        if (domain == null) {
            throw new SeaException("domain not found: " + shown(name));
        }
        return domain;
    }

    private StoredDomain findDomain(String name) {
        TableStore store = systemTable(SystemTables.DOMAINS);
        if (store == null) {
            return null;
        }
        Relation rel = relation(name);
        for (StoredRow row : store.scan()) {
            if (!Names.same(text(store, row, "SEA$SCHEMA_NAME"), rel.schema())
                    || !Names.same(text(store, row, "SEA$DOMAIN_NAME"), rel.name())) {
                continue;
            }
            String typeName = text(store, row, "SEA$FIELD_TYPE");
            DataType type = DataType.valueOf(typeName);
            int length = (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$FIELD_LENGTH")));
            int scale = (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$FIELD_SCALE")));
            return new StoredDomain(rel.schema(), rel.name(), type, length, scale,
                    text(store, row, "SEA$DEFAULT_SOURCE"), text(store, row, "SEA$CHECK_SOURCE"),
                    text(store, row, "SEA$OWNER_NAME"));
        }
        return null;
    }

    private void createFunction(CreateFunction statement) {
        ensureCatalog(SystemTables.FUNCTIONS, SystemTables.functions());
        ensureCatalog(SystemTables.FUNCTION_PARAMETERS, SystemTables.functionParameters());
        Relation rel = relation(statement.name());
        Names.checkName(rel.name(), "function");
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("reserved name: " + rel.name());
        }
        StoredFunction existing = findFunction(statement.name());
        if (existing != null) {
            if (!statement.orReplace()) {
                throw new SeaException("function already exists: " + shown(statement.name()));
            }
            if (existing.type != statement.type()) {
                throw new SeaException("function return type changed: " + shown(statement.name()));
            }
            guardRoutine("the function", existing.label(), existing.owner, null);
            systemExecute("DELETE FROM SEA$FUNCTION_PARAMETERS WHERE SEA$SCHEMA_NAME = ? AND SEA$FUNCTION_NAME = ?",
                    existing.schema, existing.name);
            insertFunctionParameters(existing.schema, existing.name, statement.inputs());
            systemExecute("""
                    UPDATE SEA$FUNCTIONS SET SEA$FUNCTION_SOURCE = ?
                    WHERE SEA$SCHEMA_NAME = ? AND SEA$FUNCTION_NAME = ?
                    """, statement.source(), existing.schema, existing.name);
            return;
        }
        systemExecute("""
                INSERT INTO SEA$FUNCTIONS (
                    SEA$SCHEMA_NAME, SEA$FUNCTION_NAME, SEA$FUNCTION_SOURCE, SEA$RETURN_TYPE,
                    SEA$RETURN_LENGTH, SEA$RETURN_SCALE, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, rel.schema(), rel.name(), statement.source(), statement.type().name(),
                statement.length(), statement.scale(), ownerName());
        insertFunctionParameters(rel.schema(), rel.name(), statement.inputs());
        recordOwner(routineKey("FUNCTION", rel.schema(), rel.name()));
    }

    private void insertFunctionParameters(String schema, String function, List<ParamDef> parameters) {
        for (int i = 0; i < parameters.size(); i++) {
            ParamDef parameter = parameters.get(i);
            systemExecute("""
                    INSERT INTO SEA$FUNCTION_PARAMETERS (
                        SEA$SCHEMA_NAME, SEA$FUNCTION_NAME, SEA$PARAMETER_NAME, SEA$PARAMETER_NUMBER,
                        SEA$FIELD_TYPE, SEA$FIELD_LENGTH, SEA$FIELD_SCALE
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, schema, function, parameter.name(), i, parameter.type().name(), parameter.length(), parameter.scale());
        }
    }

    private void dropFunction(DropFunction statement) {
        StoredFunction function = findFunction(statement.name());
        if (function == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("function not found: " + shown(statement.name()));
        }
        guardRoutine("the function", function.label(), function.owner, null);
        systemExecute("DELETE FROM SEA$FUNCTION_PARAMETERS WHERE SEA$SCHEMA_NAME = ? AND SEA$FUNCTION_NAME = ?",
                function.schema, function.name);
        systemExecute("DELETE FROM SEA$FUNCTIONS WHERE SEA$SCHEMA_NAME = ? AND SEA$FUNCTION_NAME = ?",
                function.schema, function.name);
        forgetPrivileges(routineKey("FUNCTION", function.schema, function.name));
    }

    private Object callUserFunction(String name, List<Object> args) {
        StoredFunction function = findFunction(name);
        if (function == null) {
            throw new SeaException("unknown function: " + name);
        }
        guardExecute("FUNCTION", function.schema, function.name, function.owner);
        List<ParamDef> inputs = functionInputs(function);
        if (args.size() != inputs.size()) {
            throw new SeaException("function " + function.label() + " expects " + inputs.size() + " arguments");
        }
        Object[] coerced = new Object[args.size()];
        for (int i = 0; i < args.size(); i++) {
            ParamDef input = inputs.get(i);
            coerced[i] = args.get(i) == null ? null : Values.coerce(input.type(), input.length(), input.scale(), args.get(i));
        }
        Routine routine = SqlParser.parseRoutine(function.source);
        String previousPin = pinnedSchema;
        pinnedSchema = function.schema;
        try {
            runRoutine(routine, inputs, List.of(), coerced, false);
            Object value = lastReturn;
            return value == null ? null : Values.coerce(function.type, function.length, function.scale, value);
        } finally {
            pinnedSchema = previousPin;
        }
    }

    private List<ParamDef> functionInputs(StoredFunction function) {
        TableStore store = require(SystemTables.FUNCTION_PARAMETERS);
        List<ParamDef> inputs = new ArrayList<>();
        for (StoredRow row : store.scan()) {
            if (!Names.same(text(store, row, "SEA$SCHEMA_NAME"), function.schema)
                    || !Names.same(text(store, row, "SEA$FUNCTION_NAME"), function.name)) {
                continue;
            }
            inputs.add(new ParamDef(text(store, row, "SEA$PARAMETER_NAME"),
                    DataType.valueOf(text(store, row, "SEA$FIELD_TYPE")),
                    (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$FIELD_LENGTH"))),
                    (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$FIELD_SCALE")))));
        }
        return inputs;
    }

    private StoredFunction findFunction(String name) {
        TableStore store = systemTable(SystemTables.FUNCTIONS);
        if (store == null) {
            return null;
        }
        Relation rel = relation(name);
        for (StoredRow row : store.scan()) {
            if (!Names.same(text(store, row, "SEA$SCHEMA_NAME"), rel.schema())
                    || !Names.same(text(store, row, "SEA$FUNCTION_NAME"), rel.name())) {
                continue;
            }
            return new StoredFunction(rel.schema(), rel.name(), text(store, row, "SEA$FUNCTION_SOURCE"),
                    DataType.valueOf(text(store, row, "SEA$RETURN_TYPE")),
                    (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$RETURN_LENGTH"))),
                    (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$RETURN_SCALE"))),
                    text(store, row, "SEA$OWNER_NAME"));
        }
        return null;
    }

    private SqlResult executeBlock(ExecuteBlock block) {
        Object[] args = new Object[block.inputs().size()];
        Evaluator evaluator = evaluator(null, null);
        for (int i = 0; i < args.length; i++) {
            Object raw = evaluator.eval(block.defaults().get(i));
            ParamDef input = block.inputs().get(i);
            args[i] = raw == null ? null : Values.coerce(input.type(), input.length(), input.scale(), raw);
        }
        Routine routine = SqlParser.parseRoutine(block.source());
        List<Object[]> rows = runRoutine(routine, block.inputs(), block.outputs(), args, !block.outputs().isEmpty());
        if (block.outputs().isEmpty()) {
            return SqlResult.update(0);
        }
        List<ColumnMeta> columns = outputColumns(block.outputs());
        List<Row> result = new ArrayList<>();
        for (Object[] values : rows) {
            result.add(new Row(columns, values));
        }
        return SqlResult.query(columns, result);
    }

    private void grantExecute(String kind, String name, String user, boolean grant) {
        String owner;
        Relation rel = relation(name);
        if (kind.equals("FUNCTION")) {
            StoredFunction function = findFunction(name);
            if (function == null) {
                throw new SeaException("function not found: " + shown(name));
            }
            owner = function.owner;
        } else {
            StoredProcedure procedure = findProcedure(name);
            if (procedure == null) {
                throw new SeaException("procedure not found: " + shown(name));
            }
            owner = procedure.owner();
        }
        String key = routineKey(kind, rel.schema(), rel.name());
        String grantee = normalizeUser(user);
        if (!Names.same(grantee, SystemTables.PUBLIC)) {
            requireActiveUser(grantee);
        }
        if (!grant) {
            systemExecute("""
                    DELETE FROM SEA$PRIVILEGES
                    WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ?
                    """, grantee, key, "EXECUTE");
            return;
        }
        if (actor != null && !actor.admin && !Names.same(actor.user, owner)) {
            throw new SeaException("user " + actor.user + " cannot grant EXECUTE on " + shown(name));
        }
        savePrivilege(grantee, key, "EXECUTE", false, actor == null ? SystemTables.PUBLIC : actor.user);
    }

    private void guardExecute(String kind, String schema, String name, String owner) {
        if (bypass || (actor != null && actor.admin) || (actor != null && Names.same(actor.user, owner))) {
            return;
        }
        String key = routineKey(kind, schema, name);
        if (holds(actor, key, "EXECUTE") || holds(SystemTables.PUBLIC, key, "EXECUTE")
                || (actor != null && actor.role != null && holds(actor.role, key, "EXECUTE"))) {
            return;
        }
        String who = actor == null ? "session" : actor.user;
        throw new SeaException("user " + who + " does not have EXECUTE on " + name);
    }

    private static String routineKey(String kind, String schema, String name) {
        return kind + " " + Relation.catalogName(schema, name);
    }

    private void ensureCatalog(String name, Schema schema) {
        if (systemTable(name) != null) {
            return;
        }
        createStored(schema, Relation.PUBLIC);
    }

    private int[] resolve(List<Column> columns, List<String> names, String kind) {
        int[] indexes = new int[names.size()];
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < names.size(); i++) {
            if (!seen.add(Names.fold(names.get(i)))) {
                throw new SeaException("duplicate column " + kindPlace(kind) + ": " + names.get(i));
            }
            int found = -1;
            for (int column = 0; column < columns.size(); column++) {
                if (Names.same(columns.get(column).name, names.get(i))) {
                    found = column;
                    break;
                }
            }
            if (found < 0) {
                throw new SeaException("column not found " + kindPlace(kind) + ": " + names.get(i));
            }
            indexes[i] = found;
        }
        return indexes;
    }

    private static String kindPlace(String kind) {
        return kind.equals("index") ? "in the index" : "in the " + kind;
    }

    private TableStore require(String name) {
        TableStore table = findTable(name);
        if (table == null) {
            throw new SeaException("table not found: " + shown(name));
        }
        return table;
    }

    private TableStore systemTable(String name) {
        return tables.get(Relation.key(Relation.PUBLIC, name));
    }

    private String activeSchema() {
        if (pinnedSchema != null) {
            return pinnedSchema;
        }
        if (connected != null) {
            return connected.schema();
        }
        return Relation.PUBLIC;
    }

    private String shown(String written) {
        Relation parsed = Relation.parse(written);
        if (parsed.schema() == null) {
            return parsed.name();
        }
        return written;
    }

    private Relation relation(String written) {
        Relation parsed = Relation.parse(written);
        if (parsed.schema() == null) {
            return new Relation(activeSchema(), parsed.name());
        }
        return new Relation(canonicalSchema(parsed.schema()), parsed.name());
    }

    private TableStore findTable(String written) {
        Relation parsed = Relation.parse(written);
        if (parsed.schema() == null) {
            TableStore table = tables.get(Relation.key(activeSchema(), parsed.name()));
            if (table == null && SystemTables.isSystem(parsed.name())) {
                table = systemTable(parsed.name());
            }
            return table;
        }
        return tables.get(Relation.key(canonicalSchema(parsed.schema()), parsed.name()));
    }

    private String canonicalSchema(String name) {
        TableStore table = systemTable(SystemTables.SCHEMAS);
        if (table == null) {
            throw new SeaException("schema not found: " + name);
        }
        int index = table.schema().indexOf("SEA$SCHEMA_NAME");
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof String text && Names.same(text, name)) {
                return text;
            }
        }
        throw new SeaException("schema not found: " + name);
    }

    private ColumnMeta columnMeta(SelectItem item, Schema schema, String namespace, String tableName) {
        DataType type = infer(item.expr(), schema);
        if (item.expr() instanceof ColumnRef) {
            return new ColumnMeta(item.label(), type, namespace, tableName);
        }
        return new ColumnMeta(item.label(), type);
    }

    private static Object[] bind(Object[] parameters, int expected) {
        if (parameters == null) {
            if (expected == 1) {
                return new Object[]{null};
            }
            throw new SeaException("ambiguous use of null; pass the parameters in new Object[]{...}");
        }
        if (parameters.length != expected) {
            throw new SeaException("parameter count does not match the '?' in the SQL");
        }
        return parameters;
    }

    private static Object[] copyParams(Object[] parameters) {
        if (parameters == null) {
            return new Object[0];
        }
        Object[] copy = parameters.clone();
        for (int i = 0; i < copy.length; i++) {
            if (copy[i] instanceof byte[] bytes) {
                copy[i] = bytes.clone();
            }
        }
        return copy;
    }

    private void loadTables() {
        for (DatabaseFile.StoredTable region : storage.tables()) {
            TableStore store = TableStore.open(storage, region);
            Relation id;
            try {
                id = Relation.fromCatalog(region.name(), store.schema().name);
            } catch (RuntimeException exception) {
                store.close();
                throw exception;
            }
            store.namespace(id.schema());
            String key = id.key();
            if (tables.containsKey(key)) {
                store.close();
                throw new SeaException("duplicate table: " + store.schema().name);
            }
            tables.put(key, store);
        }
    }

    int pageSize() {
        return storage.pageSize();
    }

    long cacheMaximum() {
        return storage.cacheMaximum();
    }

    private void ensureSystem(DatabaseOptions options) {
        boolean any = false;
        boolean missing = false;
        for (Schema schema : SystemTables.schemas()) {
            if (systemTable(schema.name) != null) {
                any = true;
            } else {
                missing = true;
            }
        }
        if (any && missing) {
            throw new SeaException("incomplete system tables");
        }
        if (any) {
            return;
        }
        storage.ensureScrubbed();
        storage.beginCommit();
        DatabaseFile.Allocator draft = storage.draft();
        try {
            storage.armScrub();
            List<Schema> created = new ArrayList<>(SystemTables.schemas());
            created.add(SystemTables.statistics());
            created.add(SystemTables.columnPrivileges());
            created.add(SystemTables.remarks());
            for (Schema schema : created) {
                long capacity = DatabaseFile.capacityFor(Format.HEADER_SIZE + 1024, storage.pageSize());
                long base = draft.allocate(capacity);
                storage.zero(base, capacity);
                TableStore store = TableStore.create(storage, base, capacity, schema);
                store.namespace(Relation.PUBLIC);
                tables.put(Relation.key(Relation.PUBLIC, schema.name), store);
            }
            draft.free(storage.catalogOffset(), storage.catalogSpan());
            storage.publish(snapshotTables(), draft);
            storage.scrubPending();
            storage.finishCommit();
        } catch (RuntimeException exception) {
            storage.abortCommit();
            if (storage.leaveForRecovery()) {
                throw exception;
            }
            tables.clear();
            storage.ensureScrubbed();
            storage.truncate(storage.fileSize());
            throw exception;
        }
        seedUsers(options);
    }

    private void seedUsers(DatabaseOptions options) {
        inTransaction = true;
        try {
            systemExecute("""
                    INSERT INTO SEA$DATABASE (
                        SEA$DATABASE_NAME, SEA$DESCRIPTION, SEA$OWNER_NAME, SEA$CREATED,
                        SEA$FORMAT, SEA$SQL_DIALECT, SEA$PAGE_SIZE
                    ) VALUES (?, ?, ?, ?, ?, 1, ?)
                    """, options.name(), options.description(), options.user(), LocalDateTime.now().withNano(0),
                    Format.DB_VERSION, storage.pageSize());
            systemExecute("INSERT INTO SEA$CONFIG (SEA$KEY, SEA$VALUE) VALUES ('Auth.Hash', 'bcrypt')");
            systemExecute("INSERT INTO SEA$CONFIG (SEA$KEY, SEA$VALUE) VALUES ('Default.CharacterSet', ?)", options.charset());
            seedSchemas();
            systemExecute("""
                    INSERT INTO SEA$USERS (SEA$USER_NAME, SEA$PASSWORD, SEA$FIRST_NAME, SEA$ACTIVE, SEA$ADMIN)
                    VALUES (?, ?, ?, TRUE, TRUE)
                    """, options.user(), options.password(), options.user());
            inTransaction = false;
            flush();
        } catch (RuntimeException exception) {
            try {
                rollback();
            } catch (RuntimeException ignored) {
                inTransaction = false;
            }
            throw exception;
        }
    }

    private void closeStaleConnections() {
        maintainConnections("DELETE FROM SEA$CONNECTIONS");
    }

    private Account authenticate(String user, String password) {
        if (user == null || user.isBlank() || password == null || password.isEmpty()) {
            throw new SeaException("user and password are required");
        }
        String name = user.trim().toUpperCase(Locale.ROOT);
        SqlResult result = systemExecute(
                "SELECT SEA$PASSWORD, SEA$ACTIVE, SEA$ADMIN FROM SEA$USERS WHERE SEA$USER_NAME = ?", name);
        if (result.size() != 1) {
            Password.rejectUnknown(password);
            throw new SeaException("invalid user or password");
        }
        Row row = result.rows().get(0);
        boolean active = Boolean.TRUE.equals(row.getBoolean("SEA$ACTIVE"));
        if (!Password.verify(row.getString("SEA$PASSWORD"), password) || !active) {
            throw new SeaException("invalid user or password");
        }
        return new Account(name, Boolean.TRUE.equals(row.getBoolean("SEA$ADMIN")));
    }

    private Session attach(Account account, String role, RemoteClient remote) {
        RemoteClient origin = remote == null ? RemoteClient.local() : remote;
        maintainConnections("""
                INSERT INTO SEA$CONNECTIONS (
                    SEA$USER_NAME, SEA$CONNECTED_AT, SEA$STATE,
                    SEA$REMOTE_PROTOCOL, SEA$REMOTE_ADDRESS, SEA$REMOTE_HOST,
                    SEA$REMOTE_OS_USER, SEA$REMOTE_PROCESS
                ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?, ?)
                """, account.user, LocalDateTime.now().withNano(0),
                clip(origin.protocol(), 16), clip(origin.address(), 255), clip(origin.host(), 255),
                clip(origin.osUser(), 255), clip(origin.process(), 255));
        TableStore table = require(SystemTables.CONNECTIONS);
        Column id = table.schema().columns.get(table.schema().indexOf("SEA$ATTACHMENT_ID"));
        Session session = new Session(account.user, account.admin, id.nextIdentity - 1, role);
        attachments.put(session.attachmentId, session);
        return session;
    }

    private void maintainConnections(String sql, Object... params) {
        TableStore table = require(SystemTables.CONNECTIONS);
        TableStore.Snapshot snapshot = table.savepoint();
        long oldBase = table.base();
        long oldCapacity = table.capacity();
        boolean previousBypass = bypass;
        boolean previousSuppress = suppressCapture;
        boolean previousTx = inTransaction;
        bypass = true;
        suppressCapture = true;
        inTransaction = true;
        try {
            execute(null, sql, params);
            flushMatching(candidate -> Names.same(candidate.schema().name, SystemTables.CONNECTIONS));
        } catch (RuntimeException exception) {
            table.retarget(oldBase, oldCapacity);
            table.restore(snapshot);
            throw exception;
        } finally {
            bypass = previousBypass;
            suppressCapture = previousSuppress;
            inTransaction = previousTx;
        }
    }

    private SqlResult systemExecute(String sql, Object... params) {
        boolean previous = bypass;
        boolean previousSuppress = suppressCapture;
        Object[] previousParams = this.params;
        bypass = true;
        suppressCapture = true;
        try {
            return execute(null, sql, params);
        } finally {
            bypass = previous;
            suppressCapture = previousSuppress;
            this.params = previousParams;
        }
    }

    private void createStored(Schema schema, String namespace) {
        // pending already holds this CREATE. It has to be in the log before the
        // catalog checkpoint, or the next open replays a table these pages already have.
        spillPending(transactionSession != null ? transactionSession : actor);
        storage.ensureScrubbed();
        storage.beginCommit();
        try {
            storage.armScrub();
            DatabaseFile.Allocator draft = storage.draft();
            long capacity = DatabaseFile.capacityFor(Format.HEADER_SIZE + 1024, storage.pageSize());
            long base = draft.allocate(capacity);
            storage.zero(base, capacity);
            TableStore store = TableStore.create(storage, base, capacity, schema);
            store.namespace(namespace);
            String key = Relation.key(namespace, schema.name);
            tables.put(key, store);
            draft.free(storage.catalogOffset(), storage.catalogSpan());
            storage.publish(snapshotTables(), draft);
            storage.scrubPending();
            storage.finishCommit();
        } catch (RuntimeException exception) {
            storage.abortCommit();
            if (storage.leaveForRecovery()) {
                throw exception;
            }
            tables.remove(Relation.key(namespace, schema.name));
            storage.ensureScrubbed();
            storage.truncate(storage.fileSize());
            throw exception;
        }
    }

    private void publishLayout(long freedOffset, long freedLength) {
        storage.ensureScrubbed();
        storage.beginCommit();
        try {
            storage.armScrub();
            DatabaseFile.Allocator draft = storage.draft();
            storage.retire(freedOffset, freedLength);
            draft.free(freedOffset, freedLength);
            draft.free(storage.catalogOffset(), storage.catalogSpan());
            storage.publish(snapshotTables(), draft);
            storage.scrubPending();
            storage.finishCommit();
        } catch (RuntimeException exception) {
            storage.abortCommit();
            throw exception;
        }
        compactFile();
    }

    private List<DatabaseFile.StoredTable> snapshotTables() {
        List<DatabaseFile.StoredTable> entries = new ArrayList<>();
        for (TableStore table : tables.values()) {
            entries.add(stored(table, null));
        }
        return entries;
    }

    private static DatabaseFile.StoredTable stored(TableStore table, DatabaseFile.PendingFlip pending) {
        return new DatabaseFile.StoredTable(
                Relation.catalogName(table.namespace(), table.schema().name), table.base(), table.capacity(), pending);
    }

    private void guardDdl(String table) {
        if (SystemTables.isSystem(Relation.parse(table).name())) {
            throw new SeaException("system table cannot be altered: " + shown(table));
        }
        if (findTable(table) == null) {
            return;
        }
        if (bypass || (actor != null && actor.admin) || owns(table)) {
            return;
        }
        throw new SeaException("only the owner or the DBA can alter the structure of " + shown(table));
    }

    private void guardMutation(String table, String privilege) {
        String bare = Relation.parse(table).name();
        if (SystemTables.isSystem(bare)) {
            if (bypass) {
                return;
            }
            if (SystemTables.readOnly(bare)) {
                throw new SeaException("system table is read-only: " + shown(table));
            }
            if (actor == null || !actor.admin) {
                throw new SeaException("only the DBA can alter table " + shown(table));
            }
            return;
        }
        guardAccess(table, privilege);
    }

    private void guardAccess(String table, String privilege) {
        if (bypass || (actor != null && actor.admin) || SystemTables.isSystem(Relation.parse(table).name())) {
            return;
        }
        if (findTable(table) == null && findView(table) == null) {
            return;
        }
        if (owns(table) || holds(actor, table, privilege) || holds(SystemTables.PUBLIC, table, privilege)
                || (actor != null && actor.role != null && holds(actor.role, table, privilege))
                || columnHeld(table, privilege)) {
            return;
        }
        String who = actor == null ? "session" : actor.user;
        throw new SeaException("user " + who + " does not have " + privilege + " on " + shown(table));
    }

    private void grant(Grant statement) {
        String table = userTable(statement.table());
        String grantee = normalizeUser(statement.user());
        if (!Names.same(grantee, SystemTables.PUBLIC) && findRole(grantee) == null) {
            requireActiveUser(grantee);
        }
        String grantor = actor == null ? SystemTables.PUBLIC : actor.user;
        List<String> columns = statement.columns() == null ? List.of() : statement.columns();
        if (!columns.isEmpty()) {
            TableStore target = findTable(table);
            for (String column : columns) {
                if (target == null || target.schema().indexOf(column) < 0) {
                    throw new SeaException("column not found: " + column);
                }
            }
        }
        for (String privilege : statement.privileges()) {
            if (!canGrant(table, privilege)) {
                throw new SeaException("user " + grantor + " cannot grant " + privilege + " on " + table);
            }
            if (columns.isEmpty()) {
                savePrivilege(grantee, table, privilege, statement.grantOption(), grantor);
            } else {
                for (String column : columns) {
                    saveColumnPrivilege(grantee, table, privilege, column, statement.grantOption(), grantor);
                }
            }
        }
    }

    private void revoke(Revoke statement) {
        String table = userTable(statement.table());
        String grantee = normalizeUser(statement.user());
        List<String> columns = statement.columns() == null ? List.of() : statement.columns();
        for (String privilege : statement.privileges()) {
            if (!canRevoke(table, grantee, privilege)) {
                String who = actor == null ? "session" : actor.user;
                throw new SeaException("user " + who + " cannot revoke " + privilege + " on " + table);
            }
            if (columns.isEmpty()) {
                systemExecute("""
                        DELETE FROM SEA$PRIVILEGES
                        WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ?
                        """, grantee, canonicalTable(table), privilege);
            } else {
                ensureColumnPrivileges();
                for (String column : columns) {
                    systemExecute("""
                            DELETE FROM SEA$COLUMN_PRIVILEGES
                            WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ? AND SEA$COLUMN_NAME = ?
                            """, grantee, canonicalTable(table), privilege, Names.fold(column));
                }
            }
        }
    }

    private void recordOwner(String table) {
        if (actor == null) {
            return;
        }
        savePrivilege(actor.user, table, SystemTables.OWNER, true, actor.user);
    }

    private void forgetPrivileges(String table) {
        systemExecute("DELETE FROM SEA$PRIVILEGES WHERE SEA$TABLE_NAME = ?", table);
        if (systemTable(SystemTables.COLUMN_PRIVILEGES) != null) {
            systemExecute("DELETE FROM SEA$COLUMN_PRIVILEGES WHERE SEA$TABLE_NAME = ?", table);
        }
    }

    private void saveColumnPrivilege(String user, String table, String privilege, String column,
                                     boolean grantOption, String grantor) {
        TableStore target = findTable(table);
        if (target == null || target.schema().indexOf(column) < 0) {
            throw new SeaException("column not found: " + column);
        }
        ensureColumnPrivileges();
        String storedTable = canonicalTable(table);
        String storedColumn = Names.fold(column);
        SqlResult existing = systemExecute("""
                SELECT SEA$COLUMN_NAME FROM SEA$COLUMN_PRIVILEGES
                WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ? AND SEA$COLUMN_NAME = ?
                """, user, storedTable, privilege, storedColumn);
        if (existing.size() == 0) {
            systemExecute("""
                    INSERT INTO SEA$COLUMN_PRIVILEGES (
                        SEA$USER_NAME, SEA$TABLE_NAME, SEA$PRIVILEGE, SEA$COLUMN_NAME, SEA$GRANT_OPTION, SEA$GRANTOR
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """, user, storedTable, privilege, storedColumn, grantOption, grantor);
            return;
        }
        systemExecute("""
                UPDATE SEA$COLUMN_PRIVILEGES
                SET SEA$GRANT_OPTION = ?, SEA$GRANTOR = ?
                WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ? AND SEA$COLUMN_NAME = ?
                """, grantOption, grantor, user, storedTable, privilege, storedColumn);
    }

    private void ensureColumnPrivileges() {
        if (systemTable(SystemTables.COLUMN_PRIVILEGES) != null) {
            return;
        }
        if (currentTxn() != null && !suppressCapture && !storage.leaveForRecovery()) {
            checkpoint(true);
        }
        createStored(SystemTables.columnPrivileges(), Relation.PUBLIC);
    }

    private boolean columnHeld(String table, String privilege) {
        if (actor == null) {
            return false;
        }
        if (anyColumn(actor.user, table, privilege) || anyColumn(SystemTables.PUBLIC, table, privilege)) {
            return true;
        }
        return actor.role != null && anyColumn(actor.role, table, privilege);
    }

    private boolean anyColumn(String user, String table, String privilege) {
        return !grantedColumns(user, table, privilege).isEmpty();
    }

    private Set<String> columnScope(String table, String privilege) {
        if (bypass || (actor != null && actor.admin) || SystemTables.isSystem(Relation.parse(table).name())) {
            return null;
        }
        if (owns(table) || holds(actor, table, privilege) || holds(SystemTables.PUBLIC, table, privilege)
                || (actor != null && actor.role != null && holds(actor.role, table, privilege))) {
            return null;
        }
        Set<String> columns = new HashSet<>();
        if (actor != null) {
            columns.addAll(grantedColumns(actor.user, table, privilege));
            columns.addAll(grantedColumns(SystemTables.PUBLIC, table, privilege));
            if (actor.role != null) {
                columns.addAll(grantedColumns(actor.role, table, privilege));
            }
        }
        return columns;
    }

    private Set<String> whereScope(String table, String privilege) {
        Set<String> read = columnScope(table, "SELECT");
        Set<String> write = columnScope(table, privilege);
        if (read == null || write == null) {
            return null;
        }
        Set<String> either = new HashSet<>(read);
        either.addAll(write);
        return either;
    }

    private Set<String> grantedColumns(String user, String table, String privilege) {
        Set<String> columns = new HashSet<>();
        TableStore store = systemTable(SystemTables.COLUMN_PRIVILEGES);
        if (store == null || user == null) {
            return columns;
        }
        String stored = grantName(table);
        Schema schema = store.schema();
        int userIndex = schema.indexOf("SEA$USER_NAME");
        int tableIndex = schema.indexOf("SEA$TABLE_NAME");
        int privilegeIndex = schema.indexOf("SEA$PRIVILEGE");
        int columnIndex = schema.indexOf("SEA$COLUMN_NAME");
        for (StoredRow row : store.scan()) {
            Object userValue = store.value(row, userIndex);
            Object tableValue = store.value(row, tableIndex);
            Object privilegeValue = store.value(row, privilegeIndex);
            Object columnValue = store.value(row, columnIndex);
            if (userValue instanceof String userText && Names.same(userText, user)
                    && tableValue instanceof String tableText && Names.same(tableText, stored)
                    && privilegeValue instanceof String privilegeText && privilegeText.equals(privilege)
                    && columnValue instanceof String columnText) {
                columns.add(Names.fold(columnText));
            }
        }
        return columns;
    }

    private void guardWritten(String table, String privilege, List<String> columns, Schema schema) {
        Set<String> allowed = columnScope(table, privilege);
        if (allowed == null) {
            return;
        }
        List<String> targets = columns == null || columns.isEmpty()
                ? schema.columns.stream().map(column -> column.name).toList()
                : columns;
        for (String column : targets) {
            if (schema.indexOf(column) >= 0 && !allowed.contains(Names.fold(column))) {
                throw new SeaException("column is not granted: " + column);
            }
        }
    }

    private void guardColumnUse(Schema schema, Set<String> allowed, List<SelectItem> items, Expr where) {
        if (allowed == null) {
            return;
        }
        java.util.function.Consumer<ColumnRef> check = ref -> {
            if (schema.indexOf(ref.name()) >= 0 && !allowed.contains(Names.fold(ref.name()))) {
                throw new SeaException("column is not granted: " + ref.name());
            }
        };
        for (SelectItem item : items) {
            visitColumns(item.expr(), check);
        }
        visitColumns(where, check);
    }

    private void visitColumns(Expr expr, java.util.function.Consumer<ColumnRef> columns) {
        switch (expr) {
            case null -> {
            }
            case ColumnRef ref -> columns.accept(ref);
            case Unary unary -> visitColumns(unary.expr(), columns);
            case Binary binary -> {
                visitColumns(binary.left(), columns);
                visitColumns(binary.right(), columns);
            }
            case IsNull isNull -> visitColumns(isNull.expr(), columns);
            case Like like -> {
                visitColumns(like.expr(), columns);
                visitColumns(like.pattern(), columns);
                visitColumns(like.escape(), columns);
            }
            case InList in -> {
                visitColumns(in.expr(), columns);
                for (Expr value : in.values()) {
                    visitColumns(value, columns);
                }
            }
            case Between between -> {
                visitColumns(between.expr(), columns);
                visitColumns(between.low(), columns);
                visitColumns(between.high(), columns);
            }
            case Call call -> {
                for (Expr arg : call.args()) {
                    visitColumns(arg, columns);
                }
            }
            case CastExpr cast -> visitColumns(cast.expr(), columns);
            case CaseExpr searched -> {
                visitColumns(searched.operand(), columns);
                for (WhenBranch branch : searched.branches()) {
                    visitColumns(branch.when(), columns);
                    visitColumns(branch.then(), columns);
                }
                visitColumns(searched.otherwise(), columns);
            }
            case WindowExpr window -> {
                visitColumns(window.call(), columns);
                for (Expr part : window.partition()) {
                    visitColumns(part, columns);
                }
                for (OrderItem item : window.orderBy()) {
                    visitColumns(item.expr(), columns);
                }
            }
            default -> {
            }
        }
    }

    private void armPace(Session session) {
        int seconds = Pace.seconds(configInt("Command.Timeout"), requestedTimeout);
        long deadline = seconds > 0 ? System.currentTimeMillis() + seconds * 1000L : 0L;
        Pace.arm(session, deadline);
    }

    private int configInt(String key) {
        String text = configValue(key);
        if (text == null || text.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException exception) {
            return 0;
        }
    }

    private String configValue(String key) {
        TableStore table = systemTable(SystemTables.CONFIG);
        if (table == null) {
            return null;
        }
        int keyIndex = table.schema().indexOf("SEA$KEY");
        int valueIndex = table.schema().indexOf("SEA$VALUE");
        for (StoredRow row : table.scan()) {
            Object name = table.value(row, keyIndex);
            if (name instanceof String text && Names.same(text, key)) {
                Object value = table.value(row, valueIndex);
                return value == null ? null : value.toString();
            }
        }
        return null;
    }

    private void audit(String sql, boolean ok) {
        if (!"on".equalsIgnoreCase(configValue("Audit.Log"))) {
            return;
        }
        String user = actor != null ? actor.user : "";
        long connection = connected != null ? connected.attachmentId : 0L;
        String text = sql == null ? "" : sql.replace('\r', ' ').replace('\n', ' ');
        String line = java.time.Instant.now() + "\t" + user + "\t" + connection + "\t" + (ok ? "ok" : "fail") + "\t" + text;
        try {
            Files.writeString(file.resolveSibling(file.getFileName().toString() + ".audit"),
                    line + System.lineSeparator(), java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException exception) {
            if (ok) {
                throw new SeaException("could not write the audit log");
            }
        }
    }

    private void savePrivilege(String user, String table, String privilege, boolean grantOption, String grantor) {
        String storedTable = canonicalTable(table);
        if (storedTable.length() > 128) {
            throw new SeaException("qualified name exceeds 128 characters");
        }
        SqlResult existing = systemExecute("""
                SELECT SEA$PRIVILEGE FROM SEA$PRIVILEGES
                WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ?
                """, user, storedTable, privilege);
        if (existing.size() == 0) {
            systemExecute("""
                    INSERT INTO SEA$PRIVILEGES (
                        SEA$USER_NAME, SEA$TABLE_NAME, SEA$PRIVILEGE, SEA$GRANT_OPTION, SEA$GRANTOR
                    ) VALUES (?, ?, ?, ?, ?)
                    """, user, storedTable, privilege, grantOption, grantor);
            return;
        }
        systemExecute("""
                UPDATE SEA$PRIVILEGES
                SET SEA$GRANT_OPTION = ?, SEA$GRANTOR = ?
                WHERE SEA$USER_NAME = ? AND SEA$TABLE_NAME = ? AND SEA$PRIVILEGE = ?
                """, grantOption, grantor, user, storedTable, privilege);
    }

    private String userTable(String name) {
        if (SystemTables.isSystem(Relation.parse(name).name())) {
            throw new SeaException("there is no authorization on system table: " + shown(name));
        }
        TableStore table = findTable(name);
        if (table != null) {
            return Relation.catalogName(table.namespace(), table.schema().name);
        }
        StoredView view = findView(name);
        if (view != null) {
            return view.storedKey();
        }
        throw new SeaException("table or view not found: " + shown(name));
    }

    private String canonicalTable(String name) {
        return grantName(name);
    }

    private String grantName(String written) {
        TableStore table = findTable(written);
        if (table != null) {
            return Relation.catalogName(table.namespace(), table.schema().name);
        }
        StoredView view = findView(written);
        if (view != null) {
            return view.storedKey();
        }
        Relation rel = relation(written);
        return Relation.catalogName(rel.schema(), rel.name());
    }

    private void requireActiveUser(String name) {
        SqlResult rows = systemExecute(
                "SELECT SEA$ACTIVE FROM SEA$USERS WHERE SEA$USER_NAME = ?", name);
        if (rows.size() != 1 || !Boolean.TRUE.equals(rows.rows().get(0).getBoolean(0))) {
            throw new SeaException("user not found: " + name);
        }
    }

    private void seedSchemas() {
        if (schemaExists(Relation.PUBLIC)) {
            return;
        }
        String owner = "DBA";
        TableStore database = systemTable(SystemTables.DATABASE);
        if (database != null) {
            int index = database.schema().indexOf("SEA$OWNER_NAME");
            for (StoredRow row : database.scan()) {
                Object value = database.value(row, index);
                if (value instanceof String text && !text.isBlank()) {
                    owner = text;
                    break;
                }
            }
        }
        systemExecute(
                "INSERT INTO SEA$SCHEMAS (SEA$SCHEMA_NAME, SEA$OWNER_NAME) VALUES (?, ?)",
                Relation.PUBLIC, owner);
    }

    private boolean schemaExists(String name) {
        TableStore table = systemTable(SystemTables.SCHEMAS);
        if (table == null) {
            return false;
        }
        int index = table.schema().indexOf("SEA$SCHEMA_NAME");
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof String text && Names.same(text, name)) {
                return true;
            }
        }
        return false;
    }

    private void createSchema(CreateSchema statement) {
        Names.checkName(statement.name(), "schema");
        if (SystemTables.isSystem(statement.name())) {
            throw new SeaException("reserved schema name: " + statement.name());
        }
        if (schemaExists(statement.name())) {
            if (statement.ifNotExists()) {
                return;
            }
            throw new SeaException("schema already exists: " + statement.name());
        }
        systemExecute(
                "INSERT INTO SEA$SCHEMAS (SEA$SCHEMA_NAME, SEA$OWNER_NAME) VALUES (?, ?)",
                statement.name(), ownerName());
    }

    private void dropSchema(DropSchema statement) {
        if (Names.same(statement.name(), Relation.PUBLIC)) {
            throw new SeaException("the public schema cannot be removed");
        }
        if (!schemaExists(statement.name())) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("schema not found: " + statement.name());
        }
        String schema = canonicalSchema(statement.name());
        for (Session session : attachments.values()) {
            if (!session.closed() && Names.same(session.schema(), schema)) {
                throw new SeaException("schema is in use: " + schema);
            }
        }
        List<TableStore> tablesInSchema = new ArrayList<>();
        for (TableStore table : tables.values()) {
            if (Names.same(table.namespace(), schema) && !SystemTables.isSystem(table.schema().name)) {
                tablesInSchema.add(table);
            }
        }
        boolean occupied = !tablesInSchema.isEmpty();
        for (StoredView view : allViews()) {
            if (Names.same(view.schema(), schema) || dependsOnSchema(view, schema)) {
                occupied = true;
                break;
            }
        }
        List<StoredProcedure> proceduresInSchema = new ArrayList<>();
        for (StoredProcedure procedure : allProcedures()) {
            if (Names.same(procedure.schema(), schema)) {
                proceduresInSchema.add(procedure);
            }
        }
        if (!proceduresInSchema.isEmpty()) {
            occupied = true;
        }
        List<String> generatorsInSchema = generatorNames(schema);
        if (!generatorsInSchema.isEmpty()) {
            occupied = true;
        }
        if (occupied && !statement.cascade()) {
            throw new SeaException("schema is not empty: " + schema);
        }
        for (StoredProcedure procedure : proceduresInSchema) {
            deleteProcedure(procedure.schema(), procedure.name());
        }
        for (String generator : generatorsInSchema) {
            deleteGenerator(schema, generator);
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (StoredView view : allViews()) {
                if (Names.same(view.schema(), schema) || dependsOnSchema(view, schema)) {
                    deleteCatalog(SystemTables.VIEWS, "SEA$VIEW_NAME", view.storedKey());
                    forgetPrivileges(view.storedKey());
                    changed = true;
                    break;
                }
            }
        }
        for (TableStore table : tablesInSchema) {
            drop(new DropTable(false, Relation.catalogName(table.namespace(), table.schema().name)));
        }
        deleteCatalog(SystemTables.SCHEMAS, "SEA$SCHEMA_NAME", schema);
    }

    private boolean dependsOnSchema(StoredView view, String schema) {
        String key = view.relation();
        if (key == null) {
            return false;
        }
        Relation parsed = key.indexOf('.') >= 0 ? Relation.parse(key) : new Relation(Relation.PUBLIC, key);
        String relSchema = parsed.schema() == null ? Relation.PUBLIC : parsed.schema();
        return Names.same(relSchema, schema);
    }

    private void setSchema(SetSchema statement) {
        if (connected == null) {
            throw new SeaException("there is no session to set the schema");
        }
        connected.schema(canonicalSchema(statement.name()));
    }

    private void createRole(CreateRole statement) {
        String name = normalizeRole(statement.name());
        if (name == null) {
            throw new SeaException("reserved name: NONE");
        }
        if (SystemTables.isSystem(name) || Names.same(name, SystemTables.PUBLIC)) {
            throw new SeaException("reserved name: " + name);
        }
        if (findRole(name) != null) {
            if (statement.ifNotExists()) {
                return;
            }
            throw new SeaException("role already exists: " + name);
        }
        if (userExists(name)) {
            throw new SeaException("a user with this name already exists: " + name);
        }
        systemExecute("INSERT INTO SEA$ROLES (SEA$ROLE_NAME, SEA$OWNER_NAME) VALUES (?, ?)", name, ownerName());
    }

    private void dropRole(DropRole statement) {
        String name = normalizeRole(statement.name());
        StoredRow row = name == null ? null : findRole(name);
        if (row == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("role not found: " + statement.name());
        }
        TableStore table = require(SystemTables.ROLES);
        guardRoutine("the role", name, text(table, row, "SEA$OWNER_NAME"), null);
        systemExecute("DELETE FROM SEA$ROLE_MEMBERS WHERE SEA$ROLE_NAME = ?", name);
        systemExecute("DELETE FROM SEA$PRIVILEGES WHERE SEA$USER_NAME = ?", name);
        deleteCatalog(SystemTables.ROLES, "SEA$ROLE_NAME", name);
    }

    private void grantRole(GrantRole statement) {
        String role = requireRole(statement.role());
        String user = normalizeUser(statement.user());
        if (!Names.same(user, SystemTables.PUBLIC)) {
            requireActiveUser(user);
        }
        String owner = text(require(SystemTables.ROLES), findRole(role), "SEA$OWNER_NAME");
        if (!canGrantRole(role, owner)) {
            String who = actor == null ? "session" : actor.user;
            throw new SeaException("user " + who + " cannot grant role " + role);
        }
        String grantor = actor == null ? SystemTables.PUBLIC : actor.user;
        if (memberRow(role, user) != null) {
            systemExecute("""
                    UPDATE SEA$ROLE_MEMBERS
                    SET SEA$ADMIN_OPTION = ?, SEA$GRANTOR = ?
                    WHERE SEA$ROLE_NAME = ? AND SEA$USER_NAME = ?
                    """, statement.adminOption(), grantor, role, user);
            return;
        }
        systemExecute("""
                INSERT INTO SEA$ROLE_MEMBERS (SEA$ROLE_NAME, SEA$USER_NAME, SEA$ADMIN_OPTION, SEA$GRANTOR)
                VALUES (?, ?, ?, ?)
                """, role, user, statement.adminOption(), grantor);
    }

    private void revokeRole(RevokeRole statement) {
        String role = requireRole(statement.role());
        String user = normalizeUser(statement.user());
        String owner = text(require(SystemTables.ROLES), findRole(role), "SEA$OWNER_NAME");
        if (!canGrantRole(role, owner)) {
            String who = actor == null ? "session" : actor.user;
            throw new SeaException("user " + who + " cannot revoke role " + role);
        }
        if (memberRow(role, user) == null) {
            throw new SeaException("user " + user + " does not have role " + role);
        }
        if (statement.adminOptionOnly()) {
            systemExecute("""
                    UPDATE SEA$ROLE_MEMBERS SET SEA$ADMIN_OPTION = FALSE
                    WHERE SEA$ROLE_NAME = ? AND SEA$USER_NAME = ?
                    """, role, user);
            return;
        }
        systemExecute("DELETE FROM SEA$ROLE_MEMBERS WHERE SEA$ROLE_NAME = ? AND SEA$USER_NAME = ?", role, user);
    }

    private void dropConnection(DropConnection statement) {
        Object value = evaluator(null, null).eval(statement.attachment());
        long id;
        try {
            id = Values.toLong(value);
        } catch (SeaException exception) {
            throw new SeaException("DROP CONNECTION expects the connection number");
        }
        SqlResult rows = systemExecute(
                "SELECT SEA$USER_NAME, SEA$STATE FROM SEA$CONNECTIONS WHERE SEA$ATTACHMENT_ID = ?", id);
        if (rows.size() != 1) {
            throw new SeaException("connection not found: " + id);
        }
        String user = rows.rows().get(0).getString("SEA$USER_NAME");
        String state = rows.rows().get(0).getString("SEA$STATE");
        if (state == null || !state.equalsIgnoreCase("ACTIVE")) {
            throw new SeaException("connection is not active: " + id);
        }
        Session who = connected != null ? connected : actor;
        if (who == null || (!who.admin && !Names.same(who.user, user))) {
            String name = who == null ? "session" : who.user;
            throw new SeaException("user " + name + " cannot drop connection " + id);
        }
        Session target = attachments.get(id);
        if (target != null) {
            detach(target);
            return;
        }
        maintainConnections("DELETE FROM SEA$CONNECTIONS WHERE SEA$ATTACHMENT_ID = ?", id);
    }

    private static String clip(String value, int length) {
        if (value == null) {
            return null;
        }
        String text = value.trim();
        if (text.isEmpty()) {
            return null;
        }
        return text.length() <= length ? text : text.substring(0, length);
    }

    private String requireRole(String name) {
        String role = normalizeRole(name);
        if (role == null || findRole(role) == null) {
            throw new SeaException("role not found: " + name);
        }
        return role;
    }

    private String normalizeRole(String name) {
        if (name == null || name.isBlank() || Names.same(name, "NONE")) {
            return null;
        }
        String normalized = name.trim().toUpperCase(Locale.ROOT);
        Names.checkName(normalized, "role");
        return normalized;
    }

    private boolean userExists(String name) {
        SqlResult rows = systemExecute("SELECT SEA$USER_NAME FROM SEA$USERS WHERE SEA$USER_NAME = ?", name);
        return rows.size() == 1;
    }

    private StoredRow findRole(String name) {
        if (systemTable(SystemTables.ROLES) == null) {
            return null;
        }
        return findRow(SystemTables.ROLES, "SEA$ROLE_NAME", name);
    }

    private boolean member(String role, String user) {
        return memberRow(role, user) != null || memberRow(role, SystemTables.PUBLIC) != null;
    }

    private boolean canGrantRole(String role, String owner) {
        if (bypass || (actor != null && actor.admin)) {
            return true;
        }
        if (actor != null && Names.same(actor.user, owner)) {
            return true;
        }
        if (actor == null) {
            return false;
        }
        return adminOption(role, actor.user) || adminOption(role, SystemTables.PUBLIC);
    }

    private boolean adminOption(String role, String user) {
        StoredRow row = memberRow(role, user);
        if (row == null) {
            return false;
        }
        TableStore table = require(SystemTables.ROLE_MEMBERS);
        return Boolean.TRUE.equals(table.value(row, table.schema().indexOf("SEA$ADMIN_OPTION")));
    }

    private StoredRow memberRow(String role, String user) {
        TableStore table = systemTable(SystemTables.ROLE_MEMBERS);
        if (table == null || role == null || user == null) {
            return null;
        }
        int roleIndex = table.schema().indexOf("SEA$ROLE_NAME");
        int userIndex = table.schema().indexOf("SEA$USER_NAME");
        for (StoredRow row : table.scan()) {
            Object roleValue = table.value(row, roleIndex);
            Object userValue = table.value(row, userIndex);
            if (roleValue instanceof String roleText && Names.same(roleText, role)
                    && userValue instanceof String userText && Names.same(userText, user)) {
                return row;
            }
        }
        return null;
    }

    private String normalizeUser(String name) {
        String normalized = name.trim().toUpperCase(Locale.ROOT);
        Names.checkName(normalized, "user");
        return normalized;
    }

    private boolean canGrant(String table, String privilege) {
        if (bypass || (actor != null && actor.admin) || owns(table)) {
            return true;
        }
        return actor != null && (hasGrantOption(actor.user, table, privilege)
                || (actor.role != null && hasGrantOption(actor.role, table, privilege)));
    }

    private boolean canRevoke(String table, String grantee, String privilege) {
        if (canGrant(table, privilege)) {
            return true;
        }
        String grantor = grantor(grantee, table, privilege);
        return actor != null && grantor != null && Names.same(grantor, actor.user);
    }

    private boolean owns(String table) {
        return actor != null && holds(actor.user, table, SystemTables.OWNER);
    }

    private boolean holds(String user, String table, String privilege) {
        return privilegeRow(user, table, privilege) != null;
    }

    private boolean holds(Session session, String table, String privilege) {
        return session != null && holds(session.user, table, privilege);
    }

    private boolean hasGrantOption(String user, String table, String privilege) {
        StoredRow row = privilegeRow(user, table, privilege);
        if (row == null) {
            return false;
        }
        TableStore store = require(SystemTables.PRIVILEGES);
        return Boolean.TRUE.equals(store.value(row, store.schema().indexOf("SEA$GRANT_OPTION")));
    }

    private String grantor(String user, String table, String privilege) {
        StoredRow row = privilegeRow(user, table, privilege);
        if (row == null) {
            return null;
        }
        TableStore store = require(SystemTables.PRIVILEGES);
        Object value = store.value(row, store.schema().indexOf("SEA$GRANTOR"));
        return value instanceof String text ? text : null;
    }

    private StoredRow privilegeRow(String user, String table, String privilege) {
        TableStore store = systemTable(SystemTables.PRIVILEGES);
        if (store == null || user == null) {
            return null;
        }
        String stored = grantName(table);
        Schema schema = store.schema();
        int userIndex = schema.indexOf("SEA$USER_NAME");
        int tableIndex = schema.indexOf("SEA$TABLE_NAME");
        int privilegeIndex = schema.indexOf("SEA$PRIVILEGE");
        for (StoredRow row : store.scan()) {
            if (sameText(store.value(row, userIndex), user)
                    && sameText(store.value(row, tableIndex), stored)
                    && sameText(store.value(row, privilegeIndex), privilege)) {
                return row;
            }
        }
        return null;
    }

    private static boolean sameText(Object value, String expected) {
        return value instanceof String text && Names.same(text, expected);
    }

    private void guardLastAdmin(TableStore table, Collection<Long> deleting, Map<Long, Cell[]> replacements) {
        int adminIndex = table.schema().indexOf("SEA$ADMIN");
        int activeIndex = table.schema().indexOf("SEA$ACTIVE");
        int remaining = 0;
        for (StoredRow row : table.scan()) {
            if (deleting.contains(row.id)) {
                continue;
            }
            Cell[] cells = replacements == null ? null : replacements.get(row.id);
            if (flag(table, row, cells, adminIndex) && flag(table, row, cells, activeIndex)) {
                remaining++;
            }
        }
        if (remaining == 0) {
            throw new SeaException("the database must keep at least one active DBA");
        }
    }

    private static boolean flag(TableStore table, StoredRow row, Cell[] cells, int index) {
        Object value = cells == null ? table.value(row, index) : cells[index].value;
        return Boolean.TRUE.equals(value);
    }

    private Object prepared(Schema schema, Column column, Object raw) {
        if (raw == Values.MISSING) {
            return raw;
        }
        return coerced(column, sealed(schema, column, raw));
    }

    private Object sealed(Schema schema, Column column, Object value) {
        if (!Names.same(schema.name, SystemTables.USERS) || value == null) {
            return value;
        }
        if (Names.same(column.name, "SEA$USER_NAME") && value instanceof String text) {
            return text.toUpperCase(Locale.ROOT);
        }
        if (Names.same(column.name, "SEA$PASSWORD") && value instanceof String text) {
            if (text.isEmpty()) {
                throw new SeaException("password is required");
            }
            return Password.hash(text);
        }
        return value;
    }

    private Evaluator evaluator(TableStore table, String alias) {
        Evaluator evaluator = new Evaluator(table, params, alias, scope, this::nextGenerator, this::readContext, new Evaluator.Subqueries() {
            @Override
            public Object scalar(Query query, Evaluator outer) {
                return scalarSubquery(query, outer);
            }

            @Override
            public Object in(Expr expr, Query query, boolean negated, Evaluator outer) {
                return inSubquery(expr, query, negated, outer);
            }

            @Override
            public Object exists(Query query, Evaluator outer) {
                return existsSubquery(query, outer);
            }
        });
        evaluator.bindFunctions(this::callUserFunction);
        return evaluator;
    }

    private DataType contextType(ContextRef context) {
        return switch (context.name()) {
            case "CURRENT_DATE" -> DataType.DATE;
            case "CURRENT_TIME", "LOCALTIME" -> DataType.TIME;
            case "CURRENT_TIMESTAMP", "LOCALTIMESTAMP" -> DataType.TIMESTAMP;
            case "CURRENT_CONNECTION", "CURRENT_TRANSACTION" -> DataType.BIGINT;
            default -> DataType.VARCHAR;
        };
    }

    private Object readContext(String name, Integer precision) {
        return switch (name) {
            case "CURRENT_DATE" -> contextClock().toLocalDate();
            case "CURRENT_TIME", "LOCALTIME" -> truncateTime(contextClock().toLocalTime(), precision == null ? 0 : precision);
            case "CURRENT_TIMESTAMP", "LOCALTIMESTAMP" -> truncateTimestamp(contextClock(), precision == null ? 3 : precision);
            case "CURRENT_USER", "USER" -> connectedUser();
            case "CURRENT_ROLE" -> connectedRole();
            case "CURRENT_CONNECTION" -> connectedId();
            case "CURRENT_TRANSACTION" -> currentTransaction();
            default -> throw new SeaException("unknown context: " + name);
        };
    }

    private LocalDateTime contextClock() {
        if (contextNow == null) {
            contextNow = LocalDateTime.now();
        }
        return contextNow;
    }

    private String connectedUser() {
        Session session = connected != null ? connected : actor;
        return session == null ? null : session.user;
    }

    private String connectedRole() {
        Session session = connected != null ? connected : actor;
        if (session == null || session.role == null) {
            return "NONE";
        }
        return session.role;
    }

    private long connectedId() {
        Session session = connected != null ? connected : actor;
        if (session == null) {
            throw new SeaException("there is no current connection");
        }
        return session.attachmentId;
    }

    private long currentTransaction() {
        if (transactionId == null) {
            transactionId = nextTransaction++;
        }
        return transactionId;
    }

    private static LocalTime truncateTime(LocalTime time, int precision) {
        if (precision < 0 || precision > 3) {
            throw new SeaException("time precision must be between 0 and 3");
        }
        int factor = switch (precision) {
            case 0 -> 0;
            case 1 -> 100_000_000;
            case 2 -> 10_000_000;
            default -> 1_000_000;
        };
        if (factor == 0) {
            return time.withNano(0);
        }
        return time.withNano(time.getNano() / factor * factor);
    }

    private static LocalDateTime truncateTimestamp(LocalDateTime time, int precision) {
        return time.withNano(truncateTime(time.toLocalTime(), precision).getNano());
    }

    private int inSql(java.util.function.IntSupplier action) {
        boolean previous = scope != null && scope.preferVariables;
        if (scope != null) {
            scope.preferVariables = false;
        }
        try {
            return action.getAsInt();
        } finally {
            if (scope != null) {
                scope.preferVariables = previous;
            }
        }
    }

    private Object[] logical(TableStore table, StoredRow row) {
        Object[] values = new Object[table.schema().columns.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = table.value(row, i);
        }
        return values;
    }

    private StoredRow rowById(TableStore table, long id) {
        StoredRow row = table.row(id);
        if (row == null) {
            throw new SeaException("record not found");
        }
        return row;
    }

    private void restoreStatement() {
        if (statementSnapshots == null) {
            return;
        }
        for (Map.Entry<TableStore, TableStore.Snapshot> entry : statementSnapshots.entrySet()) {
            entry.getKey().restore(entry.getValue());
        }
    }

    private void undoRenames(int base) {
        for (int i = renameUndo.size() - 1; i >= base; i--) {
            renameUndo.remove(i).run();
        }
    }

    private void callBody(Runnable body) {
        boolean boundary = procedureDepth == 0 && statementDepth == 0;
        if (boundary) {
            statementSnapshots = new IdentityHashMap<>();
        }
        procedureDepth++;
        try {
            body.run();
            if (boundary && !inTransaction) {
                flush();
            }
        } catch (RuntimeException exception) {
            if (boundary) {
                restoreStatement();
            }
            throw exception;
        } finally {
            procedureDepth--;
            if (boundary) {
                statementSnapshots = null;
            }
        }
    }

    private void enterRoutine() {
        if (++routineDepth > 32) {
            throw new SeaException("procedure or trigger exceeded 32 nested calls");
        }
    }

    private void leaveRoutine() {
        routineDepth--;
    }

    private long nextGenerator(String name, Long step) {
        Relation rel = relation(name);
        Names.checkName(rel.name(), "generator");
        TableStore table = require(SystemTables.GENERATORS);
        StoredRow row = findGenerator(name);
        if (row == null) {
            throw new SeaException("generator not found: " + shown(name));
        }
        int currentIndex = table.schema().indexOf("SEA$CURRENT_VALUE");
        int incrementIndex = table.schema().indexOf("SEA$INCREMENT");
        long current = Values.toLong(table.value(row, currentIndex));
        long increment = Values.toLong(table.value(row, incrementIndex));
        long delta = step == null ? increment : step;
        if (delta == 0) {
            return current;
        }
        long updated;
        try {
            updated = Math.addExact(current, delta);
        } catch (ArithmeticException exception) {
            throw new SeaException("generator exceeded the BIGINT limit: " + name);
        }
        Cell[] cells = row.copyCells();
        cells[currentIndex] = TableStore.toCell(table.schema().columns.get(currentIndex), updated);
        table.update(Map.of(row.id, cells));
        flushMatching(candidate -> Names.same(candidate.schema().name, SystemTables.GENERATORS));
        return updated;
    }

    private void createGenerator(CreateGenerator statement) {
        Relation rel = relation(statement.name());
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("reserved name: " + rel.name());
        }
        Names.checkName(rel.name(), "generator");
        if (findGenerator(statement.name()) != null) {
            throw new SeaException("generator already exists: " + shown(statement.name()));
        }
        long increment = statement.increment() == null ? 1L : statement.increment();
        if (increment == 0) {
            throw new SeaException("INCREMENT BY cannot be zero");
        }
        long initial = statement.start() == null ? 0L : statement.start();
        long current = statement.start() == null ? 0L : Math.subtractExact(initial, increment);
        long id = nextCatalogId(SystemTables.GENERATORS, "SEA$GENERATOR_ID");
        systemExecute("""
                INSERT INTO SEA$GENERATORS (
                    SEA$SCHEMA_NAME, SEA$GENERATOR_NAME, SEA$GENERATOR_ID, SEA$INITIAL_VALUE, SEA$INCREMENT, SEA$CURRENT_VALUE, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, rel.schema(), rel.name(), id, initial, increment, current, ownerName());
    }

    private void dropGenerator(DropGenerator statement) {
        StoredRow row = findGenerator(statement.name());
        if (row == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("generator not found: " + shown(statement.name()));
        }
        Relation rel = relation(statement.name());
        guardRoutine("the generator", shown(statement.name()), text(require(SystemTables.GENERATORS), row, "SEA$OWNER_NAME"), null);
        deleteGenerator(rel.schema(), rel.name());
    }

    private void setGenerator(SetGenerator statement) {
        TableStore table = require(SystemTables.GENERATORS);
        StoredRow row = findGenerator(statement.name());
        if (row == null) {
            throw new SeaException("generator not found: " + shown(statement.name()));
        }
        Object raw = evaluator(null, null).eval(statement.value());
        if (raw == null) {
            throw new SeaException("generator value cannot be null");
        }
        long value = Values.toLong(raw);
        long increment = Values.toLong(table.value(row, table.schema().indexOf("SEA$INCREMENT")));
        long current = value;
        if (statement.restart()) {
            try {
                current = Math.subtractExact(value, increment);
            } catch (ArithmeticException exception) {
                throw new SeaException("generator value exceeded the BIGINT limit");
            }
        }
        int currentIndex = table.schema().indexOf("SEA$CURRENT_VALUE");
        Cell[] cells = row.copyCells();
        cells[currentIndex] = TableStore.toCell(table.schema().columns.get(currentIndex), current);
        table.update(Map.of(row.id, cells));
        flushMatching(candidate -> Names.same(candidate.schema().name, SystemTables.GENERATORS));
    }

    private void createProcedure(CreateProcedure statement) {
        Relation rel = relation(statement.name());
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("reserved name: " + rel.name());
        }
        Names.checkName(rel.name(), "procedure");
        if (statement.source().length() > Format.MAX_CHAR) {
            throw new SeaException("procedure source exceeds " + Format.MAX_CHAR + " characters");
        }
        StoredProcedure existing = findProcedure(statement.name());
        if (existing != null) {
            if (!statement.orReplace()) {
                throw new SeaException("procedure already exists: " + shown(statement.name()));
            }
            replaceProcedure(existing, statement);
            return;
        }
        Set<String> names = new HashSet<>();
        rejectDuplicate(names, statement.inputs());
        rejectDuplicate(names, statement.outputs());
        Routine routine = SqlParser.parseRoutine(statement.source());
        rejectDuplicate(names, routine.locals());
        for (CursorDef cursor : routine.cursors()) {
            if (!names.add(Names.fold(cursor.name()))) {
                throw new SeaException("duplicate name: " + cursor.name());
            }
        }
        String owner = ownerName();
        try {
            systemExecute("""
                    INSERT INTO SEA$PROCEDURES (
                        SEA$SCHEMA_NAME, SEA$PROCEDURE_NAME, SEA$PROCEDURE_SOURCE, SEA$OWNER_NAME
                    ) VALUES (?, ?, ?, ?)
                    """, rel.schema(), rel.name(), statement.source(), owner);
            insertParameters(rel.schema(), rel.name(), statement.inputs(), 0);
            insertParameters(rel.schema(), rel.name(), statement.outputs(), 1);
        } catch (RuntimeException exception) {
            deleteProcedure(rel.schema(), rel.name());
            throw exception;
        }
    }

    private void insertParameters(String schema, String procedure, List<ParamDef> parameters, int direction) {
        for (int i = 0; i < parameters.size(); i++) {
            ParamDef parameter = parameters.get(i);
            systemExecute("""
                    INSERT INTO SEA$PROCEDURE_PARAMETERS (
                        SEA$SCHEMA_NAME, SEA$PROCEDURE_NAME, SEA$PARAMETER_NAME, SEA$PARAMETER_NUMBER, SEA$PARAMETER_TYPE,
                        SEA$FIELD_TYPE, SEA$FIELD_LENGTH, SEA$FIELD_SCALE
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, schema, procedure, parameter.name(), i, direction, parameter.type().name(), parameter.length(), parameter.scale());
        }
    }

    private void dropProcedure(DropProcedure statement) {
        StoredProcedure procedure = findProcedure(statement.name());
        if (procedure == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("procedure not found: " + shown(statement.name()));
        }
        guardRoutine("the procedure", procedure.label(), procedure.owner(), null);
        deleteProcedure(procedure.schema(), procedure.name());
    }

    private SqlResult invokeProcedure(ExecuteProcedure statement, boolean selectable) {
        ProcedureRows called = callProcedure(statement, selectable);
        if (!statement.returning().isEmpty()) {
            if (called.rows.isEmpty()) {
                throw new SeaException("procedure did not return values: " + statement.name());
            }
            Object[] values = called.rows.get(0);
            for (int i = 0; i < statement.returning().size(); i++) {
                scope.setVariable(statement.returning().get(i), values[i]);
            }
            return SqlResult.update(0);
        }
        if (called.outputs.isEmpty()) {
            return SqlResult.update(0);
        }
        List<ColumnMeta> columns = outputColumns(called.outputs);
        List<Row> rows = new ArrayList<>();
        for (Object[] values : called.rows) {
            rows.add(new Row(columns, values));
        }
        return SqlResult.query(columns, rows);
    }

    private ProcedureRows callProcedure(ExecuteProcedure statement, boolean selectable) {
        StoredProcedure procedure = findProcedure(statement.name());
        if (procedure == null) {
            throw new SeaException("procedure not found: " + shown(statement.name()));
        }
        guardExecute("PROCEDURE", procedure.schema(), procedure.name(), procedure.owner());
        String label = procedure.label();
        List<ParamDef> inputs = parameters(procedure.schema(), procedure.name(), 0);
        List<ParamDef> outputs = parameters(procedure.schema(), procedure.name(), 1);
        if (selectable && outputs.isEmpty()) {
            throw new SeaException("procedure without RETURNS cannot be used in FROM: " + label);
        }
        if (statement.arguments().size() != inputs.size()) {
            throw new SeaException("procedure " + label + " expects " + inputs.size() + " arguments");
        }
        if (!statement.returning().isEmpty() && statement.returning().size() != outputs.size()) {
            throw new SeaException("RETURNING_VALUES does not match the returns of " + label);
        }
        Object[] args = new Object[inputs.size()];
        for (int i = 0; i < inputs.size(); i++) {
            Object raw = evalRoutine(statement.arguments().get(i));
            ParamDef input = inputs.get(i);
            args[i] = raw == null ? null : Values.coerce(input.type(), input.length(), input.scale(), raw);
        }
        Routine routine = SqlParser.parseRoutine(procedure.source());
        String previousPin = pinnedSchema;
        pinnedSchema = procedure.schema();
        try {
            List<Object[]> rows = runRoutine(routine, inputs, outputs, args, selectable);
            return new ProcedureRows(procedure.name(), outputs, rows);
        } finally {
            pinnedSchema = previousPin;
        }
    }

    private List<Object[]> runRoutine(Routine routine, List<ParamDef> inputs, List<ParamDef> outputs, Object[] args, boolean selectable) {
        RoutineScope local = new RoutineScope();
        local.preferVariables = true;
        for (int i = 0; i < inputs.size(); i++) {
            local.declare(inputs.get(i), args[i]);
        }
        for (ParamDef output : outputs) {
            local.declare(output, null);
        }
        for (ParamDef variable : routine.locals()) {
            local.declare(variable, null);
        }
        RoutineScope previousScope = scope;
        Map<String, OpenCursor> previousCursors = cursors;
        RoutineFrame previousFrame = frame;
        boolean previousHalt = psqlHalt;
        scope = local;
        cursors = installCursors(routine);
        frame = new RoutineFrame(outputs, selectable);
        psqlHalt = false;
        Map<String, Held> savedScope = cteScope;
        Set<String> savedDefining = cteDefining;
        cteScope = null;
        cteDefining = new HashSet<>();
        enterRoutine();
        try {
            callBody(() -> runPsql(routine.body()));
            lastReturn = frame.returned;
            if (!selectable && !frame.suspended && !outputs.isEmpty()) {
                frame.rows.add(outputValues());
            }
            return new ArrayList<>(frame.rows);
        } finally {
            leaveRoutine();
            scope = previousScope;
            cursors = previousCursors;
            frame = previousFrame;
            psqlHalt = previousHalt;
            cteScope = savedScope;
            cteDefining = savedDefining;
        }
    }

    private SqlResult queryProcedure(SelectProcedure statement) {
        ProcedureRows called = callProcedure(
                new ExecuteProcedure(statement.procedure(), statement.arguments(), List.of()), true);
        Schema schema = outputSchema(called.name, called.outputs);
        List<SelectItem> items = statement.star() ? starOutputs(called.outputs)
                : expandStars(statement.items(), schema, schema.name, null, statement.procedure(), statement.alias());
        if (items.isEmpty()) {
            throw new SeaException("SELECT needs at least one expression");
        }
        List<ColumnMeta> columns = new ArrayList<>();
        for (SelectItem item : items) {
            columns.add(new ColumnMeta(item.label(), infer(item.expr(), schema)));
        }
        boolean aggregate = false;
        for (SelectItem item : items) {
            if (Evaluator.containsAggregate(item.expr())) {
                aggregate = true;
            }
        }
        if (!statement.groupBy().isEmpty() || statement.having() != null || aggregate) {
            if (statement.star() && !statement.groupBy().isEmpty()) {
                throw new SeaException("SELECT * cannot be combined with GROUP BY");
            }
            Evaluator evaluator = evaluator(null, statement.alias());
            evaluator.useView(schema, called.name);
            List<Object[]> matched = new ArrayList<>();
            for (Object[] produced : called.rows) {
                evaluator.bindView(produced);
                if (statement.where() == null || evaluator.truth(statement.where())) {
                    matched.add(produced);
                }
            }
            String procedureName = called.name;
            return grouped(statement.star(), statement.distinct(), statement.groupBy(), statement.having(),
                    statement.orderBy(), statement.limit(), statement.offset(), items, columns, evaluator, new GroupSource() {
                @Override
                public int size() {
                    return matched.size();
                }

                @Override
                public void bind(int index) {
                    evaluator.bindView(matched.get(index));
                }

                @Override
                public Projected projected(int index, Object[] values) {
                    return new Projected(null, values, matched.get(index));
                }

                @Override
                public String identify(ColumnRef column) {
                    return identifyView(column, schema, statement.alias(), procedureName);
                }

                @Override
                public boolean hasColumn(String name) {
                    return schema.indexOf(name) >= 0;
                }
            });
        }
        List<Projected> projected = new ArrayList<>();
        for (Object[] produced : called.rows) {
            RoutineScope local = scopeFor(called.outputs, produced);
            if (statement.where() != null && !truthIn(statement.where(), local, statement.alias())) {
                continue;
            }
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = materialize(evalIn(items.get(i).expr(), local, statement.alias()));
            }
            projected.add(new Projected(null, values, null));
        }
        if (statement.distinct()) {
            projected = distinct(projected);
        }
        if (!statement.orderBy().isEmpty()) {
            projected.sort((left, right) -> {
                for (OrderItem order : statement.orderBy()) {
                    Integer index = selectIndex(order.expr(), items);
                    if (index == null) {
                        throw new SeaException("procedure ORDER BY must use columns from the SELECT list");
                    }
                    int cmp = compareValues(left.values[index], right.values[index]);
                    if (order.descending()) {
                        cmp = -cmp;
                    }
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return 0;
            });
        }
        projected = page(projected, statement.limit(), statement.offset());
        List<Row> rows = new ArrayList<>();
        for (Projected row : projected) {
            rows.add(new Row(columns, row.values));
        }
        return SqlResult.query(columns, rows);
    }

    private void createTrigger(CreateTrigger statement) {
        if (SystemTables.isSystem(statement.name())) {
            throw new SeaException("reserved name: " + statement.name());
        }
        if (statement.source().length() > Format.MAX_CHAR) {
            throw new SeaException("trigger source exceeds " + Format.MAX_CHAR + " characters");
        }
        boolean instead = statement.phase().startsWith("INSTEAD");
        TableStore relation = findTable(statement.table());
        StoredView view = findView(statement.table());
        if (instead) {
            if (view == null) {
                throw new SeaException("INSTEAD OF trigger requires a view: " + shown(statement.table()));
            }
            if (relation != null) {
                throw new SeaException("INSTEAD OF trigger requires a view: " + shown(statement.table()));
            }
        } else if (relation == null) {
            if (view != null) {
                throw new SeaException("BEFORE or AFTER trigger requires a table: " + shown(statement.table()));
            }
            throw new SeaException("table not found: " + shown(statement.table()));
        }
        String relationName;
        String guardName;
        if (relation != null) {
            guardDdl(relation.schema().name);
            if (SystemTables.isSystem(relation.schema().name)) {
                throw new SeaException("system table cannot have a trigger: " + relation.schema().name);
            }
            relationName = Relation.catalogName(relation.namespace(), relation.schema().name);
            guardName = relation.schema().name;
        } else {
            relationName = view.storedKey();
            guardName = view.name();
        }
        if (guardName != null && SystemTables.isSystem(guardName)) {
            throw new SeaException("system table cannot have a trigger: " + guardName);
        }
        if (findRow(SystemTables.TRIGGERS, "SEA$TRIGGER_NAME", statement.name()) != null) {
            throw new SeaException("trigger already exists: " + statement.name());
        }
        SqlParser.parseRoutine(statement.source());
        String type = statement.phase() + " " + String.join(" OR ", statement.events());
        if (relationName.length() > 128) {
            throw new SeaException("qualified name exceeds 128 characters");
        }
        systemExecute("""
                INSERT INTO SEA$TRIGGERS (
                    SEA$TRIGGER_NAME, SEA$RELATION_NAME, SEA$TRIGGER_SEQUENCE, SEA$TRIGGER_TYPE,
                    SEA$TRIGGER_SOURCE, SEA$TRIGGER_INACTIVE, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, statement.name(), relationName, statement.position(), type, statement.source(),
                statement.inactive(), ownerName());
    }

    private void dropTrigger(DropTrigger statement) {
        StoredRow row = findRow(SystemTables.TRIGGERS, "SEA$TRIGGER_NAME", statement.name());
        if (row == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("trigger not found: " + statement.name());
        }
        TableStore table = require(SystemTables.TRIGGERS);
        guardRoutine("the trigger", statement.name(), text(table, row, "SEA$OWNER_NAME"), text(table, row, "SEA$RELATION_NAME"));
        deleteCatalog(SystemTables.TRIGGERS, "SEA$TRIGGER_NAME", statement.name());
    }

    private void alterTrigger(AlterTrigger statement) {
        TableStore table = require(SystemTables.TRIGGERS);
        StoredRow row = findRow(SystemTables.TRIGGERS, "SEA$TRIGGER_NAME", statement.name());
        if (row == null) {
            throw new SeaException("trigger not found: " + statement.name());
        }
        guardRoutine("the trigger", statement.name(), text(table, row, "SEA$OWNER_NAME"), text(table, row, "SEA$RELATION_NAME"));
        int index = table.schema().indexOf("SEA$TRIGGER_INACTIVE");
        Cell[] cells = row.copyCells();
        cells[index] = TableStore.toCell(table.schema().columns.get(index), statement.inactive());
        boolean previous = bypass;
        bypass = true;
        try {
            mutating(SystemTables.TRIGGERS, () -> {
                table.update(Map.of(row.id, cells));
                return 1;
            });
        } finally {
            bypass = previous;
        }
    }

    private void forgetTriggers(String table) {
        if (systemTable(SystemTables.TRIGGERS) == null) {
            return;
        }
        deleteCatalog(SystemTables.TRIGGERS, "SEA$RELATION_NAME", table);
    }

    private void fireTriggers(TableStore table, String phase, String event, Object[] oldValues, Object[] newValues) {
        String previousPin = pinnedSchema;
        pinnedSchema = table.namespace();
        try {
            for (TriggerDef trigger : triggersFor(Relation.catalogName(table.namespace(), table.schema().name), phase, event)) {
                Routine routine = SqlParser.parseRoutine(trigger.source);
                RoutineScope local = new RoutineScope();
                local.preferVariables = true;
                local.bindRow(table.schema(), newValues, oldValues);
                for (ParamDef variable : routine.locals()) {
                    local.declare(variable, null);
                }
                RoutineScope previousScope = scope;
                Map<String, OpenCursor> previousCursors = cursors;
                RoutineFrame previousFrame = frame;
                boolean previousHalt = psqlHalt;
                scope = local;
                cursors = installCursors(routine);
                frame = null;
                psqlHalt = false;
                enterRoutine();
                try {
                    runPsql(routine.body());
                } finally {
                    leaveRoutine();
                    scope = previousScope;
                    cursors = previousCursors;
                    frame = previousFrame;
                    psqlHalt = previousHalt;
                }
            }
        } finally {
            pinnedSchema = previousPin;
        }
    }

    private List<TriggerDef> triggersFor(String table, String phase, String event) {
        TableStore store = require(SystemTables.TRIGGERS);
        List<TriggerDef> list = new ArrayList<>();
        for (StoredRow row : store.scan()) {
            if (!Names.same(text(store, row, "SEA$RELATION_NAME"), table)) {
                continue;
            }
            if (Boolean.TRUE.equals(store.value(row, store.schema().indexOf("SEA$TRIGGER_INACTIVE")))) {
                continue;
            }
            String type = text(store, row, "SEA$TRIGGER_TYPE");
            if (type == null || !type.startsWith(phase + " ")) {
                continue;
            }
            boolean matches = false;
            for (String part : type.substring(phase.length() + 1).split(" OR ")) {
                if (part.equals(event)) {
                    matches = true;
                }
            }
            if (!matches) {
                continue;
            }
            int position = (int) Values.toLong(store.value(row, store.schema().indexOf("SEA$TRIGGER_SEQUENCE")));
            list.add(new TriggerDef(text(store, row, "SEA$TRIGGER_NAME"), position, text(store, row, "SEA$TRIGGER_SOURCE")));
        }
        list.sort(Comparator.comparingInt(TriggerDef::position).thenComparing(TriggerDef::name, String.CASE_INSENSITIVE_ORDER));
        return list;
    }

    private void runPsql(Psql statement) {
        if (psqlHalt || statement == null) {
            return;
        }
        switch (statement) {
            case PsqlBlock block -> runBlock(block);
            case PsqlReturn returned -> {
                if (frame == null) {
                    throw new SeaException("RETURN can only be used in a function or a block");
                }
                frame.returned = evalRoutine(returned.value());
                psqlHalt = true;
            }
            case PsqlExecuteStatement dynamic -> executeDynamic(dynamic);
            case PsqlAssign assign -> assignPsql(assign);
            case PsqlIf condition -> {
                if (truthRoutine(condition.condition())) {
                    runPsql(condition.thenBranch());
                } else if (condition.elseBranch() != null) {
                    runPsql(condition.elseBranch());
                }
            }
            case PsqlWhile loop -> {
                int turns = 0;
                while (truthRoutine(loop.condition())) {
                    if (++turns > 100_000) {
                        throw new SeaException("WHILE exceeded 100000 iterations");
                    }
                    runPsql(loop.body());
                    if (psqlHalt) {
                        return;
                    }
                }
            }
            case PsqlExit ignored -> psqlHalt = true;
            case PsqlSuspend ignored -> {
                if (frame == null) {
                    throw new SeaException("SUSPEND can only be used in a procedure");
                }
                frame.rows.add(outputValues());
                frame.suspended = true;
                if (frame.stopAtFirstSuspend) {
                    psqlHalt = true;
                }
            }
            case PsqlException error -> {
                Object message = evalRoutine(error.message());
                throw new Raised(message == null ? "exception" : String.valueOf(message));
            }
            case PsqlInsert insert -> scope.rowCount = insert(insert.statement());
            case PsqlUpdate update -> scope.rowCount = update(update.statement());
            case PsqlDelete delete -> scope.rowCount = delete(delete.statement());
            case PsqlExecute execute -> callProcedure(execute.call(), false);
            case PsqlSelectInto into -> selectInto(into);
            case PsqlOpen open -> openCursor(open.cursor());
            case PsqlFetch fetch -> fetchCursor(fetch.cursor(), fetch.targets());
            case PsqlClose close -> closeCursor(close.cursor());
            case PsqlForSelect loop -> forSelect(loop);
        }
    }

    private void assignPsql(PsqlAssign assign) {
        Object value = evalRoutine(assign.value());
        if (assign.qualifier() == null) {
            scope.setVariable(assign.name(), value);
            return;
        }
        if (Names.same(assign.qualifier(), "NEW")) {
            scope.setNew(assign.name(), value);
            return;
        }
        if (Names.same(assign.qualifier(), "OLD")) {
            throw new SeaException("OLD is read-only");
        }
        throw new SeaException("unknown qualifier: " + assign.qualifier());
    }

    private void selectInto(PsqlSelectInto statement) {
        if (statement.targets().size() != statement.expressions().size()) {
            throw new SeaException("SELECT INTO needs the same number of variables and expressions");
        }
        List<SelectItem> items = new ArrayList<>();
        for (int i = 0; i < statement.expressions().size(); i++) {
            items.add(new SelectItem(statement.expressions().get(i), null, "c" + i));
        }
        boolean previous = scope.preferVariables;
        scope.preferVariables = false;
        SqlResult result;
        try {
            result = query(new Select(false, false, items, statement.table(), null, List.of(), statement.where(),
                    List.of(), null, List.of(), null, null));
        } finally {
            scope.preferVariables = previous;
        }
        if (result.size() > 1) {
            throw new SeaException("SELECT INTO returned more than one row");
        }
        if (result.size() == 0) {
            scope.rowCount = 0;
            for (String target : statement.targets()) {
                scope.setVariable(target, null);
            }
            return;
        }
        scope.rowCount = 1;
        Row row = result.rows().get(0);
        for (int i = 0; i < statement.targets().size(); i++) {
            scope.setVariable(statement.targets().get(i), row.get(i));
        }
    }

    private Map<String, OpenCursor> installCursors(Routine routine) {
        Map<String, OpenCursor> map = new LinkedHashMap<>();
        for (CursorDef cursor : routine.cursors()) {
            if (scope.hasVariable(cursor.name())) {
                throw new SeaException("duplicate name: " + cursor.name());
            }
            String key = Names.fold(cursor.name());
            if (map.containsKey(key)) {
                throw new SeaException("duplicate cursor: " + cursor.name());
            }
            map.put(key, new OpenCursor(cursor));
        }
        return map;
    }

    private void openCursor(String name) {
        OpenCursor cursor = requireCursor(name);
        if (cursor.open) {
            throw new SeaException("cursor is already open: " + name);
        }
        SqlResult result = runCursorQuery(cursor.definition.query());
        cursor.rows = result.rows();
        cursor.position = 0;
        cursor.open = true;
    }

    private void fetchCursor(String name, List<String> targets) {
        OpenCursor cursor = requireCursor(name);
        if (!cursor.open) {
            throw new SeaException("cursor is not open: " + name);
        }
        if (cursor.position >= cursor.rows.size()) {
            scope.rowCount = 0;
            return;
        }
        assignFetched(targets, cursor.rows.get(cursor.position));
        cursor.position++;
        scope.rowCount = 1;
    }

    private void closeCursor(String name) {
        OpenCursor cursor = requireCursor(name);
        if (!cursor.open) {
            throw new SeaException("cursor is not open: " + name);
        }
        cursor.open = false;
        cursor.rows = List.of();
        cursor.position = 0;
    }

    private void forSelect(PsqlForSelect statement) {
        SqlResult result = runCursorQuery(statement.query());
        for (Row row : result.rows()) {
            if (psqlHalt) {
                return;
            }
            assignFetched(statement.targets(), row);
            scope.rowCount = 1;
            runPsql(statement.body());
            if (psqlHalt) {
                return;
            }
        }
        scope.rowCount = 0;
    }

    private SqlResult runCursorQuery(Query query) {
        boolean previous = scope.preferVariables;
        scope.preferVariables = false;
        try {
            return runQuery(query);
        } finally {
            scope.preferVariables = previous;
        }
    }

    private void assignFetched(List<String> targets, Row row) {
        if (targets.size() != row.size()) {
            throw new SeaException("INTO needs the same number of variables and columns");
        }
        for (int i = 0; i < targets.size(); i++) {
            scope.setVariable(targets.get(i), row.get(i));
        }
    }

    private OpenCursor requireCursor(String name) {
        OpenCursor cursor = cursors == null ? null : cursors.get(Names.fold(name));
        if (cursor == null) {
            throw new SeaException("cursor is not declared: " + name);
        }
        return cursor;
    }

    private Object evalRoutine(Expr expr) {
        boolean previous = scope != null && scope.preferVariables;
        if (scope != null) {
            scope.preferVariables = true;
        }
        try {
            return evaluator(null, null).eval(expr);
        } finally {
            if (scope != null) {
                scope.preferVariables = previous;
            }
        }
    }

    private boolean truthRoutine(Expr expr) {
        boolean previous = scope != null && scope.preferVariables;
        if (scope != null) {
            scope.preferVariables = true;
        }
        try {
            return evaluator(null, null).truth(expr);
        } finally {
            if (scope != null) {
                scope.preferVariables = previous;
            }
        }
    }

    private Object evalIn(Expr expr, RoutineScope local, String alias) {
        RoutineScope previous = scope;
        scope = local;
        try {
            return evaluator(null, alias).eval(expr);
        } finally {
            scope = previous;
        }
    }

    private boolean truthIn(Expr expr, RoutineScope local, String alias) {
        RoutineScope previous = scope;
        scope = local;
        try {
            return evaluator(null, alias).truth(expr);
        } finally {
            scope = previous;
        }
    }

    private Object[] outputValues() {
        Object[] values = new Object[frame.outputs.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = materialize(scope.variable(frame.outputs.get(i).name()));
        }
        return values;
    }

    private List<ParamDef> parameters(String schema, String procedure, int direction) {
        TableStore table = require(SystemTables.PROCEDURE_PARAMETERS);
        int numberIndex = table.schema().indexOf("SEA$PARAMETER_NUMBER");
        int typeIndex = table.schema().indexOf("SEA$PARAMETER_TYPE");
        List<StoredRow> rows = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            if (!Names.same(text(table, row, "SEA$SCHEMA_NAME"), schema)
                    || !Names.same(text(table, row, "SEA$PROCEDURE_NAME"), procedure)) {
                continue;
            }
            if (Values.toLong(table.value(row, typeIndex)) != direction) {
                continue;
            }
            rows.add(row);
        }
        rows.sort(Comparator.comparingLong(row -> Values.toLong(table.value(row, numberIndex))));
        List<ParamDef> result = new ArrayList<>();
        for (StoredRow row : rows) {
            result.add(new ParamDef(
                    text(table, row, "SEA$PARAMETER_NAME"),
                    DataType.valueOf(text(table, row, "SEA$FIELD_TYPE")),
                    (int) Values.toLong(table.value(row, table.schema().indexOf("SEA$FIELD_LENGTH"))),
                    (int) Values.toLong(table.value(row, table.schema().indexOf("SEA$FIELD_SCALE")))
            ));
        }
        return result;
    }

    private List<ColumnMeta> outputColumns(List<ParamDef> outputs) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (ParamDef output : outputs) {
            columns.add(new ColumnMeta(output.name(), output.type()));
        }
        return columns;
    }

    private List<SelectItem> starOutputs(List<ParamDef> outputs) {
        List<SelectItem> items = new ArrayList<>();
        for (ParamDef output : outputs) {
            items.add(new SelectItem(new ColumnRef(null, output.name()), null, output.name()));
        }
        return items;
    }

    private Schema outputSchema(String name, List<ParamDef> outputs) {
        List<Column> columns = new ArrayList<>();
        for (ParamDef output : outputs) {
            columns.add(new Column(output.name(), output.type(), output.length(), output.scale(), false, false, 0L, false, null));
        }
        return new Schema(name, columns, new int[0], List.of(), List.of());
    }

    private RoutineScope scopeFor(List<ParamDef> outputs, Object[] values) {
        RoutineScope local = new RoutineScope();
        local.preferVariables = true;
        for (int i = 0; i < outputs.size(); i++) {
            local.declare(outputs.get(i), values[i]);
        }
        return local;
    }

    private static boolean sameValue(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
            return Arrays.equals(leftBytes, rightBytes);
        }
        if (left == null || right == null || left.getClass() != right.getClass()) {
            return false;
        }
        return Values.compare(left, right) == 0;
    }

    private static int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return Values.compare(left, right);
    }

    private void rejectDuplicate(Set<String> names, List<ParamDef> parameters) {
        for (ParamDef parameter : parameters) {
            if (!names.add(Names.fold(parameter.name()))) {
                throw new SeaException("duplicate variable: " + parameter.name());
            }
        }
    }

    private String ownerName() {
        if (actor == null) {
            throw new SeaException("must be connected");
        }
        return actor.user;
    }

    private void guardRoutine(String kind, String name, String owner, String relation) {
        if (bypass || (actor != null && actor.admin)) {
            return;
        }
        if (actor != null && Names.same(actor.user, owner)) {
            return;
        }
        if (relation != null && owns(relation)) {
            return;
        }
        throw new SeaException("only the owner or the DBA can alter " + kind + " " + name);
    }

    private StoredRow findRow(String tableName, String column, String name) {
        TableStore table = require(tableName);
        int index = table.schema().indexOf(column);
        StoredRow found = null;
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof String text && Names.same(text, name)) {
                if (found != null) {
                    throw new SeaException("ambiguous name: " + name);
                }
                found = row;
            }
        }
        return found;
    }

    private String text(TableStore table, StoredRow row, String column) {
        Object value = table.value(row, table.schema().indexOf(column));
        return value == null ? null : value.toString();
    }

    private long nextCatalogId(String tableName, String column) {
        TableStore table = require(tableName);
        int index = table.schema().indexOf(column);
        long max = 0;
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof Number number) {
                max = Math.max(max, number.longValue());
            }
        }
        return max + 1;
    }

    private void deleteCatalog(String tableName, String column, String name) {
        TableStore table = require(tableName);
        int index = table.schema().indexOf(column);
        List<Long> ids = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, index);
            if (value instanceof String text && Names.same(text, name)) {
                ids.add(row.id);
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        boolean previous = bypass;
        bypass = true;
        try {
            mutating(tableName, () -> {
                table.delete(ids);
                return ids.size();
            });
        } finally {
            bypass = previous;
        }
    }

    private static final String VIEW_COLUMN_SEPARATOR = "\u001f";
    /** Splits output names from the base columns captured before ALTER COLUMN POSITION. */
    private static final String VIEW_BINDING_SEPARATOR = "\u001e";

    private void applyWindows(List<Projected> rows, List<SelectItem> items, List<OrderItem> order, Evaluator evaluator) {
        orderWindows.clear();
        List<WindowExpr> selected = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).expr() instanceof WindowExpr window) {
                selected.add(window);
                indexes.add(i);
            }
        }
        List<WindowExpr> extra = new ArrayList<>();
        for (OrderItem item : order) {
            if (item.expr() instanceof WindowExpr window && selectIndex(window, items) == null) {
                extra.add(window);
            }
        }
        if (selected.isEmpty() && extra.isEmpty()) {
            return;
        }
        for (int i = 0; i < selected.size(); i++) {
            Object[] values = windowValues(selected.get(i), rows, evaluator);
            int slot = indexes.get(i);
            for (int row = 0; row < rows.size(); row++) {
                rows.get(row).values[slot] = values[row];
            }
        }
        for (WindowExpr window : extra) {
            Object[] values = windowValues(window, rows, evaluator);
            for (int row = 0; row < rows.size(); row++) {
                orderWindows.computeIfAbsent(rows.get(row), key -> new IdentityHashMap<>()).put(window, values[row]);
            }
        }
    }

    private Object[] windowValues(WindowExpr window, List<Projected> rows, Evaluator evaluator) {
        List<Object[]> partitions = new ArrayList<>();
        List<Object[]> ordering = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        List<Object> constants = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            bindProjected(evaluator, rows.get(i));
            Object[] part = new Object[window.partition().size()];
            for (int p = 0; p < part.length; p++) {
                part[p] = evaluator.eval(window.partition().get(p));
            }
            Object[] keys = new Object[window.orderBy().size()];
            for (int p = 0; p < keys.length; p++) {
                keys[p] = evaluator.eval(window.orderBy().get(p).expr());
            }
            partitions.add(part);
            ordering.add(keys);
            Call call = window.call();
            args.add(call.star() || call.args().isEmpty() ? null : evaluator.eval(call.args().get(0)));
            if (i == 0) {
                for (Expr arg : call.args()) {
                    constants.add(evaluator.eval(arg));
                }
            }
        }
        return Windows.values(new WindowExpr(window.call(), window.partition(), window.orderBy(), window.frame(), constants),
                partitions, ordering, args);
    }

    private void bindProjected(Evaluator evaluator, Projected projected) {
        if (evaluator.joined() && projected.source != null) {
            evaluator.bindJoined(projected.source);
            return;
        }
        if (projected.row != null) {
            evaluator.bind(projected.row);
            return;
        }
        if (projected.source != null) {
            evaluator.bindView(projected.source);
        }
    }

    private DataType windowType(WindowExpr window, Schema schema) {
        String name = window.call().name().toUpperCase(Locale.ROOT);
        return switch (name) {
            case "ROW_NUMBER", "RANK", "DENSE_RANK", "COUNT" -> DataType.BIGINT;
            case "AVG" -> DataType.DOUBLE;
            case "LAG", "LEAD", "SUM", "MIN", "MAX" -> window.call().args().isEmpty()
                    ? DataType.BIGINT : infer(window.call().args().get(0), schema);
            default -> throw new SeaException("unknown window function: " + window.call().name());
        };
    }

    private void createView(CreateView statement) {
        Relation rel = relation(statement.name());
        if (SystemTables.isSystem(rel.name())) {
            throw new SeaException("reserved name: " + rel.name());
        }
        Names.checkName(rel.name(), "view");
        String stored = Relation.catalogName(rel.schema(), rel.name());
        if (stored.length() > 63) {
            throw new SeaException("qualified view name exceeds 63 characters");
        }
        if (statement.source().length() > Format.MAX_CHAR) {
            throw new SeaException("view source exceeds " + Format.MAX_CHAR + " characters");
        }
        if (findTable(statement.name()) != null) {
            throw new SeaException("table already exists: " + shown(statement.name()));
        }
        StoredView existingView = findView(statement.name());
        if (existingView != null) {
            if (statement.orReplace()) {
                replaceView(existingView, statement);
                return;
            }
            if (statement.ifNotExists()) {
                return;
            }
            throw new SeaException("view already exists: " + shown(statement.name()));
        }
        if (complexView(statement.source())) {
            createComplexView(statement, stored);
            return;
        }
        Select select = SqlParser.parseView(statement.source());
        if (relation(select.table()).key().equals(rel.key())) {
            throw new SeaException("view cannot depend on itself: " + shown(statement.name()));
        }
        Schema base = relationSchema(select.table());
        guardAccess(select.table(), "SELECT");
        List<SelectItem> items = viewItems(select, base);
        List<String> names = new ArrayList<>();
        if (statement.columns().isEmpty()) {
            for (SelectItem item : items) {
                names.add(item.label());
            }
        } else {
            if (statement.columns().size() != items.size()) {
                throw new SeaException("view column count does not match the SELECT");
            }
            names.addAll(statement.columns());
        }
        Set<String> used = new HashSet<>();
        for (String column : names) {
            Names.checkName(column, "column");
            if (column.indexOf(VIEW_COLUMN_SEPARATOR.charAt(0)) >= 0) {
                throw new SeaException("invalid column name: " + column);
            }
            if (!used.add(Names.fold(column))) {
                throw new SeaException("duplicate column in the view: " + column);
            }
        }
        String joined = String.join(VIEW_COLUMN_SEPARATOR, names);
        if (joined.length() > Format.MAX_CHAR) {
            throw new SeaException("view column list exceeds " + Format.MAX_CHAR + " characters");
        }
        systemExecute("""
                INSERT INTO SEA$VIEWS (
                    SEA$VIEW_NAME, SEA$VIEW_SOURCE, SEA$RELATION_NAME, SEA$COLUMNS, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?)
                """, stored, statement.source(), storedRelationName(select.table()), joined, ownerName());
        recordOwner(stored);
    }

    private boolean complexView(String source) {
        try {
            SqlParser.parseView(source);
            return false;
        } catch (SeaException exception) {
            if ("view reads one table".equals(exception.getMessage())) {
                return true;
            }
            throw exception;
        }
    }

    private void createComplexView(CreateView statement, String stored) {
        Query query = SqlParser.parseViewQuery(statement.source());
        SqlResult preview = runQuery(query);
        List<String> names = new ArrayList<>();
        if (statement.columns().isEmpty()) {
            for (ColumnMeta column : preview.columns()) {
                names.add(column.name());
            }
        } else {
            if (statement.columns().size() != preview.columns().size()) {
                throw new SeaException("view column count does not match the SELECT");
            }
            names.addAll(statement.columns());
        }
        Set<String> used = new HashSet<>();
        for (String column : names) {
            Names.checkName(column, "column");
            if (!used.add(Names.fold(column))) {
                throw new SeaException("duplicate column in the view: " + column);
            }
        }
        String joined = String.join(VIEW_COLUMN_SEPARATOR, names);
        systemExecute("""
                INSERT INTO SEA$VIEWS (
                    SEA$VIEW_NAME, SEA$VIEW_SOURCE, SEA$RELATION_NAME, SEA$COLUMNS, SEA$OWNER_NAME
                ) VALUES (?, ?, ?, ?, ?)
                """, stored, statement.source(), viewRelation(query), joined, ownerName());
        recordOwner(stored);
    }

    private String viewRelation(Query query) {
        if (query instanceof Select select && select.table() != null) {
            return storedRelationName(select.table());
        }
        if (query instanceof UnionQuery union) {
            return viewRelation(union.left());
        }
        if (query instanceof WithQuery with) {
            return viewRelation(with.body());
        }
        return "";
    }

    private void dropView(DropView statement) {
        StoredView view = findView(statement.name());
        if (view == null) {
            if (statement.ifExists()) {
                return;
            }
            throw new SeaException("view not found: " + shown(statement.name()));
        }
        guardRoutine("the view", view.name(), view.owner(), null);
        ensureNoViewDepends(view.storedKey());
        deleteCatalog(SystemTables.VIEWS, "SEA$VIEW_NAME", view.storedKey());
        forgetPrivileges(view.storedKey());
    }

    private void replaceView(StoredView existing, CreateView statement) {
        guardRoutine("the view", existing.name(), existing.owner(), null);
        List<String> names = describeView(statement);
        if (!sameOutput(existing.columns(), names)) {
            throw new SeaException("view output columns changed: " + existing.name());
        }
        systemExecute("UPDATE SEA$VIEWS SET SEA$VIEW_SOURCE = ? WHERE SEA$VIEW_NAME = ?",
                statement.source(), existing.storedKey());
    }

    private List<String> describeView(CreateView statement) {
        if (complexView(statement.source())) {
            SqlResult preview = runQuery(SqlParser.parseViewQuery(statement.source()));
            if (statement.columns().isEmpty()) {
                List<String> names = new ArrayList<>();
                for (ColumnMeta column : preview.columns()) {
                    names.add(column.name());
                }
                return names;
            }
            if (statement.columns().size() != preview.columns().size()) {
                throw new SeaException("view column count does not match the SELECT");
            }
            return statement.columns();
        }
        Select select = SqlParser.parseView(statement.source());
        Schema base = relationSchema(select.table());
        List<SelectItem> items = viewItems(select, base);
        if (statement.columns().isEmpty()) {
            List<String> names = new ArrayList<>();
            for (SelectItem item : items) {
                names.add(item.label());
            }
            return names;
        }
        if (statement.columns().size() != items.size()) {
            throw new SeaException("view column count does not match the SELECT");
        }
        return statement.columns();
    }

    private void replaceProcedure(StoredProcedure existing, CreateProcedure statement) {
        guardRoutine("the procedure", existing.label(), existing.owner(), null);
        List<ParamDef> outputs = parameters(existing.schema(), existing.name(), 1);
        if (outputs.size() != statement.outputs().size()) {
            throw new SeaException("procedure output columns changed: " + existing.label());
        }
        for (int i = 0; i < outputs.size(); i++) {
            ParamDef before = outputs.get(i);
            ParamDef after = statement.outputs().get(i);
            if (!Names.same(before.name(), after.name()) || before.type() != after.type()) {
                throw new SeaException("procedure output columns changed: " + existing.label());
            }
        }
        List<ParamDef> inputs = parameters(existing.schema(), existing.name(), 0);
        try {
            systemExecute("""
                    DELETE FROM SEA$PROCEDURE_PARAMETERS
                    WHERE SEA$SCHEMA_NAME = ? AND SEA$PROCEDURE_NAME = ?
                    """, existing.schema(), existing.name());
            insertParameters(existing.schema(), existing.name(), statement.inputs(), 0);
            insertParameters(existing.schema(), existing.name(), statement.outputs(), 1);
            systemExecute("""
                    UPDATE SEA$PROCEDURES SET SEA$PROCEDURE_SOURCE = ?
                    WHERE SEA$SCHEMA_NAME = ? AND SEA$PROCEDURE_NAME = ?
                    """, statement.source(), existing.schema(), existing.name());
        } catch (RuntimeException exception) {
            if (!inTransaction) {
                systemExecute("""
                        DELETE FROM SEA$PROCEDURE_PARAMETERS
                        WHERE SEA$SCHEMA_NAME = ? AND SEA$PROCEDURE_NAME = ?
                        """, existing.schema(), existing.name());
                insertParameters(existing.schema(), existing.name(), inputs, 0);
                insertParameters(existing.schema(), existing.name(), outputs, 1);
            }
            throw exception;
        }
    }

    private boolean sameOutput(List<String> before, List<String> after) {
        if (before.size() != after.size()) {
            return false;
        }
        for (int i = 0; i < before.size(); i++) {
            if (!Names.same(before.get(i), after.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void rejectView(String name) {
        if (findView(name) != null) {
            throw new SeaException("view is read-only: " + name);
        }
    }

    private int insteadInsert(Insert statement, StoredView view) {
        if (triggersFor(view.storedKey(), "INSTEAD OF", "INSERT").isEmpty()) {
            throw new SeaException("view is read-only: " + statement.table());
        }
        Schema schema = viewWriteSchema(view);
        List<Object[]> rows = incomingRows(statement, schema);
        for (Object[] values : rows) {
            fireInstead(view, schema, "INSERT", null, values);
        }
        return rows.size();
    }

    private int insteadUpdate(Update statement, StoredView view) {
        if (triggersFor(view.storedKey(), "INSTEAD OF", "UPDATE").isEmpty()) {
            throw new SeaException("view is read-only: " + statement.table());
        }
        Schema schema = viewWriteSchema(view);
        SqlResult selected = query(new Select(false, true, List.of(), statement.table(), null, List.of(), statement.where(),
                List.of(), null, List.of(), null, null));
        int count = 0;
        for (Row row : selected.rows()) {
            Object[] old = rowValues(row);
            Object[] neu = old.clone();
            Evaluator evaluator = evaluator(null, null);
            evaluator.useView(schema, schema.name);
            evaluator.bindView(old);
            for (Assignment assignment : statement.sets()) {
                int index = schema.indexOf(assignment.column());
                if (index < 0) {
                    throw new SeaException("column not found: " + assignment.column());
                }
                neu[index] = prepared(schema, schema.columns.get(index), evaluator.eval(assignment.expr()));
            }
            fireInstead(view, schema, "UPDATE", old, neu);
            count++;
        }
        return count;
    }

    private int insteadDelete(Delete statement, StoredView view) {
        if (triggersFor(view.storedKey(), "INSTEAD OF", "DELETE").isEmpty()) {
            throw new SeaException("view is read-only: " + statement.table());
        }
        Schema schema = viewWriteSchema(view);
        SqlResult selected = query(new Select(false, true, List.of(), statement.table(), null, List.of(), statement.where(),
                List.of(), null, List.of(), null, null));
        for (Row row : selected.rows()) {
            fireInstead(view, schema, "DELETE", rowValues(row), null);
        }
        return selected.size();
    }

    private Schema viewWriteSchema(StoredView view) {
        try {
            return new Schema(view.name(), viewOutputColumns(view), new int[0], List.of(), List.of());
        } catch (SeaException exception) {
            SqlResult preview = runQuery(SqlParser.parseViewQuery(view.source()));
            return new Schema(view.name(), viewColumns(view, preview.columns()), new int[0], List.of(), List.of());
        }
    }

    private void fireInstead(StoredView view, Schema schema, String event, Object[] oldValues, Object[] newValues) {
        String previousPin = pinnedSchema;
        pinnedSchema = view.schema();
        try {
            for (TriggerDef trigger : triggersFor(view.storedKey(), "INSTEAD OF", event)) {
                Routine routine = SqlParser.parseRoutine(trigger.source);
                RoutineScope local = new RoutineScope();
                local.preferVariables = true;
                local.bindRow(schema, newValues, oldValues);
                for (ParamDef variable : routine.locals()) {
                    local.declare(variable, null);
                }
                RoutineScope previousScope = scope;
                Map<String, OpenCursor> previousCursors = cursors;
                RoutineFrame previousFrame = frame;
                boolean previousHalt = psqlHalt;
                scope = local;
                cursors = installCursors(routine);
                frame = null;
                psqlHalt = false;
                enterRoutine();
                try {
                    runPsql(routine.body());
                } finally {
                    leaveRoutine();
                    scope = previousScope;
                    cursors = previousCursors;
                    frame = previousFrame;
                    psqlHalt = previousHalt;
                }
            }
        } finally {
            pinnedSchema = previousPin;
        }
    }

    private void runBlock(PsqlBlock block) {
        try {
            for (Psql item : block.statements()) {
                runPsql(item);
                if (psqlHalt) {
                    return;
                }
            }
        } catch (SeaException exception) {
            PsqlHandler handler = null;
            for (PsqlHandler candidate : block.handlers()) {
                if (candidate.any() || exception instanceof Raised) {
                    handler = candidate;
                    break;
                }
            }
            if (handler == null) {
                throw exception;
            }
            if (scope == null || !scope.hasVariable(handler.variable())) {
                throw new SeaException("variable " + handler.variable() + " is not declared");
            }
            scope.setVariable(handler.variable(), exception.getMessage());
            runPsql(handler.body());
        }
    }

    private void executeDynamic(PsqlExecuteStatement statement) {
        Object text = evalRoutine(statement.sql());
        if (!(text instanceof String sql) || sql.isBlank()) {
            throw new SeaException("EXECUTE STATEMENT expects text");
        }
        Object[] arguments = new Object[statement.arguments().size()];
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = evalRoutine(statement.arguments().get(i));
        }
        boolean previous = suppressCapture;
        suppressCapture = true;
        try {
            SqlResult result = execute(actor, sql, arguments);
            if (!statement.targets().isEmpty()) {
                if (!result.isQuery() || result.size() != 1) {
                    throw new SeaException("EXECUTE STATEMENT INTO expects one row");
                }
                assignFetched(statement.targets(), result.rows().get(0));
                return;
            }
            if (scope != null) {
                scope.rowCount = result.isQuery() ? result.size() : result.affectedRows();
            }
        } finally {
            suppressCapture = previous;
        }
    }

    private void ensureNoViewDepends(String name) {
        for (StoredView view : allViews()) {
            if (Names.same(view.relation(), name)) {
                throw new SeaException("view " + view.storedKey() + " depends on " + name);
            }
        }
    }

    private List<SelectItem> viewItems(Select select, Schema base) {
        if (select.star()) {
            return star(base);
        }
        return expandStars(select.items(), base, base.name, relationNamespace(select.table()), select.table(), select.alias());
    }

    private String relationNamespace(String written) {
        StoredView view = findView(written);
        if (view != null) {
            return view.schema();
        }
        return require(written).namespace();
    }

    private Schema relationSchema(String name) {
        if (name == null) {
            throw new SeaException("view needs a table");
        }
        StoredView view = findView(name);
        if (view == null) {
            return require(name).schema();
        }
        return new Schema(view.name(), viewOutputColumns(view), new int[0], List.of(), List.of());
    }

    private List<Column> viewOutputColumns(StoredView view) {
        if (viewDepth >= 16) {
            throw new SeaException("view exceeded 16 levels");
        }
        viewDepth++;
        try {
            Select select = preparedView(view);
            Schema base = relationSchema(select.table());
            List<SelectItem> items = viewItems(select, base);
            if (items.size() != view.columns().size()) {
                throw new SeaException("the view " + view.name() + " does not match the stored SELECT");
            }
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                columns.add(outputColumn(view.columns().get(i), items.get(i).expr(), base));
            }
            return columns;
        } finally {
            viewDepth--;
        }
    }

    private List<Column> viewColumns(StoredView view, List<ColumnMeta> metas) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < view.columns().size(); i++) {
            DataType type = metas.get(i).type();
            int length = type == DataType.VARCHAR || type == DataType.CHAR ? 256 : 0;
            columns.add(new Column(view.columns().get(i), type, length, 0, false, false, 0L, false, null));
        }
        return columns;
    }

    private Column outputColumn(String name, Expr expr, Schema base) {
        if (expr instanceof ColumnRef ref) {
            Column source = base.column(ref.name());
            return new Column(name, source.type, source.length, source.scale, false, false, 0L, false, null);
        }
        DataType type = infer(expr, base);
        int length = type == DataType.VARCHAR || type == DataType.CHAR ? 256 : 0;
        return new Column(name, type, length, 0, false, false, 0L, false, null);
    }

    private Session ownerSession(String user) {
        long attachment = actor == null ? 0 : actor.attachmentId;
        if (user == null || user.isBlank()) {
            return new Session(SystemTables.PUBLIC, false, attachment);
        }
        SqlResult rows = systemExecute("SELECT SEA$ADMIN FROM SEA$USERS WHERE SEA$USER_NAME = ?", user);
        boolean admin = rows.size() == 1 && Boolean.TRUE.equals(rows.rows().get(0).getBoolean(0));
        return new Session(user, admin, attachment);
    }

    private String storedRelationName(String name) {
        StoredView view = findView(name);
        if (view != null) {
            return view.storedKey();
        }
        TableStore table = require(name);
        String stored = Relation.catalogName(table.namespace(), table.schema().name);
        if (stored.length() > 128) {
            throw new SeaException("qualified name exceeds 128 characters");
        }
        return stored;
    }

    /**
     * A view defined with {@code SELECT *} is positional. Before the first move,
     * record the base columns in that order so each view column keeps its value.
     */
    private void bindViewColumns(TableStore table) {
        String relation = Relation.catalogName(table.namespace(), table.schema().name);
        List<String> sources = new ArrayList<>();
        for (Column column : table.schema().columns) {
            sources.add(column.name);
        }
        for (StoredView view : allViews()) {
            if (!Names.same(view.relation(), relation) || !view.sources().isEmpty()) {
                continue;
            }
            Select select = SqlParser.parseView(view.source());
            if (!select.star() || view.columns().size() != sources.size()) {
                continue;
            }
            String packed = packViewColumns(view.columns(), sources);
            if (packed.length() > Format.MAX_CHAR) {
                throw new SeaException("view column list exceeds " + Format.MAX_CHAR + " characters");
            }
            systemExecute("UPDATE SEA$VIEWS SET SEA$COLUMNS = ? WHERE SEA$VIEW_NAME = ?", packed, view.storedKey());
        }
    }

    private static String packViewColumns(List<String> columns, List<String> sources) {
        String joined = String.join(VIEW_COLUMN_SEPARATOR, columns);
        if (sources == null || sources.isEmpty()) {
            return joined;
        }
        return joined + VIEW_BINDING_SEPARATOR + String.join(VIEW_COLUMN_SEPARATOR, sources);
    }

    private Select preparedView(StoredView view) {
        Select select = boundView(view);
        if (!select.star() || view.sources().isEmpty() || view.sources().size() != view.columns().size()) {
            return select;
        }
        Schema base = relationSchema(select.table());
        List<SelectItem> items = new ArrayList<>();
        for (String name : view.sources()) {
            Column column = base.column(name);
            items.add(new SelectItem(new ColumnRef(null, column.name), null, column.name));
        }
        return new Select(select.distinct(), false, items, select.table(), select.alias(), select.joins(),
                select.where(), select.groupBy(), select.having(), select.orderBy(), select.limit(), select.offset());
    }

    private Select boundView(StoredView view) {
        Select select = SqlParser.parseView(view.source());
        if (select.table() == null || Relation.parse(select.table()).schema() != null) {
            return select;
        }
        String bound = view.relation();
        if (bound.indexOf('.') < 0) {
            bound = Relation.PUBLIC + "." + bound;
        }
        return new Select(select.distinct(), select.star(), select.items(), bound, select.alias(), select.joins(),
                select.where(), select.groupBy(), select.having(), select.orderBy(), select.limit(), select.offset());
    }

    private StoredRow findGenerator(String written) {
        TableStore table = systemTable(SystemTables.GENERATORS);
        if (table == null || written == null) {
            return null;
        }
        Relation rel = relation(written);
        StoredRow found = null;
        for (StoredRow row : table.scan()) {
            if (!Names.same(text(table, row, "SEA$SCHEMA_NAME"), rel.schema())
                    || !Names.same(text(table, row, "SEA$GENERATOR_NAME"), rel.name())) {
                continue;
            }
            if (found != null) {
                throw new SeaException("ambiguous name: " + shown(written));
            }
            found = row;
        }
        return found;
    }

    private List<String> generatorNames(String schema) {
        TableStore table = systemTable(SystemTables.GENERATORS);
        if (table == null) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            if (Names.same(text(table, row, "SEA$SCHEMA_NAME"), schema)) {
                names.add(text(table, row, "SEA$GENERATOR_NAME"));
            }
        }
        return names;
    }

    private void deleteGenerator(String schema, String name) {
        TableStore table = require(SystemTables.GENERATORS);
        List<Long> ids = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            if (Names.same(text(table, row, "SEA$SCHEMA_NAME"), schema)
                    && Names.same(text(table, row, "SEA$GENERATOR_NAME"), name)) {
                ids.add(row.id);
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        boolean previous = bypass;
        bypass = true;
        try {
            mutating(SystemTables.GENERATORS, () -> {
                table.delete(ids);
                return ids.size();
            });
        } finally {
            bypass = previous;
        }
    }

    private StoredProcedure findProcedure(String name) {
        TableStore table = systemTable(SystemTables.PROCEDURES);
        if (table == null || name == null) {
            return null;
        }
        Relation rel = relation(name);
        StoredProcedure found = null;
        for (StoredRow row : table.scan()) {
            if (!Names.same(text(table, row, "SEA$SCHEMA_NAME"), rel.schema())
                    || !Names.same(text(table, row, "SEA$PROCEDURE_NAME"), rel.name())) {
                continue;
            }
            if (found != null) {
                throw new SeaException("ambiguous name: " + shown(name));
            }
            found = readProcedure(table, row);
        }
        return found;
    }

    private List<StoredProcedure> allProcedures() {
        TableStore table = systemTable(SystemTables.PROCEDURES);
        if (table == null) {
            return List.of();
        }
        List<StoredProcedure> procedures = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            procedures.add(readProcedure(table, row));
        }
        return procedures;
    }

    private StoredProcedure readProcedure(TableStore table, StoredRow row) {
        String schema = text(table, row, "SEA$SCHEMA_NAME");
        String name = text(table, row, "SEA$PROCEDURE_NAME");
        if (schema == null || schema.isBlank() || name == null || name.isBlank()) {
            throw new SeaException("inconsistent catalog in the procedure");
        }
        return new StoredProcedure(schema, name, text(table, row, "SEA$PROCEDURE_SOURCE"), text(table, row, "SEA$OWNER_NAME"));
    }

    private void deleteProcedure(String schema, String name) {
        deleteProcedureRows(SystemTables.PROCEDURE_PARAMETERS, schema, name);
        deleteProcedureRows(SystemTables.PROCEDURES, schema, name);
    }

    private void deleteProcedureRows(String tableName, String schema, String name) {
        TableStore table = require(tableName);
        List<Long> ids = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            if (Names.same(text(table, row, "SEA$SCHEMA_NAME"), schema)
                    && Names.same(text(table, row, "SEA$PROCEDURE_NAME"), name)) {
                ids.add(row.id);
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        boolean previous = bypass;
        bypass = true;
        try {
            mutating(tableName, () -> {
                table.delete(ids);
                return ids.size();
            });
        } finally {
            bypass = previous;
        }
    }

    private StoredView findView(String name) {
        TableStore table = systemTable(SystemTables.VIEWS);
        if (table == null || name == null) {
            return null;
        }
        Relation rel = relation(name);
        String expected = Relation.catalogName(rel.schema(), rel.name());
        int nameIndex = table.schema().indexOf("SEA$VIEW_NAME");
        StoredView found = null;
        for (StoredRow row : table.scan()) {
            Object value = table.value(row, nameIndex);
            if (value instanceof String text && Names.same(text, expected)) {
                if (found != null) {
                    throw new SeaException("ambiguous name: " + shown(name));
                }
                found = readView(table, row);
            }
        }
        return found;
    }

    private List<StoredView> allViews() {
        TableStore table = systemTable(SystemTables.VIEWS);
        if (table == null) {
            return List.of();
        }
        List<StoredView> views = new ArrayList<>();
        for (StoredRow row : table.scan()) {
            views.add(readView(table, row));
        }
        return views;
    }

    private StoredView readView(TableStore table, StoredRow row) {
        String packed = text(table, row, "SEA$COLUMNS");
        List<String> columns = new ArrayList<>();
        List<String> sources = List.of();
        if (packed != null && !packed.isBlank()) {
            String outputs = packed;
            int bound = packed.indexOf(VIEW_BINDING_SEPARATOR);
            if (bound >= 0) {
                outputs = packed.substring(0, bound);
                String rest = packed.substring(bound + VIEW_BINDING_SEPARATOR.length());
                if (!rest.isEmpty()) {
                    sources = List.copyOf(Arrays.asList(rest.split(VIEW_COLUMN_SEPARATOR, -1)));
                }
            }
            if (!outputs.isEmpty()) {
                columns.addAll(Arrays.asList(outputs.split(VIEW_COLUMN_SEPARATOR, -1)));
            }
        }
        String storedName = text(table, row, "SEA$VIEW_NAME");
        Relation id = storedName.indexOf('.') >= 0
                ? Relation.parse(storedName)
                : new Relation(Relation.PUBLIC, storedName);
        return new StoredView(
                id.schema() == null ? Relation.PUBLIC : id.schema(),
                id.name(),
                text(table, row, "SEA$VIEW_SOURCE"),
                text(table, row, "SEA$RELATION_NAME"),
                columns,
                sources,
                text(table, row, "SEA$OWNER_NAME"));
    }

    private void checkOpen() {
        if (closed) {
            throw new SeaException("database is already closed");
        }
    }

    private record Projected(StoredRow row, Object[] values, Object[] source) {
    }

    private record Held(List<Evaluator.Frame> frames, List<Object[]> rows) {
    }

    private record Probe(List<Evaluator.Frame> frames, List<Object[]> rows, TableStore table, String alias) {
    }

    private record Equi(Evaluator.Frame left, int leftIndex, Evaluator.Frame right, int rightIndex) {
    }

    private record Fixed(Map<Integer, Object> bound, boolean contradiction) {
    }

    private interface GroupSource {
        int size();

        void bind(int index);

        Projected projected(int index, Object[] values);

        String identify(ColumnRef column);

        boolean hasColumn(String name);
    }

    private record StoredDomain(String schema, String name, DataType type, int length, int scale,
                                String defaultSource, String check, String owner) {
    }

    private record StoredFunction(String schema, String name, String source, DataType type, int length, int scale, String owner) {
        String label() {
            if (Names.same(schema, Relation.PUBLIC)) {
                return name;
            }
            return schema + "." + name;
        }
    }

    private record StoredProcedure(String schema, String name, String source, String owner) {
        String label() {
            if (Names.same(schema, Relation.PUBLIC)) {
                return name;
            }
            return schema + "." + name;
        }
    }

    private record StoredView(String schema, String name, String source, String relation, List<String> columns,
                              List<String> sources, String owner) {
        String storedKey() {
            return Relation.catalogName(schema, name);
        }
    }

    private record Account(String user, boolean admin) {
    }

    private static final class EqualsProbe {
        final Map<Integer, Object> values = new LinkedHashMap<>();
        final Map<String, Object> calls = new LinkedHashMap<>();
        boolean contradiction;
        Integer rangeColumn;
        Object low;
        Object high;
        boolean lowInclusive = true;
        boolean highInclusive = true;
        String likePrefix;
        Integer orColumn;
        List<Object> orValues;
    }

    private static final class RoutineFrame {
        private final List<ParamDef> outputs;
        private final boolean stopAtFirstSuspend;
        private final List<Object[]> rows = new ArrayList<>();
        private boolean suspended;
        private Object returned;

        private RoutineFrame(List<ParamDef> outputs, boolean selectable) {
            this.outputs = outputs;
            this.stopAtFirstSuspend = !selectable;
        }
    }

    private record TriggerDef(String name, int position, String source) {
    }

    private record ProcedureRows(String name, List<ParamDef> outputs, List<Object[]> rows) {
    }

    private void pinRead(TableStore table) {
        OpenTxn txn = currentTxn();
        if (txn == null) {
            return;
        }
        if (txn.readPin.containsKey(table)) {
            return;
        }
        txn.readPin.put(table, table.capture());
    }

    private TableStore.Snapshot pinnedRead(TableStore table) {
        if (!repeatable()) {
            return null;
        }
        OpenTxn txn = currentTxn();
        if (txn == null) {
            return null;
        }
        return txn.readPin.get(table);
    }

    private boolean repeatable() {
        return actor != null && (actor.isolation == 4 || actor.isolation == 8);
    }

    private TableStore.Snapshot uncommitted(TableStore table) {
        if (actor == null || actor.isolation != 1) {
            return null;
        }
        for (Map.Entry<Session, OpenTxn> entry : transactions.entrySet()) {
            if (entry.getKey() == actor) {
                continue;
            }
            TableStore.Snapshot workspace = entry.getValue().workspace.get(table);
            if (workspace != null) {
                return workspace;
            }
        }
        return null;
    }

    private void lockRows(TableStore table, List<StoredRow> rows) {
        if (!inTransaction || actor == null || SystemTables.isSystem(table.schema().name)) {
            return;
        }
        if (actor.isolation == 1) {
            return;
        }
        List<Long> ids = new ArrayList<>();
        for (StoredRow row : rows) {
            ids.add(row.id);
        }
        for (Long id : ids) {
            Session holder = lockHolder(table, id);
            if (holder == null || holder == actor) {
                rowLocks.computeIfAbsent(table, key -> new HashMap<>()).put(id, actor);
                continue;
            }
            int timeout = lockTimeoutSeconds();
            long deadline = System.nanoTime() + timeout * 1_000_000_000L;
            waiting.put(actor, holder);
            try {
                while (lockHolder(table, id) == holder) {
                    if (waitsFor(holder, actor)) {
                        throw new SeaException("deadlock");
                    }
                    long left = deadline - System.nanoTime();
                    if (timeout == 0 || left <= 0) {
                        throw new SeaException("lock timeout");
                    }
                    try {
                        wait(Math.min(left / 1_000_000L, 200));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new SeaException("lock timeout");
                    }
                }
            } finally {
                waiting.remove(actor);
            }
            rowLocks.computeIfAbsent(table, key -> new HashMap<>()).put(id, actor);
        }
        notifyAll();
    }

    private Session lockHolder(TableStore table, long id) {
        Map<Long, Session> locks = rowLocks.get(table);
        return locks == null ? null : locks.get(id);
    }

    private boolean waitsFor(Session holder, Session me) {
        Session cursor = waiting.get(holder);
        int guard = 0;
        while (cursor != null && guard++ < 64) {
            if (cursor == me) {
                return true;
            }
            cursor = waiting.get(cursor);
        }
        return false;
    }

    private void releaseLocks(Session session) {
        for (Map<Long, Session> locks : rowLocks.values()) {
            locks.entrySet().removeIf(entry -> entry.getValue() == session);
        }
        waiting.remove(session);
        notifyAll();
    }

    private int lockTimeoutSeconds() {
        for (TableStore table : tables.values()) {
            if (!Names.same(table.schema().name, SystemTables.CONFIG)) {
                continue;
            }
            for (StoredRow row : table.scan()) {
                Object key = table.value(row, 0);
                if (key != null && "lock.timeout".equalsIgnoreCase(String.valueOf(key))) {
                    try {
                        return Math.max(0, Integer.parseInt(String.valueOf(table.value(row, 1)).trim()));
                    } catch (NumberFormatException exception) {
                        return 10;
                    }
                }
            }
        }
        return 10;
    }

    private void makeSavepoint(String name) {
        Names.checkName(name, "savepoint");
        OpenTxn txn = currentTxn();
        if (txn == null) {
            throw new SeaException("there is no open transaction");
        }
        txn.savepoints.removeIf(mark -> Names.same(mark.name, name));
        SaveMark mark = new SaveMark(name, pending.size());
        for (TableStore table : txn.tables.keySet()) {
            if (!Names.same(table.schema().name, SystemTables.GENERATORS)) {
                mark.rows.put(table, table.capture());
            }
        }
        mark.created.putAll(txn.created);
        mark.dropped.putAll(txn.dropped);
        txn.savepoints.add(mark);
    }

    private void rollbackToSavepoint(String name) {
        OpenTxn txn = currentTxn();
        if (txn == null) {
            throw new SeaException("there is no open transaction");
        }
        int at = -1;
        for (int i = 0; i < txn.savepoints.size(); i++) {
            if (Names.same(txn.savepoints.get(i).name, name)) {
                at = i;
            }
        }
        if (at < 0) {
            throw new SeaException("savepoint not found: " + name);
        }
        SaveMark mark = txn.savepoints.get(at);
        for (TableStore table : List.copyOf(txn.created.keySet())) {
            if (!mark.created.containsKey(table)) {
                txn.created.remove(table);
                txn.tables.remove(table);
                if (tables.containsValue(table)) {
                    discardTable(table);
                }
            }
        }
        for (Map.Entry<String, TableStore> entry : List.copyOf(txn.dropped.entrySet())) {
            if (!mark.dropped.containsKey(entry.getKey())) {
                tables.put(entry.getKey(), entry.getValue());
                txn.dropped.remove(entry.getKey());
            }
        }
        for (Map.Entry<TableStore, TableStore.Snapshot> entry : mark.rows.entrySet()) {
            if (entry.getKey().schema() != null) {
                entry.getKey().restore(entry.getValue());
            }
        }
        if (pending.size() > mark.pendingSize) {
            pending.subList(mark.pendingSize, pending.size()).clear();
        }
        while (txn.savepoints.size() > at + 1) {
            txn.savepoints.remove(txn.savepoints.size() - 1);
        }
    }

    private void releaseSavepoint(String name) {
        OpenTxn txn = currentTxn();
        if (txn == null) {
            throw new SeaException("there is no open transaction");
        }
        int at = -1;
        for (int i = 0; i < txn.savepoints.size(); i++) {
            if (Names.same(txn.savepoints.get(i).name, name)) {
                at = i;
            }
        }
        if (at < 0) {
            throw new SeaException("savepoint not found: " + name);
        }
        while (txn.savepoints.size() > at) {
            txn.savepoints.remove(txn.savepoints.size() - 1);
        }
    }

    private void track(TableStore table) {
        Session owner = transactionSession != null ? transactionSession : actor;
        if (owner == null) {
            return;
        }
        OpenTxn txn = transactions.get(owner);
        if (txn == null || txn.tables.containsKey(table)) {
            return;
        }
        table.materializeDirectory();
        table.markShared();
        TableStore.Snapshot layout = table.savepoint();
        txn.layout.put(table, layout);
        txn.tables.put(table, Boolean.TRUE);
        table.fork();
        installed = owner;
    }

    private void rejectBorrowed(TableStore table) {
        if (table == null) {
            return;
        }
        for (Map.Entry<Session, OpenTxn> entry : transactions.entrySet()) {
            if (entry.getKey() != actor && entry.getValue().tables.containsKey(table)) {
                throw new SeaException("another transaction is using the table " + table.schema().name);
            }
        }
    }

    private <T> T seeing(Session session, java.util.function.Supplier<T> action) {
        boolean lifted = session != null && transactions.containsKey(session) && installed != session;
        if (lifted) {
            activate(session);
        }
        try {
            return action.get();
        } finally {
            if (lifted) {
                park(session);
                inTransaction = false;
                transactionSession = null;
                transactionId = null;
            }
        }
    }

    private void activate(Session session) {
        OpenTxn txn = transactions.get(session);
        if (txn == null) {
            return;
        }
        installed = session;
        inTransaction = true;
        transactionSession = session;
        transactionId = txn.id;
        for (TableStore table : txn.tables.keySet()) {
            TableStore.Snapshot latest = table.savepoint();
            txn.held.put(table, latest);
            TableStore.Snapshot workspace = txn.workspace.get(table);
            if (workspace != null) {
                table.restore(workspace);
                table.raiseAllocation(latest.schema, latest.nextRowId);
            }
        }
    }

    private void park(Session session) {
        OpenTxn txn = transactions.get(session);
        if (txn == null) {
            installed = null;
            return;
        }
        for (TableStore table : txn.tables.keySet()) {
            long next = table.nextRowId();
            Schema schema = table.schema();
            TableStore.Snapshot shot = table.savepoint();
            if (session.isolation == 4 || session.isolation == 8) {
                shot = table.pin(shot);
            }
            txn.workspace.put(table, shot);
            TableStore.Snapshot back = txn.held.remove(table);
            if (back == null) {
                back = txn.layout.get(table);
            }
            if (back != null) {
                table.restore(back);
                table.raiseAllocation(schema, next);
            }
        }
        installed = null;
    }

    private void rollbackSession(Session session) {
        pending.clear();
        undoRenames(0);
        releaseLocks(session);
        OpenTxn txn = transactions.remove(session);
        if (txn == null) {
            throw new SeaException("there is no open transaction");
        }
        if (storage.leaveForRecovery()) {
            restoreTxnMemory(session, txn);
            clearTxnFlags(session);
            return;
        }
        if (txn.hardened && txn.hardenedImage != null && !txn.hardenedImage.empty()) {
            if (installed == session) {
                park(session);
            }
            awaitPageForce();
            applyUndo(txn.hardenedImage);
            undoCatalogBeyond(txn, txn.hardenedImage);
            refreshActiveUndo();
            rollbackMarker = txn.id;
            try {
                flush();
            } finally {
                rollbackMarker = null;
            }
            clearTxnFlags(session);
            return;
        }
        restoreTxnMemory(session, txn);
        undoCatalog(txn);
        if (!suppressCapture && !storage.leaveForRecovery()) {
            awaitPageForce();
            storage.logRollback(txn.id);
        }
        clearTxnFlags(session);
    }

    private void restoreTxnMemory(Session session, OpenTxn txn) {
        if (installed == session) {
            for (TableStore table : txn.tables.keySet()) {
                TableStore.Snapshot back = txn.held.get(table);
                if (back == null) {
                    back = txn.layout.get(table);
                }
                if (back != null) {
                    table.restore(back);
                }
            }
        }
    }

    private void clearTxnFlags(Session session) {
        if (installed == session || transactionSession == session) {
            installed = null;
            inTransaction = false;
            transactionSession = null;
            transactionId = null;
        }
    }

    void checkpoint() {
        checkpoint(true);
    }

    void checkpointPolicy(CheckpointPolicy next) {
        if (next == null) {
            throw new SeaException("checkpoint policy is required");
        }
        policy = next;
        if (checkpointThread == null && next.intervalMillis() > 0 && !closed) {
            startCheckpoints();
        }
    }

    /** Closes the files as a process crash: no rollback and no checkpoint. */
    void crash() {
        if (closed) {
            return;
        }
        stopping = true;
        if (checkpointThread != null) {
            checkpointThread.interrupt();
        }
        awaitPageForce();
        closed = true;
        storage.dropDeferredCheckpoint();
        storage.abandon();
    }

    private void startCheckpoints() {
        if (policy.intervalMillis() <= 0 || checkpointThread != null) {
            return;
        }
        lastCheckpointNanos = System.nanoTime();
        checkpointThread = new Thread(this::checkpointLoop, "seadb-checkpoint");
        checkpointThread.setDaemon(true);
        checkpointThread.start();
    }

    private void checkpointLoop() {
        while (!stopping) {
            long wait;
            synchronized (this) {
                wait = policy.intervalMillis();
            }
            if (wait <= 0 || stopping) {
                return;
            }
            try {
                Thread.sleep(wait);
            } catch (InterruptedException interrupted) {
                return;
            }
            synchronized (this) {
                if (stopping || closed) {
                    return;
                }
                try {
                    maybeCheckpoint();
                } catch (RuntimeException exception) {
                    if (storage.leaveForRecovery()) {
                        stopping = true;
                    }
                }
            }
        }
    }

    private void maybeCheckpoint() {
        if (checkpointRunning || closed || stopping || storage.leaveForRecovery() || !policyDue()) {
            return;
        }
        // Pages of an open transaction are written between statements. The file sync
        // runs on another thread so the insert does not wait for it. Commit still
        // syncs whatever that thread has not published yet.
        if (!transactions.isEmpty()) {
            requestPageSync();
            return;
        }
        checkpoint(true);
    }

    private void requestPageSync() {
        Thread running = pageSync;
        if (running != null && running.isAlive() && running != Thread.currentThread()) {
            return;
        }
        Thread worker = new Thread(this::runPageSync, "seadb-sync");
        worker.setDaemon(true);
        pageSync = worker;
        worker.start();
    }

    private void runPageSync() {
        List<TableStore> frozen = List.of();
        List<TransactionLog.TxnUndo> frozenUndo = List.of();
        long mark = -1;
        synchronized (this) {
            if (closed || stopping || transactions.isEmpty() || !policyDue()) {
                return;
            }
            mark = storage.logLength();
            frozen = freezeDirty();
            if (!frozen.isEmpty()) {
                frozenUndo = snapshotFrozenUndo();
            }
        }
        boolean encoded = true;
        for (TableStore table : frozen) {
            try {
                table.encodePrepared();
            } catch (RuntimeException exception) {
                table.abortEncode();
                encoded = false;
            }
        }
        List<byte[]> packedUndos = null;
        byte[] packedCheckpoint = null;
        if (encoded && frozenUndo != null && !frozenUndo.isEmpty()) {
            packedUndos = new ArrayList<>();
            for (TransactionLog.TxnUndo undo : frozenUndo) {
                packedUndos.add(storage.packUndo(undo));
            }
            packedCheckpoint = storage.packActive(frozenUndo);
        }
        boolean sync = false;
        synchronized (this) {
            try {
                if (!closed && !stopping && !transactions.isEmpty()) {
                    snapshotLsn = encoded && !frozen.isEmpty();
                    if (packedUndos != null) {
                        storage.armPackedCheckpoint(packedUndos, packedCheckpoint);
                    }
                    storage.beginDeferredSync();
                    try {
                        checkpoint(true);
                    } finally {
                        storage.endDeferredSync();
                        storage.disarmPackedCheckpoint();
                    }
                    if (snapshotLsn) {
                        storage.limitDeferredCheckpoint(mark);
                    }
                }
            } catch (RuntimeException exception) {
                storage.dropDeferredCheckpoint();
            } finally {
                for (TableStore table : frozen) {
                    table.abortEncode();
                }
            }
            if (storage.deferredCheckpointLsn() >= 0 || storage.hasDeferredIo()) {
                forcing = true;
                sync = true;
            }
        }
        if (!sync) {
            return;
        }
        try {
            storage.forceLog();
            storage.flushDeferredIo();
            if (storage.deferredCheckpointLsn() >= 0) {
                storage.forceDurable();
                if (storage.publishDeferredCheckpoint()) {
                    storage.forceDurable();
                }
            }
        } catch (RuntimeException exception) {
            storage.dropDeferredCheckpoint();
        } finally {
            forcing = false;
        }
        synchronized (this) {
            if (!closed && !stopping && !transactions.isEmpty() && policyDue()) {
                requestPageSync();
            }
        }
    }

    private List<TableStore> freezeDirty() {
        List<TableStore> frozen = new ArrayList<>();
        for (Session session : new ArrayList<>(transactions.keySet())) {
            activate(session);
            OpenTxn txn = transactions.get(session);
            if (txn != null) {
                for (TableStore table : txn.tables.keySet()) {
                    if (table.isDirty() && !frozen.contains(table)) {
                        table.beginEncode();
                        frozen.add(table);
                    }
                }
            }
            park(session);
        }
        return frozen;
    }

    /**
     * Insert-only undo as of the frozen pages. Compressed after this lock is
     * released. A structure change returns null so the checkpoint encodes live.
     */
    private List<TransactionLog.TxnUndo> snapshotFrozenUndo() {
        List<TransactionLog.TxnUndo> images = new ArrayList<>();
        for (OpenTxn txn : transactions.values()) {
            if (!txn.created.isEmpty() || !txn.dropped.isEmpty()) {
                return null;
            }
            List<TransactionLog.TableUndo> tables = new ArrayList<>();
            for (TableStore table : txn.tables.keySet()) {
                if (table.structureEpoch() != 0) {
                    return null;
                }
                long[] ids = table.copyInsertedIds();
                if (ids.length == 0) {
                    continue;
                }
                tables.add(new TransactionLog.TableUndo(table.namespace(), table.schema().name, ids, List.of(), List.of()));
            }
            if (!tables.isEmpty()) {
                images.add(new TransactionLog.TxnUndo(txn.id, List.of(), tables));
            }
        }
        return images;
    }

    private void awaitPageForce() {
        while (forcing) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean policyDue() {
        if (policy.intervalMillis() > 0) {
            long elapsed = (System.nanoTime() - lastCheckpointNanos) / 1_000_000L;
            if (elapsed >= policy.intervalMillis()) {
                return true;
            }
        }
        int dirtyTables = 0;
        long dirtyBytes = 0;
        for (Map.Entry<Session, OpenTxn> entry : transactions.entrySet()) {
            for (TableStore table : entry.getValue().tables.keySet()) {
                if (!changedSinceCheckpoint(entry.getKey(), entry.getValue(), table)) {
                    continue;
                }
                dirtyTables++;
                dirtyBytes += 256;
            }
        }
        if (dirtyTables >= policy.dirtyTables() || dirtyBytes >= policy.dirtyBytes()) {
            return true;
        }
        if (storage.logLength() - storage.checkpointLsn() >= policy.logBytes()) {
            return true;
        }
        long maximum = storage.cacheMaximum();
        return maximum > 0 && policy.cachePressure() <= 1
                && storage.cacheResident() >= (long) (maximum * policy.cachePressure());
    }

    private void checkpoint(boolean force) {
        if (checkpointRunning || closed || storage.leaveForRecovery()) {
            return;
        }
        checkpointRunning = true;
        boolean previousIn = inTransaction;
        Session previousSession = transactionSession;
        Long previousId = transactionId;
        Session previousInstalled = installed;
        try {
            if (previousInstalled != null && transactions.containsKey(previousInstalled)) {
                park(previousInstalled);
            }
            List<TransactionLog.TxnUndo> writing = new ArrayList<>();
            List<Session> sessions = new ArrayList<>();
            for (Map.Entry<Session, OpenTxn> entry : transactions.entrySet()) {
                OpenTxn txn = entry.getValue();
                TransactionLog.TxnUndo image = capture(entry.getKey(), txn);
                if (image.empty()) {
                    continue;
                }
                if (txn.hardened && !dirtySince(entry.getKey(), txn)) {
                    continue;
                }
                writing.add(image);
                sessions.add(entry.getKey());
            }
            if (!writing.isEmpty()) {
                storage.logUndo(writing);
                for (int i = 0; i < sessions.size(); i++) {
                    Session session = sessions.get(i);
                    OpenTxn txn = transactions.get(session);
                    if (txn == null) {
                        continue;
                    }
                    activate(session);
                    for (TableStore table : txn.tables.keySet()) {
                        txn.layout.put(table, table.pin(txn.layout.get(table)));
                        txn.held.put(table, table.pin(txn.held.get(table)));
                    }
                    flushMatching(table -> txn.tables.containsKey(table), List.of(), true);
                    txn.hardened = true;
                    txn.hardenedImage = writing.get(i);
                    park(session);
                }
            } else {
                storage.disarmPackedCheckpoint();
            }
            refreshActiveUndo();
            boolean catalogOpen = openCatalogChange();
            if (!catalogOpen) {
                flushCommitted();
            }
            if ((force || !writing.isEmpty()) && (!catalogOpen || !committedPagesPending())) {
                storage.writeCheckpoint();
            }
        } finally {
            if (previousInstalled != null && transactions.containsKey(previousInstalled) && !storage.leaveForRecovery()) {
                activate(previousInstalled);
            } else if (!storage.leaveForRecovery()) {
                installed = previousInstalled != null && transactions.containsKey(previousInstalled) ? previousInstalled : null;
                inTransaction = previousIn;
                transactionSession = previousSession;
                transactionId = previousId;
            }
            lastCheckpointNanos = System.nanoTime();
            checkpointRunning = false;
        }
    }

    private void spillPending(Session session) {
        if (suppressCapture || session == null || pending.isEmpty()) {
            return;
        }
        OpenTxn txn = transactions.get(session);
        if (txn == null) {
            return;
        }
        List<TransactionLog.Applied> fresh = new ArrayList<>();
        for (TransactionLog.Applied applied : pending) {
            if (!transactionControl(applied.sql)) {
                fresh.add(applied);
            }
        }
        pending.clear();
        if (!fresh.isEmpty()) {
            storage.logStatements(txn.id, fresh);
        }
    }

    private void recoverCommittedSql() {
        List<TransactionLog.Batch> batches = storage.takeSqlReplay();
        if (batches.isEmpty()) {
            return;
        }
        Session replay = new Session("DBA", true, 0);
        boolean previousBypass = bypass;
        boolean previousSuppress = suppressCapture;
        bypass = true;
        suppressCapture = true;
        try {
            execute(replay, "BEGIN", new Object[0]);
            for (TransactionLog.Batch batch : batches) {
                for (TransactionLog.Applied statement : batch.statements) {
                    if (transactionControl(statement.sql)) {
                        continue;
                    }
                    replay.schema(statement.schema);
                    try {
                        execute(replay, statement.sql, statement.params);
                    } catch (SeaException exception) {
                        // A commit that did not close still checkpointed the CREATE.
                        // The statement text sits after that checkpoint and would
                        // create the table a second time.
                        if (!alreadyStored(exception)) {
                            throw exception;
                        }
                    }
                    if (!transactions.containsKey(replay)) {
                        execute(replay, "BEGIN", new Object[0]);
                    }
                }
            }
            if (transactions.containsKey(replay)) {
                execute(replay, "COMMIT", new Object[0]);
            }
            flush();
        } finally {
            bypass = previousBypass;
            suppressCapture = previousSuppress;
        }
    }

    private static boolean alreadyStored(SeaException exception) {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        return message.startsWith("table already exists:")
                || message.startsWith("index already exists:")
                || message.startsWith("constraint already exists:")
                || message.startsWith("view already exists:");
    }

    private void flushCommitted() {
        if (closed || storage.leaveForRecovery() || openCatalogChange()) {
            return;
        }
        if (!catalogDirty && !committedPagesPending()) {
            return;
        }
        flushMatching(table -> !openTransactionTable(table), List.of());
    }

    private boolean openTransactionTable(TableStore table) {
        for (OpenTxn txn : transactions.values()) {
            if (txn.tables.containsKey(table)) {
                return true;
            }
        }
        return false;
    }

    private boolean openCatalogChange() {
        for (OpenTxn txn : transactions.values()) {
            if (!txn.created.isEmpty() || !txn.dropped.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private boolean committedPagesPending() {
        for (TableStore table : tables.values()) {
            if (table.isDirty() && !openTransactionTable(table)) {
                return true;
            }
        }
        return false;
    }

    private static boolean transactionControl(String sql) {
        if (sql == null) {
            return false;
        }
        int index = 0;
        while (index < sql.length() && Character.isWhitespace(sql.charAt(index))) {
            index++;
        }
        int start = index;
        while (index < sql.length() && Character.isLetter(sql.charAt(index))) {
            index++;
        }
        if (start == index) {
            return false;
        }
        String word = sql.substring(start, index);
        return word.equalsIgnoreCase("BEGIN")
                || word.equalsIgnoreCase("COMMIT")
                || word.equalsIgnoreCase("ROLLBACK")
                || word.equalsIgnoreCase("START");
    }

    private void recoverUncommitted() {
        if (!storage.undoPending()) {
            return;
        }
        for (TransactionLog.TxnUndo undo : storage.pendingUndos()) {
            applyUndo(undo);
        }
        storage.activeUndo(List.of());
        flush();
        storage.clearUndoPending();
    }

    private void refreshActiveUndo() {
        List<TransactionLog.TxnUndo> images = new ArrayList<>();
        for (Map.Entry<Session, OpenTxn> entry : transactions.entrySet()) {
            OpenTxn txn = entry.getValue();
            if (!txn.hardened) {
                continue;
            }
            TransactionLog.TxnUndo image = capture(entry.getKey(), txn);
            if (image.empty()) {
                txn.hardened = false;
                txn.hardenedImage = null;
                continue;
            }
            txn.hardenedImage = image;
            images.add(image);
        }
        storage.activeUndo(images);
    }

    private TransactionLog.TxnUndo capture(Session session, OpenTxn txn) {
        List<TransactionLog.TableUndo> tables = new ArrayList<>();
        for (TableStore table : txn.tables.keySet()) {
            TransactionLog.TableUndo diff = diff(session, txn, table);
            if (diff != null && !diff.empty()) {
                tables.add(diff);
            }
        }
        List<String[]> created = new ArrayList<>();
        for (TableStore table : txn.created.keySet()) {
            if (txn.dropped.containsValue(table)) {
                continue;
            }
            created.add(new String[]{table.namespace(), table.schema().name});
        }
        return new TransactionLog.TxnUndo(txn.id, created, tables);
    }

    private TransactionLog.TableUndo diff(Session session, OpenTxn txn, TableStore table) {
        TableStore.Snapshot before = txn.layout.get(table);
        if (before == null) {
            return null;
        }
        Schema afterSchema;
        RowMap afterRows;
        if (installed == session) {
            afterSchema = table.schema();
            afterRows = table.rowMap();
        } else {
            TableStore.Snapshot after = txn.workspace.get(table);
            if (after == null) {
                after = txn.held.get(table);
            }
            if (after == null) {
                return null;
            }
            afterSchema = after.schema;
            afterRows = after.rows;
        }
        if (!sameColumns(before.schema, afterSchema)) {
            throw new SeaException("checkpoint cannot store a structure change of " + table.schema().name);
        }
        if (table.structureEpoch() == 0) {
            long[] ids = table.copyInsertedIds();
            if (ids.length == 0) {
                return null;
            }
            return new TransactionLog.TableUndo(table.namespace(), table.schema().name, ids, List.of(), List.of());
        }
        TransactionLog.TableUndo previous = previousUndo(txn, table);
        long[] inserted = new long[afterRows.size()];
        int[] used = new int[1];
        List<TransactionLog.RowImage> updated = new ArrayList<>();
        List<TransactionLog.RowImage> deleted = new ArrayList<>();
        afterRows.forEach(row -> {
            StoredRow original = before.rows.get(row.id);
            if (original == null) {
                inserted[used[0]++] = row.id;
                return;
            }
            if (original == row) {
                return;
            }
            Object[] oldValues = previousValues(previous, row.id);
            if (oldValues == null) {
                oldValues = table.copyValues(before.schema, original);
            }
            Object[] newValues = table.copyValues(afterSchema, row);
            if (!Arrays.deepEquals(oldValues, newValues)) {
                updated.add(new TransactionLog.RowImage(row.id, oldValues));
            }
        });
        before.rows.forEach(row -> {
            if (afterRows.containsKey(row.id)) {
                return;
            }
            Object[] oldValues = previousValues(previous, row.id);
            if (oldValues == null) {
                oldValues = table.copyValues(before.schema, row);
            }
            deleted.add(new TransactionLog.RowImage(row.id, oldValues));
        });
        if (used[0] == 0 && updated.isEmpty() && deleted.isEmpty()) {
            return null;
        }
        long[] ids = used[0] == inserted.length ? inserted : Arrays.copyOf(inserted, used[0]);
        return new TransactionLog.TableUndo(table.namespace(), table.schema().name, ids, updated, deleted);
    }

    private static TransactionLog.TableUndo previousUndo(OpenTxn txn, TableStore table) {
        if (txn.hardenedImage == null) {
            return null;
        }
        for (TransactionLog.TableUndo undo : txn.hardenedImage.tables) {
            if (Names.same(undo.schema, table.namespace()) && Names.same(undo.table, table.schema().name)) {
                return undo;
            }
        }
        return null;
    }

    private static Object[] previousValues(TransactionLog.TableUndo previous, long id) {
        if (previous == null) {
            return null;
        }
        for (TransactionLog.RowImage row : previous.updated) {
            if (row.id == id) {
                return row.values;
            }
        }
        for (TransactionLog.RowImage row : previous.deleted) {
            if (row.id == id) {
                return row.values;
            }
        }
        return null;
    }

    private static boolean sameColumns(Schema left, Schema right) {
        if (left.columns.size() != right.columns.size()) {
            return false;
        }
        for (int i = 0; i < left.columns.size(); i++) {
            Column one = left.columns.get(i);
            Column other = right.columns.get(i);
            if (!Names.same(one.name, other.name) || one.type != other.type) {
                return false;
            }
        }
        return true;
    }

    private boolean dirtySince(Session session, OpenTxn txn) {
        for (TableStore table : txn.tables.keySet()) {
            if (changedSinceCheckpoint(session, txn, table)) {
                return true;
            }
        }
        return false;
    }

    private boolean changedSinceCheckpoint(Session session, OpenTxn txn, TableStore table) {
        if (installed == session) {
            if (table.isDirty()) {
                return true;
            }
            for (StoredRow row : table.scan()) {
                if (row.dirty) {
                    return true;
                }
            }
            return false;
        }
        TableStore.Snapshot after = txn.workspace.get(table);
        if (after == null) {
            return !txn.hardened && txn.layout.containsKey(table);
        }
        if (after.dirty) {
            return true;
        }
        for (StoredRow row : after.rows.values()) {
            if (row.dirty) {
                return true;
            }
        }
        return false;
    }

    private void applyUndo(TransactionLog.TxnUndo undo) {
        for (TransactionLog.TableUndo tableUndo : undo.tables) {
            TableStore table = tables.get(Relation.key(tableUndo.schema, tableUndo.table));
            if (table == null) {
                throw new SeaException("table not found: " + tableUndo.table);
            }
            table.reload();
            if (tableUndo.inserted.length > 0) {
                List<Long> ids = new ArrayList<>(tableUndo.inserted.length);
                for (long id : tableUndo.inserted) {
                    ids.add(id);
                }
                table.delete(ids);
            }
            Map<Long, Cell[]> replacements = new LinkedHashMap<>();
            for (TransactionLog.RowImage row : tableUndo.updated) {
                replacements.put(row.id, cellsOf(table, row.values));
            }
            if (!replacements.isEmpty()) {
                table.update(replacements);
            }
            for (TransactionLog.RowImage row : tableUndo.deleted) {
                table.restoreRow(row.id, row.values);
            }
        }
        for (String[] created : undo.created) {
            String key = Relation.key(created[0], created[1]);
            TableStore table = tables.remove(key);
            if (table == null) {
                continue;
            }
            storage.retire(table.base(), table.capacity());
            catalogDirty = true;
        }
    }

    private static Cell[] cellsOf(TableStore table, Object[] values) {
        Cell[] cells = new Cell[values.length];
        List<Column> columns = table.schema().columns;
        if (values.length != columns.size()) {
            throw new SeaException("value count does not match the columns of " + table.schema().name);
        }
        for (int i = 0; i < values.length; i++) {
            cells[i] = TableStore.toCell(columns.get(i), values[i]);
        }
        return cells;
    }

    private void discardAll() {
        if (installed != null && transactions.containsKey(installed)) {
            rollbackSession(installed);
        }
        transactions.clear();
        installed = null;
    }

    private void commitTxn(Session owner, OpenTxn txn) {
        awaitPageForce();
        Map<TableStore, TableStore.Snapshot> safety = new IdentityHashMap<>();
        for (TableStore table : txn.tables.keySet()) {
            safety.put(table, table.savepoint());
        }
        try {
            for (TableStore table : txn.tables.keySet()) {
                if (txn.dropped.containsValue(table)) {
                    continue;
                }
                TableStore.Snapshot onto = txn.held.get(table);
                if (onto == null) {
                    onto = txn.layout.get(table);
                }
                table.publishChanges(txn.layout.get(table), onto);
            }
        } catch (RuntimeException exception) {
            for (Map.Entry<TableStore, TableStore.Snapshot> entry : safety.entrySet()) {
                entry.getKey().restore(entry.getValue());
            }
            throw exception;
        }
        commitMarker = txn.id;
        txn.hardened = false;
        txn.hardenedImage = null;
        refreshActiveUndo();
        try {
            if (!suppressCapture && !storage.leaveForRecovery()) {
                // The sync thread publishes its checkpoint before the commit record,
                // so a crash cannot replay rows that are already in the pages.
                awaitPageForce();
                spillPending(owner);
                storage.logCommit(txn.id);
                commitMarker = null;
                for (TableStore table : txn.dropped.values()) {
                    discardTable(table);
                }
                // The commit record is already durable. Writing the pages and a
                // checkpoint puts those rows in the database file before close.
                flush();
                if (storage.logLength() > storage.checkpointLsn()) {
                    storage.writeCheckpoint();
                }
            } else {
                pending.clear();
                for (TableStore table : txn.dropped.values()) {
                    discardTable(table);
                }
            }
        } catch (RuntimeException exception) {
            transactions.clear();
            installed = null;
            inTransaction = false;
            transactionSession = null;
            transactionId = null;
            try {
                reloadAll();
            } catch (RuntimeException ignored) {
                // the flush failure already explains the error
            }
            throw exception;
        } finally {
            commitMarker = null;
        }
        releaseLocks(owner);
        renameUndo.clear();
        transactions.remove(owner);
        installed = null;
        inTransaction = false;
        transactionSession = null;
        transactionId = null;
    }

    private OpenTxn currentTxn() {
        Session owner = transactionSession != null ? transactionSession : actor;
        if (owner == null || !inTransaction) {
            return null;
        }
        return transactions.get(owner);
    }

    private void undoCatalog(OpenTxn txn) {
        undoCatalogBeyond(txn, null);
    }

    /** Drops tables created after {@code covered}, and puts back tables dropped after it. */
    private void undoCatalogBeyond(OpenTxn txn, TransactionLog.TxnUndo covered) {
        for (Map.Entry<String, TableStore> entry : txn.dropped.entrySet()) {
            if (!txn.created.containsKey(entry.getValue()) && !tables.containsKey(entry.getKey())) {
                tables.put(entry.getKey(), entry.getValue());
            }
        }
        for (TableStore table : List.copyOf(txn.created.keySet())) {
            if (covered != null && catalogCovers(covered, table)) {
                continue;
            }
            if (tables.containsKey(Relation.key(table.namespace(), table.schema().name))) {
                discardTable(table);
            }
        }
    }

    private static boolean catalogCovers(TransactionLog.TxnUndo undo, TableStore table) {
        for (String[] created : undo.created) {
            if (Names.same(created[0], table.namespace()) && Names.same(created[1], table.schema().name)) {
                return true;
            }
        }
        return false;
    }

    private void discardTable(TableStore table) {
        String key = Relation.key(table.namespace(), table.schema().name);
        boolean present = tables.containsKey(key);
        tables.remove(key);
        try {
            publishLayout(table.base(), table.capacity());
        } catch (RuntimeException exception) {
            if (present) {
                tables.put(key, table);
            }
            throw exception;
        }
        table.close();
    }

    private static final class OpenTxn {
        private final long id;
        private final IdentityHashMap<TableStore, TableStore.Snapshot> layout = new IdentityHashMap<>();
        private final IdentityHashMap<TableStore, TableStore.Snapshot> held = new IdentityHashMap<>();
        private final IdentityHashMap<TableStore, TableStore.Snapshot> workspace = new IdentityHashMap<>();
        private final IdentityHashMap<TableStore, Boolean> tables = new IdentityHashMap<>();
        private final IdentityHashMap<TableStore, Boolean> created = new IdentityHashMap<>();
        private final LinkedHashMap<String, TableStore> dropped = new LinkedHashMap<>();
        private final List<SaveMark> savepoints = new ArrayList<>();
        private final IdentityHashMap<TableStore, TableStore.Snapshot> readPin = new IdentityHashMap<>();

        private boolean hardened;
        private TransactionLog.TxnUndo hardenedImage;

        private OpenTxn(long id) {
            this.id = id;
        }
    }

    private static final class SaveMark {
        private final String name;
        private final IdentityHashMap<TableStore, TableStore.Snapshot> rows = new IdentityHashMap<>();
        private final IdentityHashMap<TableStore, Boolean> created = new IdentityHashMap<>();
        private final LinkedHashMap<String, TableStore> dropped = new LinkedHashMap<>();
        private final int pendingSize;

        private SaveMark(String name, int pendingSize) {
            this.name = name;
            this.pendingSize = pendingSize;
        }
    }

    private static final class OpenCursor {
        private final CursorDef definition;
        private List<Row> rows = List.of();
        private int position;
        private boolean open;

        private OpenCursor(CursorDef definition) {
            this.definition = definition;
        }
    }
}
