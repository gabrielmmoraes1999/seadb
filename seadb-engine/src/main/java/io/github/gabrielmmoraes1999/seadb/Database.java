package io.github.gabrielmmoraes1999.seadb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.IntConsumer;
import java.util.function.Consumer;

/**
 * SeaDB. The database is a {@code .db} file: tables, BLOBs,
 * configuration, users, and connections live in it. The {@code .log} beside it
 * stores the committed transactions since creation or the last full backup.
 * If that file does not exist, or is empty, opening creates a log with only the header
 * and advances the generation: the old file can no longer open the database.
 * Starting writes a checkpoint into the database and into the log. Stopping writes it again.
 * A checkpoint may also store pages of a transaction that is still open. That does not commit it.
 * Normal opening refuses pending recovery; explicit log application undoes those transactions.
 * SQL waits for that checkpoint, two open transactions cannot
 * change the same table, and a large before-image is stored whole in the log. The log is not
 * cut until a full backup. Applying a log records only the checkpoint already stored
 * in that file. A rejected log leaves the database checkpoint unchanged.
 *
 * <p>Opening requires a user and password and a file that already exists.
 * {@link #create(Path, DatabaseOptions)} chooses the page size, the password, and the name.
 * The password is stored with bcrypt.
 *
 * <pre>{@code
 * try (Database db = Database.open(Path.of("store.db"), "dba", "dba")) {
 *     db.execute("""
 *             CREATE TABLE customer (
 *                 id INTEGER PRIMARY KEY AUTOINCREMENT,
 *                 name VARCHAR(120) NOT NULL,
 *                 photo BLOB
 *             )
 *             """);
 *     db.execute("INSERT INTO customer (name, photo) VALUES (?, ?)", "Ann", photo);
 * }
 * }</pre>
 *
 * <p>In DBeaver, the driver is {@link io.github.gabrielmmoraes1999.seadb.jdbc.SeaDriver} and the URL
 * is {@code jdbc:seadb:embedded:<file.db>} or {@code jdbc:seadb://host:port/name}.
 */
public final class Database implements AutoCloseable {

    private final Engine engine;
    private Session owner;

    private Database(Engine engine, Session owner) {
        this.engine = engine;
        this.owner = owner;
    }

    /**
     * Creates a new file. The page size is 4096, 8192, 16384, or 32768 and does not change afterward.
     * A file that already has content is not reused.
     */
    public static void create(Path file, DatabaseOptions options) {
        if (file == null) {
            throw new SeaException("database file is required");
        }
        if (options == null) {
            throw new SeaException("database options are required");
        }
        Engine.create(file, options);
    }

    /**
     * Full backup: copies the {@code .db} and the {@code .log} into the directory
     * and makes the live log record only what changes afterward.
     */
    public static void backup(Path database, Path directory) {
        if (database == null) {
            throw new SeaException("database file is required");
        }
        if (directory == null) {
            throw new SeaException("backup directory is required");
        }
        try (DatabaseFile file = DatabaseFile.open(database)) {
            file.backupTo(directory);
        }
    }

    /**
     * Modification backup: copies the {@code .log} into the directory.
     * The live log stays, with the transactions since creation or the last full backup.
     */
    public static void backupLog(Path database, Path directory) {
        if (database == null) {
            throw new SeaException("database file is required");
        }
        if (directory == null) {
            throw new SeaException("backup directory is required");
        }
        try (DatabaseFile file = DatabaseFile.open(database)) {
            file.backupLogTo(directory);
        }
    }

    /**
     * Applies a log onto the {@code .db}. The same generation advances the database
     * from the checkpoint. The next generation brings what changed after the backup.
     */
    public static void applyLog(Path database, Path log) {
        if (database == null) {
            throw new SeaException("database file is required");
        }
        if (log == null) {
            throw new SeaException("transaction log is required");
        }
        applyLog(database, log, null);
    }

    public static void applyLog(Path database, Path log, Cache cache) {
        applyLog(database, log, cache, null);
    }

    /**
     * Applies a log onto the {@code .db}. {@code applied} receives the percent complete,
     * from 1 to 100, when that percent changes. It is not called when the log is rejected.
     */
    public static void applyLog(Path database, Path log, Cache cache, IntConsumer applied) {
        applyLog(database, log, cache, applied, null);
    }

    /**
     * Also reports recovery stages and checkpoint positions, including recovery performed
     * before SQL replay. Messages about reading a checkpoint do not imply a durable apply;
     * the final checkpoint message is sent only after successful application and close.
     */
    public static void applyLog(Path database, Path log, Cache cache, IntConsumer applied,
                                Consumer<String> messages) {
        applyLog(database, log, cache, applied, messages, null);
    }

    /**
     * Applies commits at or before {@code until}. A commit record without a time is applied.
     * A later commit is left out, and pages already stored for it are undone.
     */
    public static void applyLog(Path database, Path log, Cache cache, IntConsumer applied,
                                Consumer<String> messages, java.time.Instant until) {
        if (database == null) {
            throw new SeaException("database file is required");
        }
        if (log == null) {
            throw new SeaException("transaction log is required");
        }
        ReplayCut.arm(until);
        try {
            if (messages != null) {
                messages.accept("Starting database \"" + databaseName(database) + "\" ("
                        + database.toAbsolutePath().normalize() + ") at " + recoveryWhen());
            }
            try (Engine engine = Engine.openApplying(database, cache, messages)) {
                engine.applyExternalLog(log, applied);
            }
            if (messages != null) {
                messages.accept("Recovery complete");
            }
        } finally {
            ReplayCut.clear();
        }
    }

    /** Clock shown while a log is applied. The minute is enough, as on the server console. */
    static String recoveryWhen() {
        return DateTimeFormatter.ofPattern("EEE MMM dd yyyy HH:mm", Locale.ENGLISH).format(LocalDateTime.now());
    }

    static String databaseName(Path file) {
        Path name = file == null ? null : file.getFileName();
        String text = name == null ? "database" : name.toString();
        int dot = text.lastIndexOf('.');
        return dot > 0 ? text.substring(0, dot) : text;
    }

    /**
     * Reads the compressed transactions from the {@code .log} and writes the SQL to {@code destination}.
     * {@code source} may be the {@code .log} or the {@code .db} beside it.
     * Parameters are written into the text in place of each {@code ?}.
     * A query is omitted. The script uses {@code BEGIN TRANSACTION}, {@code COMMIT WORK}, and {@code go}.
     */
    public static void exportSql(Path source, Path destination) {
        exportSql(source, destination, null);
    }

    /**
     * Reads the compressed transactions from the {@code .log} and writes the SQL to {@code destination}.
     * {@code progress} receives 0 at the start, advances as each statement is written, and receives 100
     * when the file has been written.
     */
    public static void exportSql(Path source, Path destination, IntConsumer progress) {
        if (source == null) {
            throw new SeaException("transaction log is required");
        }
        if (destination == null) {
            throw new SeaException("the .sql file is required");
        }
        Path log = logOf(source);
        Path target = destination.toAbsolutePath().normalize();
        if (target.equals(log)) {
            throw new SeaException("the SQL destination is a different file");
        }
        if (Files.exists(target) && Files.isDirectory(target)) {
            throw new SeaException("the SQL destination is a file");
        }
        try {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            int[] last = {-1};
            IntConsumer mark = percent -> {
                int shown = Math.max(0, Math.min(99, percent));
                if (shown == last[0]) {
                    return;
                }
                last[0] = shown;
                report(progress, shown);
            };
            try (TransactionLog transactionLog = TransactionLog.inspect(log);
                 BufferedWriter writer = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
                long total = Math.max(1L, transactionLog.size());
                mark.accept(0);
                SqlScript script = new SqlScript(writer);
                transactionLog.forEachTransaction(Format.LOG_HEADER_SIZE, at ->
                        mark.accept((int) Math.min(99L, at * 99L / total)), (batch, start) -> {
                    int count = batch.statements.size();
                    script.write(batch, start, index -> {
                        long span = batch.end - start;
                        long pos = count == 0 ? batch.end : start + span * index / count;
                        mark.accept((int) Math.min(99L, pos * 99L / total));
                    });
                }, script::checkpoint);
                script.finish();
            }
            report(progress, 100);
        } catch (IOException exception) {
            throw new SeaException("could not write the SQL: " + exception.getMessage(), exception);
        }
    }

    private static void report(IntConsumer progress, int percent) {
        if (progress != null) {
            progress.accept(percent);
        }
    }

    private static Path logOf(Path source) {
        Path path = source.toAbsolutePath().normalize();
        Path name = path.getFileName();
        String text = name == null ? "" : name.toString().toLowerCase(Locale.ROOT);
        if (text.endsWith(".db")) {
            return TransactionLog.beside(path);
        }
        if (text.endsWith(".log")) {
            return path;
        }
        throw new SeaException("provide a .log file or the .db beside it");
    }

    /** Writes the SQL of each commit as it is read, in the shape of a SQL Anywhere script. */
    private static final class SqlScript {
        private final Writer out;
        private String connectKey;
        private boolean started;

        private SqlScript(Writer out) {
            this.out = out;
        }

        private void checkpoint(long offset) throws IOException {
            line("--CHECKPOINT-0000-" + offset(offset));
        }

        private void write(TransactionLog.Batch batch, long start, IntConsumer written) throws IOException {
            List<TransactionLog.Applied> kept = new ArrayList<>();
            int index = 0;
            for (TransactionLog.Applied statement : batch.statements) {
                index++;
                TransactionLog.Applied logged = logged(statement);
                if (logged != null) {
                    kept.add(logged);
                }
                if (written != null) {
                    written.accept(index);
                }
            }
            if (kept.isEmpty()) {
                return;
            }
            TransactionLog.Applied first = kept.get(0);
            String conn = connection(first.connection);
            if (first.user != null) {
                String key = conn + "\0" + first.user;
                if (!key.equals(connectKey)) {
                    connectKey = key;
                    StringBuilder connect = new StringBuilder("--CONNECT-").append(conn).append('-')
                            .append(offset(start)).append('-').append(first.user);
                    if (batch.commitMillis != null) {
                        connect.append('-').append(minute(batch.commitMillis));
                    }
                    line(connect.toString());
                }
            }
            line("--BEGIN TRANSACTION-" + conn + "-" + offset(start));
            line("BEGIN TRANSACTION");
            line("go");
            for (TransactionLog.Applied statement : kept) {
                line("--" + operation(statement.sql) + "-" + conn + "-" + offset(start));
                line(bind(statement.sql, statement.params));
                line("go");
            }
            long commitAt = batch.commitAt > 0 ? batch.commitAt : batch.end;
            StringBuilder commit = new StringBuilder("--COMMIT-").append(conn).append('-').append(offset(commitAt));
            if (batch.commitMillis != null) {
                commit.append('-').append(stamp(batch.commitMillis));
            }
            line(commit.toString());
            line("COMMIT WORK");
            line("go");
        }

        private void line(String text) throws IOException {
            out.write(text);
            out.write('\n');
            started = true;
        }

        private void finish() throws IOException {
            if (!started) {
                out.write('\n');
            }
        }
    }

    private static TransactionLog.Applied logged(TransactionLog.Applied statement) {
        if (statement == null || statement.sql == null || statement.sql.isBlank() || control(statement.sql)) {
            return null;
        }
        SqlParser.DurableSql durable = SqlParser.durable(statement.sql);
        if (durable == null) {
            return null;
        }
        Object[] params = statement.params == null ? new Object[0] : statement.params;
        if (params.length > durable.parameters()) {
            params = Arrays.copyOf(params, durable.parameters());
        }
        return new TransactionLog.Applied(statement.schema, durable.sql(), params, statement.connection, statement.user);
    }

    private static boolean control(String sql) {
        String word = operation(sql);
        return word.equals("BEGIN") || word.equals("COMMIT") || word.equals("ROLLBACK") || word.equals("START");
    }

    private static String operation(String sql) {
        int index = 0;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (Character.isWhitespace(current)) {
                index++;
                continue;
            }
            if (current == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
                int end = sql.indexOf('\n', index);
                index = end < 0 ? sql.length() : end + 1;
                continue;
            }
            if (current == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
                int end = sql.indexOf("*/", index + 2);
                index = end < 0 ? sql.length() : end + 2;
                continue;
            }
            break;
        }
        int start = index;
        while (index < sql.length() && Character.isLetter(sql.charAt(index))) {
            index++;
        }
        if (start == index) {
            return "SQL";
        }
        return sql.substring(start, index).toUpperCase(Locale.ROOT);
    }

    private static String offset(long value) {
        if (value < 0) {
            value = 0;
        }
        return String.format("%011d", value);
    }

    private static String connection(long id) {
        if (id < 0) {
            id = 0;
        }
        if (id <= 9999) {
            return String.format("%04d", id);
        }
        return Long.toString(id);
    }

    private static String stamp(long millis) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        return COMMIT_STAMP.format(time) + "000";
    }

    private static String minute(long millis) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        return CONNECT_STAMP.format(time);
    }

    private static final DateTimeFormatter COMMIT_STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter CONNECT_STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static String bind(String sql, Object[] params) {
        StringBuilder out = new StringBuilder();
        int index = 0;
        boolean quote = false;
        for (int i = 0; i < sql.length(); i++) {
            char current = sql.charAt(i);
            if (current == '\'') {
                out.append(current);
                if (quote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    out.append('\'');
                    i++;
                    continue;
                }
                quote = !quote;
                continue;
            }
            if (current == '?' && !quote) {
                if (params != null && index < params.length) {
                    out.append(literal(params[index]));
                } else {
                    out.append('?');
                }
                index++;
                continue;
            }
            out.append(current);
        }
        String text = out.toString().strip();
        while (text.endsWith(";")) {
            text = text.substring(0, text.length() - 1).stripTrailing();
        }
        return text;
    }

    private static String literal(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean bool) {
            return bool ? "TRUE" : "FALSE";
        }
        if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
            return value.toString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Float || value instanceof Double) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            StringBuilder hex = new StringBuilder("X'");
            for (byte piece : bytes) {
                hex.append(String.format("%02X", piece & 0xFF));
            }
            return hex.append('\'').toString();
        }
        if (value instanceof LocalDate date) {
            return "'" + date + "'";
        }
        if (value instanceof LocalTime time) {
            return "'" + time + "'";
        }
        if (value instanceof LocalDateTime time) {
            return "'" + time.toString().replace('T', ' ') + "'";
        }
        if (value instanceof java.sql.Date date) {
            return "'" + date.toLocalDate() + "'";
        }
        if (value instanceof java.sql.Time time) {
            return "'" + time.toLocalTime() + "'";
        }
        if (value instanceof java.sql.Timestamp time) {
            return "'" + time.toLocalDateTime().toString().replace('T', ' ') + "'";
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }

    public static Database open(Path file, String user, String password) {
        return open(file, user, password, (String) null, null);
    }

    /**
     * Opens the file with a role. {@code null}, empty, or {@code NONE} sign in without a role.
     * The user must have been granted the role, except the DBA.
     */
    public static Database open(Path file, String user, String password, String role) {
        return open(file, user, password, role, null);
    }

    public static Database open(Path file, String user, String password, Cache cache) {
        return open(file, user, password, null, cache);
    }

    public static Database open(Path file, String user, String password, String role, Cache cache) {
        if (user == null || user.isBlank() || password == null || password.isEmpty()) {
            throw new SeaException("user and password are required");
        }
        if (file == null) {
            throw new SeaException("database file is required");
        }
        Engine engine = Engine.open(file, cache);
        try {
            return new Database(engine, engine.login(user, password, role));
        } catch (RuntimeException exception) {
            engine.close();
            throw exception;
        }
    }

    public static Database open(String file, String user, String password) {
        if (file == null || file.isBlank()) {
            throw new SeaException("database file is required");
        }
        return open(Path.of(file), user, password);
    }

    /**
     * Opens the file without authenticating. The network server holds the {@code .db} this way
     * and calls {@link #login} for each client. The file must already exist.
     */
    public static Database attach(Path file) {
        return attach(file, null);
    }

    public static Database attach(Path file, Cache cache) {
        if (file == null) {
            throw new SeaException("database file is required");
        }
        return new Database(Engine.open(file, cache), null);
    }

    /**
     * Runs one or more statements separated by {@code ';'}.
     * The return value is the result of the last statement.
     * {@code ?} placeholders are positional and apply to the whole script.
     */
    public SqlResult execute(String sql, Object... params) {
        synchronized (engine) {
            if (owner == null || owner.closed()) {
                throw new SeaException("session closed");
            }
            return engine.execute(owner, sql, params);
        }
    }

    public SqlResult execute(Session session, String sql, Object... params) {
        synchronized (engine) {
            return engine.execute(session, sql, params);
        }
    }

    public SqlResult execute(Session session, String sql, Object[] params, int fetch) {
        return execute(session, sql, params, fetch, 0);
    }

    public SqlResult execute(Session session, String sql, Object[] params, int fetch, int timeoutSeconds) {
        synchronized (engine) {
            return engine.execute(session, sql, params, fetch, timeoutSeconds);
        }
    }

    public void cancel(Session session) {
        engine.cancel(session);
    }

    public SqlResult fetch(Session session, long cursor, int count) {
        synchronized (engine) {
            return engine.fetchCursor(session, cursor, count);
        }
    }

    public Session login(String user, String password) {
        return login(user, password, null, null);
    }

    /**
     * Signs in with a role already granted. The DBA may use any existing role.
     * The recorded origin is this process.
     */
    public Session login(String user, String password, String role) {
        return login(user, password, role, null);
    }

    /**
     * Signs in and records the remote origin in {@code SEA$CONNECTIONS}.
     * A null {@code remote} uses the local process.
     */
    public Session login(String user, String password, String role, RemoteClient remote) {
        synchronized (engine) {
            return engine.login(user, password, role, remote);
        }
    }

    public void logout(Session session) {
        synchronized (engine) {
            engine.detach(session);
            if (owner == session) {
                owner = null;
            }
        }
    }

    public Session session() {
        return owner;
    }

    public int executeUpdate(String sql, Object... params) {
        SqlResult result = execute(sql, params);
        if (result.isQuery()) {
            throw new SeaException("the statement returned rows; use query or execute");
        }
        return result.affectedRows();
    }

    /**
     * Types of the {@code ?} placeholders. Nothing is executed and nothing is written.
     */
    public List<ParameterMeta> describeParameters(String sql) {
        synchronized (engine) {
            if (owner == null || owner.closed()) {
                throw new SeaException("session closed");
            }
            return engine.describeParameters(owner, sql);
        }
    }

    public List<ParameterMeta> describeParameters(Session session, String sql) {
        synchronized (engine) {
            return engine.describeParameters(session, sql);
        }
    }

    public SqlResult query(String sql, Object... params) {
        SqlResult result = execute(sql, params);
        if (!result.isQuery()) {
            throw new SeaException("the statement is not a query");
        }
        return result;
    }

    public List<String> tables() {
        synchronized (engine) {
            return engine.tableNames();
        }
    }

    /**
     * Schemas visible to this connection, in name order. Every database has {@code public}.
     */
    public List<String> schemas() {
        synchronized (engine) {
            return engine.schemaNames(owner);
        }
    }

    public List<String> schemas(Session session) {
        synchronized (engine) {
            return engine.schemaNames(session);
        }
    }

    /**
     * Sets the session schema. An unquoted name is case-insensitive.
     * The default, on connect, is {@code public}.
     */
    public void setSchema(Session session, String schema) {
        synchronized (engine) {
            engine.useSchema(session, schema);
        }
    }

    /**
     * Describes the user tables, in name order, with columns and indexes.
     * {@code SEA$} tables are omitted.
     */
    public List<TableDescription> describeTables() {
        synchronized (engine) {
            return engine.describeTables(owner);
        }
    }

    public List<TableDescription> describeTables(Session session) {
        synchronized (engine) {
            return engine.describeTables(session);
        }
    }

    public List<TableDescription> describeAllTables() {
        synchronized (engine) {
            return engine.describeAllTables(owner);
        }
    }

    public List<TableDescription> describeAllTables(Session session) {
        synchronized (engine) {
            return engine.describeAllTables(session);
        }
    }

    /**
     * Describes the views visible to this connection, in name order.
     * Each one is read-only and has no index.
     */
    public List<TableDescription> describeViews() {
        synchronized (engine) {
            return engine.describeViews(owner);
        }
    }

    public List<TableDescription> describeViews(Session session) {
        synchronized (engine) {
            return engine.describeViews(session);
        }
    }

    /**
     * Checkpoints the open database and returns the files a backup will copy.
     * Only a DBA may call this. Other sessions may continue writing while the
     * captured pages and log prefix are read. A full backup passes false.
     */
    public List<BackupPiece> beginBackup(Session session, boolean incremental) {
        synchronized (engine) {
            requireDba(session);
            return engine.beginBackup(incremental);
        }
    }

    /** Reads one slice of a file returned by {@link #beginBackup}. */
    public byte[] readBackup(Session session, int index, long offset, int length) {
        synchronized (engine) {
            requireDba(session);
            return engine.readBackup(index, offset, length);
        }
    }

    /**
     * Ends a backup. An unchanged full backup advances the live log generation;
     * concurrent changes or open transactions keep the log for recovery.
     */
    public void finishBackup(Session session) {
        synchronized (engine) {
            requireDba(session);
            engine.finishBackup();
        }
    }

    /** Drops a backup that will not be kept. The live log stays as it is. */
    public void abortBackup(Session session) {
        synchronized (engine) {
            engine.abortBackup();
        }
    }

    private static void requireDba(Session session) {
        if (session == null || session.closed()) {
            throw new SeaException("session closed");
        }
        if (!session.admin()) {
            throw new SeaException("only the DBA can backup the database");
        }
    }

    public Path file() {
        return engine.file;
    }

    /**
     * Page size, in bytes. It is the value stored when the file was created.
     */
    public int pageSize() {
        return engine.pageSize();
    }

    /** Bytes the page cache may occupy after it was rounded up to whole pages. */
    public long cacheMaximum() {
        return engine.cacheMaximum();
    }

    public boolean transactionActive() {
        synchronized (engine) {
            return engine.transactionActive();
        }
    }

    /**
     * The session has {@code BEGIN} without {@code COMMIT} or {@code ROLLBACK}.
     * Another session may have its own transaction at the same time.
     */
    public boolean transactionActive(Session session) {
        synchronized (engine) {
            return engine.transactionActive(session);
        }
    }

    long checkpointLsn() {
        synchronized (engine) {
            return engine.checkpointLsn();
        }
    }

    long logLength() {
        synchronized (engine) {
            return engine.logLength();
        }
    }

    /**
     * Writes dirty pages, including those of transactions that are still open.
     * The open transactions stay open. {@code COMMIT} is a separate operation.
     */
    public void checkpoint() {
        synchronized (engine) {
            if (owner == null || owner.closed()) {
                throw new SeaException("session closed");
            }
            engine.checkpoint();
        }
    }

    /** Replaces the thresholds of the automatic checkpoint. */
    public void checkpointPolicy(CheckpointPolicy policy) {
        synchronized (engine) {
            engine.checkpointPolicy(policy);
        }
    }

    /** Drops the process without rolling back and without a checkpoint. */
    void crash() {
        synchronized (engine) {
            owner = null;
            engine.crash();
        }
    }

    @Override
    public void close() {
        synchronized (engine) {
            try {
                if (owner != null && !owner.closed()) {
                    engine.discardTransaction();
                    engine.detach(owner);
                }
            } finally {
                owner = null;
                engine.close();
            }
        }
    }
}
