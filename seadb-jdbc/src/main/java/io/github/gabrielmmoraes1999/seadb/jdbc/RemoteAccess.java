package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.SeaException;
import io.github.gabrielmmoraes1999.seadb.RemoteClient;
import io.github.gabrielmmoraes1999.seadb.SqlResult;
import io.github.gabrielmmoraes1999.seadb.TableDescription;
import io.github.gabrielmmoraes1999.seadb.net.Wire;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.List;

final class RemoteAccess implements DatabaseAccess {

    private final Object lock = new Object();
    private final Socket socket;
    private final DataInputStream input;
    private final DataOutputStream output;
    private String user;
    private String schema;
    private boolean transactionActive;
    private boolean ownsTransaction;
    private boolean closed;

    private RemoteAccess(Socket socket, DataInputStream input, DataOutputStream output, Wire.SessionView session) {
        this.socket = socket;
        this.input = input;
        this.output = output;
        this.user = session.user();
        this.schema = session.schema();
        this.transactionActive = session.transactionActive();
        this.ownsTransaction = session.ownsTransaction();
    }

    static RemoteAccess connect(String host, int port, String database, String user, String password, String role)
            throws SQLException {
        return connect(host, port, database, user, password, role, false, "");
    }

    static RemoteAccess connect(String host, int port, String database, String user, String password, String role,
                                boolean ssl, String certificate) throws SQLException {
        Socket socket = ssl ? secure(certificate) : new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), 10_000);
            socket.setTcpNoDelay(true);
            DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            RemoteClient local = RemoteClient.local();
            Wire.writeFrame(output, Wire.LOGIN, Wire.encodeLogin(
                    database, user, password, role, local.host(), local.osUser(), local.process()));
            Wire.Frame response = Wire.readFrame(input);
            if (response.opcode() == Wire.FAIL) {
                throw new SQLException(Wire.decodeError(response.body()), Wire.decodeErrorState(response.body()));
            }
            if (response.opcode() != Wire.OK) {
                throw new SQLException("invalid response from the server", "08001");
            }
            return new RemoteAccess(socket, input, output, Wire.decodeLoginOk(response.body()));
        } catch (SQLException exception) {
            closeQuietly(socket);
            throw exception;
        } catch (IOException exception) {
            closeQuietly(socket);
            throw new SQLException("could not connect to " + host + ":" + port, "08001", exception);
        }
    }

    @Override
    public Object lock() {
        return lock;
    }

    @Override
    public String user() {
        return user;
    }

    @Override
    public String schema() {
        return schema;
    }

    @Override
    public void refresh() throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                Wire.writeFrame(output, Wire.STATUS, new byte[0]);
                apply(readState());
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    @Override
    public boolean transactionActive() {
        return transactionActive;
    }

    @Override
    public boolean ownsTransaction() {
        return ownsTransaction;
    }

    @Override
    public List<io.github.gabrielmmoraes1999.seadb.ParameterMeta> describeParameters(String sql) throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                Wire.writeFrame(output, Wire.DESCRIBE_PARAMETERS, Wire.encodeDescribe(sql));
                return Wire.decodeParameters(read().body());
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    public SqlResult execute(String sql, Object[] params) throws SQLException {
        return execute(sql, params, 0);
    }

    private volatile boolean busy;

    public SqlResult execute(String sql, Object[] params, int fetch) throws SQLException {
        return execute(sql, params, fetch, 0);
    }

    public SqlResult execute(String sql, Object[] params, int fetch, int timeoutSeconds) throws SQLException {
        synchronized (lock) {
            ensureOpen();
            busy = true;
            try {
                Wire.writeFrame(output, Wire.EXECUTE, Wire.encodeExecute(sql, params, fetch, timeoutSeconds));
                Wire.Frame response = read();
                Wire.ExecuteReply reply = Wire.decodeExecuteOk(response.body());
                schema = reply.schema();
                transactionActive = reply.transactionActive();
                ownsTransaction = reply.ownsTransaction();
                return reply.result();
            } catch (SeaException exception) {
                throw Jdbc.wrap(exception);
            } catch (IOException exception) {
                throw failed(exception);
            } finally {
                busy = false;
            }
        }
    }

    public void cancel() throws SQLException {
        ensureOpen();
        try {
            Wire.writeFrame(output, Wire.CANCEL, new byte[0]);
            if (!busy) {
                Wire.Frame response = Wire.readFrame(input);
                if (response.opcode() == Wire.FAIL) {
                    return;
                }
            }
        } catch (IOException exception) {
            throw failed(exception);
        }
    }

    public SqlResult fetch(long cursor, int count) throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                Wire.writeFrame(output, Wire.FETCH, Wire.encodeFetch(cursor, count));
                return Wire.decodeExecuteOk(read().body()).result();
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                String name = schema == null || schema.isBlank() ? "public" : schema;
                Wire.writeFrame(output, Wire.SET_SCHEMA, Wire.encodeSchema(name));
                apply(readState());
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    @Override
    public void release() throws SQLException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                Wire.writeFrame(output, Wire.LOGOUT, new byte[0]);
                read();
            } catch (IOException ignored) {
                // the server may already be down
            } finally {
                closeQuietly(socket);
            }
        }
    }

    @Override
    public List<String> schemas() throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                Wire.writeFrame(output, Wire.SCHEMAS, new byte[0]);
                return Wire.decodeNames(read().body());
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    @Override
    public List<TableDescription> describeAllTables() throws SQLException {
        return describe(Wire.DESCRIBE_TABLES);
    }

    @Override
    public List<TableDescription> describeViews() throws SQLException {
        return describe(Wire.DESCRIBE_VIEWS);
    }

    private List<TableDescription> describe(byte opcode) throws SQLException {
        synchronized (lock) {
            ensureOpen();
            try {
                Wire.writeFrame(output, opcode, new byte[0]);
                return Wire.decodeTables(read().body());
            } catch (IOException exception) {
                throw failed(exception);
            }
        }
    }

    private Wire.Frame read() throws IOException, SQLException {
        Wire.Frame response = Wire.readFrame(input);
        if (response.opcode() == Wire.FAIL) {
            throw new SQLException(Wire.decodeError(response.body()), Wire.decodeErrorState(response.body()));
        }
        if (response.opcode() != Wire.OK) {
            throw new SQLException("invalid response from the server", "08001");
        }
        return response;
    }

    private Wire.StateView readState() throws IOException, SQLException {
        return Wire.decodeState(read().body());
    }

    private void apply(Wire.StateView state) {
        schema = state.schema();
        transactionActive = state.transactionActive();
        ownsTransaction = state.ownsTransaction();
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("the connection is closed", "08003");
        }
    }

    private static SQLException failed(IOException exception) {
        return new SQLException("connection to the server failed", "08001", exception);
    }

    private static Socket secure(String certificate) throws SQLException {
        try {
            java.security.cert.CertificateFactory factory = java.security.cert.CertificateFactory.getInstance("X.509");
            java.security.cert.Certificate cert;
            try (java.io.InputStream input = java.nio.file.Files.newInputStream(java.nio.file.Path.of(certificate))) {
                cert = factory.generateCertificate(input);
            }
            java.security.KeyStore trust = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
            trust.load(null, null);
            trust.setCertificateEntry("server", cert);
            javax.net.ssl.TrustManagerFactory managers = javax.net.ssl.TrustManagerFactory.getInstance(
                    javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance("TLS");
            context.init(null, managers.getTrustManagers(), null);
            javax.net.ssl.SSLSocket created = (javax.net.ssl.SSLSocket) context.getSocketFactory().createSocket();
            javax.net.ssl.SSLParameters parameters = created.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm(null);
            created.setSSLParameters(parameters);
            return created;
        } catch (Exception exception) {
            throw new SQLException("could not trust " + certificate, "08001", exception);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // the connection is already down
        }
    }
}
