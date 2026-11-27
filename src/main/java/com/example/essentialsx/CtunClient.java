package com.example.essentialsx;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * ctun java client — single file, no build step, no dependencies.
 *
 *   javac CtunClient.java && java CtunClient --server tunnel.example.com --token ct_xxx \
 *        --route 127.0.0.1:8080 --route web:127.0.0.1:3000 --conns 4
 *
 *   or without compiling:  java CtunClient.java --server ... --token ... --route ...
 *
 * Transport is plain ws:// always — no TLS anywhere in this client.  If you need
 * wss, terminate TLS in front of the server (nginx/caddy) and point --port at it.
 *
 * Embedding in another program (same package):
 *
 *   CtunTunnel t = new CtunTunnel("1.2.3.4", 65000, "ct_xxx");
 *   t.route("127.0.0.1:8080");
 *   t.start();
 *   ...
 *   t.close();
 *
 * Requires JDK 11+ (single-file source launch). Tested on 21.
 */
public final class CtunClient {

    static final String VERSION = "ctun-java";
    static final int F_OPEN = 1, F_DATA = 2, F_END = 3, F_CLOSE = 4, F_HEAD = 5, F_PING = 6;
    static final int OP_TEXT = 0x1, OP_BIN = 0x2, OP_CLOSE = 0x8, OP_PING = 0x9, OP_PONG = 0xA;
    static final int CHUNK = 65536;

    // Heartbeat.  The pinger sends an app-level ping every 20s; the server
    // answers with one of its own every 25s.  A tunnel socket that reads
    // nothing for SO_TIMEOUT is dead (half-open socket, NAT rebind, killed
    // peer) even though the kernel still accepts writes into its buffer:
    // recv() throws SocketTimeoutException and run() reconnects.
    static final int PING_EVERY_MS = 20000;
    static final int SO_TIMEOUT = 90000;

    static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Object LOG_LOCK = new Object();

    /**
     * Console output switch.  When this client is embedded in a game server jar
     * the panel console is often visible to other people, so nothing is printed
     * unless the host explicitly turns it on (ProxyService.TUNNEL_VERBOSE).
     */
    static boolean VERBOSE = false;

    static void log(String fmt, Object... a) {
        if (!VERBOSE) return;
        synchronized (LOG_LOCK) {
            System.out.print(LocalDateTime.now().format(TS) + " " + String.format(fmt, a) + "\n");
            System.out.flush();
        }
    }

    // ------------------------------------------------------------------ ws

    /** Minimal RFC6455 client. Server frames are unmasked, ours are masked. */
    static final class Ws implements AutoCloseable {
        final Socket sock;
        final InputStream in;
        final OutputStream out;
        final Object writeLock = new Object();
        volatile boolean closed;

        Ws(Socket sock, InputStream in, OutputStream out) {
            this.sock = sock;
            this.in = in;
            this.out = out;
        }

        void send(int op, byte[] payload) throws IOException {
            if (payload == null) payload = new byte[0];
            // Never drop a frame silently: the caller decides what to do when the
            // tunnel is gone.  Swallowing it here is what produced responses that
            // reached the panel with an END but no HEAD.
            if (closed) throw new IOException("tunnel socket closed");
            int n = payload.length;
            ByteArrayOutputStream b = new ByteArrayOutputStream(n + 14);
            b.write(0x80 | op);
            if (n < 126) {
                b.write(0x80 | n);
            } else if (n < 65536) {
                b.write(0x80 | 126);
                b.write((n >>> 8) & 0xff);
                b.write(n & 0xff);
            } else {
                b.write(0x80 | 127);
                long l = n;
                for (int i = 7; i >= 0; i--) b.write((int) ((l >>> (8 * i)) & 0xff));
            }
            byte[] mask = new byte[4];
            RND.nextBytes(mask);
            b.write(mask, 0, 4);
            byte[] masked = new byte[n];
            for (int i = 0; i < n; i++) masked[i] = (byte) (payload[i] ^ mask[i & 3]);
            b.write(masked, 0, n);
            synchronized (writeLock) {
                try {
                    out.write(b.toByteArray());
                    out.flush();
                } catch (IOException e) {
                    closed = true;
                    throw e;
                }
            }
        }

        void sendText(String s) throws IOException {
            send(OP_TEXT, s.getBytes(StandardCharsets.UTF_8));
        }

        static final class Msg {
            final int op;
            final byte[] data;
            Msg(int op, byte[] data) { this.op = op; this.data = data; }
        }

        Msg recv() throws IOException {
            ByteArrayOutputStream frag = null;
            int fragOp = 0;
            for (;;) {
                int b0 = readByte();
                int b1 = readByte();
                if (b0 < 0 || b1 < 0) throw new EOFException("ws eof");
                int op = b0 & 0x0f;
                boolean fin = (b0 & 0x80) != 0;
                long len = b1 & 0x7f;
                if (len == 126) {
                    len = (readByte() << 8) | readByte();
                } else if (len == 127) {
                    len = 0;
                    for (int i = 0; i < 8; i++) len = (len << 8) | readByte();
                }
                byte[] mask = null;
                if ((b1 & 0x80) != 0) {
                    mask = readFully(4);
                }
                byte[] payload = len == 0 ? new byte[0] : readFully((int) len);
                if (mask != null) {
                    for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
                }
                if (op == OP_PING) { send(OP_PONG, payload); continue; }
                if (op == OP_PONG) continue;
                if (op == OP_CLOSE) { closed = true; return new Msg(OP_CLOSE, payload); }
                if (op == 0x0) {
                    if (frag != null) {
                        frag.write(payload, 0, payload.length);
                        if (fin) return new Msg(fragOp, frag.toByteArray());
                    }
                    continue;
                }
                if (!fin) {
                    frag = new ByteArrayOutputStream();
                    frag.write(payload, 0, payload.length);
                    fragOp = op;
                    continue;
                }
                return new Msg(op, payload);
            }
        }

        int readByte() throws IOException {
            int c = in.read();
            return c;
        }

        byte[] readFully(int n) throws IOException {
            byte[] b = new byte[n];
            int off = 0;
            while (off < n) {
                int r = in.read(b, off, n - off);
                if (r < 0) throw new EOFException("ws eof");
                off += r;
            }
            return b;
        }

        @Override
        public void close() {
            if (closed) return;
            try { send(OP_CLOSE, new byte[0]); } catch (Exception ignored) { }  // best effort
            closed = true;
            try { sock.close(); } catch (IOException ignored) { }
        }
    }

    static final SecureRandom RND = new SecureRandom();

    /** Buffered reader over the socket stream; tracks pos/end explicitly. */
    static final class Buffered {
        final InputStream in;
        byte[] buf = new byte[0];
        int pos;
        int end;

        Buffered(InputStream in) {
            this.in = in;
        }

        /** Pull more bytes in; false at clean EOF. */
        boolean fill() throws IOException {
            if (end > 65536 && pos == end) { buf = new byte[0]; pos = 0; end = 0; }
            byte[] tmp = new byte[CHUNK];
            int r = in.read(tmp, 0, tmp.length);
            if (r <= 0) return false;
            byte[] joined = new byte[(end - pos) + r];
            System.arraycopy(buf, pos, joined, 0, end - pos);
            System.arraycopy(tmp, 0, joined, end - pos, r);
            buf = joined;
            pos = 0;
            end = joined.length;
            return true;
        }

        int available() { return end - pos; }

        /** Read up to and including the delimiter; null at clean EOF. */
        byte[] until(byte[] delim) throws IOException {
            for (;;) {
                for (int i = pos; i + delim.length <= end; i++) {
                    boolean hit = true;
                    for (int j = 0; j < delim.length; j++) {
                        if (buf[i + j] != delim[j]) { hit = false; break; }
                    }
                    if (hit) {
                        int stop = i + delim.length;
                        byte[] out = new byte[stop - pos];
                        System.arraycopy(buf, pos, out, 0, out.length);
                        pos = stop;
                        return out;
                    }
                }
                if (!fill()) {
                    if (available() == 0) return null;
                    byte[] out = new byte[available()];
                    System.arraycopy(buf, pos, out, 0, out.length);
                    pos = end;
                    return out;
                }
            }
        }

        /** Up to n bytes; null at EOF, never empty. */
        byte[] readUpTo(int n) throws IOException {
            if (available() == 0 && !fill()) return null;
            int take = Math.min(n, available());
            byte[] out = new byte[take];
            System.arraycopy(buf, pos, out, 0, take);
            pos += take;
            return out;
        }
    }

    static final byte[] CRLFCRLF = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    static Socket dialTcp(String host, int port) throws IOException {
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        // keeps NAT/firewall state from lapsing during long quiet periods
        s.setKeepAlive(true);
        s.connect(new InetSocketAddress(host, port), 15000);
        return s;
    }

    static Ws wsConnect(String host, int port, String path) throws IOException {
        Socket sock = dialTcp(host, port);
        // Only the tunnel socket gets a read timeout: recv() must surface a
        // silent peer as an exception instead of blocking forever.  Upstream
        // stream sockets legitimately idle for minutes, so they keep no timeout.
        sock.setSoTimeout(SO_TIMEOUT);
        try {
            return handshake(sock, host, port, path);
        } catch (IOException | RuntimeException e) {
            // A refused handshake (401 from a revoked token) leaves the peer
            // waiting on our FIN.  Without this close the socket sits in
            // CLOSE-WAIT and every retry leaks another fd, so a client left
            // pointed at a dead token slowly eats the box.
            try { sock.close(); } catch (IOException ignored) { }
            throw e;
        }
    }

    static Ws handshake(Socket sock, String host, int port, String path) throws IOException {
        OutputStream out = sock.getOutputStream();
        InputStream in = sock.getInputStream();
        byte[] kb = new byte[16];
        RND.nextBytes(kb);
        String key = Base64.getEncoder().encodeToString(kb);
        String hostHdr = (port == 443 || port == 80) ? host : host + ":" + port;
        String req = "GET " + path + " HTTP/1.1\r\n" +
                "Host: " + hostHdr + "\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Key: " + key + "\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                "User-Agent: ctun-client/" + VERSION + "\r\n\r\n";
        out.write(req.getBytes(StandardCharsets.US_ASCII));
        out.flush();

        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        while (matched < 4) {
            int c = in.read();
            if (c < 0) throw new IOException("handshake eof");
            head.write(c);
            if (c == CRLFCRLF[matched]) matched++;
            else matched = (c == '\r') ? 1 : 0;
        }
        String h = head.toString(StandardCharsets.ISO_8859_1);
        String status = h.split("\r\n", 2)[0];
        if (!status.contains(" 101 ")) throw new IOException("upgrade refused: " + status);
        return new Ws(sock, in, out);
    }

    // ------------------------------------------------------------------ stream

    static final class Stream {
        final Tunnel tunnel;
        final int sid;
        final Map<String, Object> meta;
        final BlockingQueue<byte[]> inbound = new LinkedBlockingQueue<>();
        static final byte[] END = new byte[0];
        volatile boolean closed;
        Socket sock;
        OutputStream out;
        boolean bodyChunked;
        /** Set once F_HEAD has been handed to the tunnel socket. */
        private volatile boolean headSent;

        /** Block until this stream's response head has gone out (or timeout). */
        void awaitHeadSent(long millis) {
            long deadline = System.currentTimeMillis() + millis;
            while (!headSent && !closed && System.currentTimeMillis() < deadline) {
                try { Thread.sleep(5); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        @SuppressWarnings("unchecked")
        Stream(Tunnel tunnel, int sid, Map<String, Object> meta) {
            this.tunnel = tunnel;
            this.sid = sid;
            this.meta = meta;
        }

        void feed(byte[] d) {
            if (!closed) inbound.offer(d);
        }

        void feedEnd() {
            if (!closed) inbound.offer(END);
        }

        /**
         * Emit F_HEAD, waiting briefly for the tunnel to come back if a reconnect
         * is in flight.  The head is the one frame the panel cannot reconstruct:
         * losing it turns a good response into "502 upstream closed before
         * sending a head".
         */
        void sendHead(byte[] head) throws Exception {
            long deadline = System.currentTimeMillis() + 5000;
            while (!tunnel.canSend() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            tunnel.send(F_HEAD, sid, head);
        }

        @SuppressWarnings("unchecked")
        String[] target() {
            String t = String.valueOf(meta.getOrDefault("target", "")).trim();
            if (t.isEmpty()) t = tunnel.defaultTarget;
            if (t.startsWith("[")) {
                int i = t.indexOf("]:");
                if (i > 0) return new String[]{t.substring(1, i), t.substring(i + 2)};
            }
            int i = t.lastIndexOf(':');
            if (i > 0) return new String[]{t.substring(0, i), t.substring(i + 1)};
            return new String[]{t, "80"};
        }

        @SuppressWarnings("unchecked")
        void start() {
            String kind = String.valueOf(meta.getOrDefault("kind", "http"));
            boolean forWs = "ws".equals(kind);
            String[] tgt = target();
            try {
                sock = dialTcp(tgt[0], Integer.parseInt(tgt[1]));
                sock.setTcpNoDelay(true);
                out = sock.getOutputStream();
            } catch (Exception e) {
                log("stream %d: dial %s failed: %s", sid, meta.get("target"), e.getMessage());
                try {
                    tunnel.send(F_CLOSE, sid, ("dial failed: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
                } catch (Exception ignored) { }
                closed = true;
                return;
            }
            byte[] head = buildHead(forWs);
            try {
                out.write(head);
                out.flush();
            } catch (IOException e) {
                try { tunnel.send(F_CLOSE, sid, e.getMessage().getBytes(StandardCharsets.UTF_8)); } catch (Exception ignored) { }
                closed = true;
                return;
            }
            Thread pump = new Thread(this::pumpInbound, "ctun-in-" + sid);
            pump.setDaemon(true);
            pump.start();
            Thread resp = new Thread(forWs ? this::doWs : this::doHttp, "ctun-out-" + sid);
            resp.setDaemon(true);
            resp.start();
        }

        void pumpInbound() {
            try {
                for (;;) {
                    byte[] d = inbound.take();
                    if (d == END) {
                        if (bodyChunked) {
                            out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                        }
                        // Do NOT shutdownOutput() here for a body-less request.
                        // The server's END just means "that's the whole request";
                        // FINing the socket at that instant races the origin's
                        // response, and the reader wakes to EOF with zero bytes —
                        // the panel then 502s with "upstream closed before
                        // sending a head: end".  Requests that are delimiter-
                        // terminated (chunked, or a body we already wrote whole)
                        // are finished by the "Connection: close" header instead.
                        return;
                    }
                    if (bodyChunked) {
                        out.write((Integer.toHexString(d.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                        out.write(d);
                        out.write(CRLF);
                    } else {
                        out.write(d);
                    }
                    out.flush();
                }
            } catch (Exception ignored) {
            }
        }

        @SuppressWarnings("unchecked")
        byte[] buildHead(boolean forWs) {
            List<String[]> hdrs = new ArrayList<>();
            String host = String.valueOf(meta.getOrDefault("host", ""));
            if (!host.isEmpty()) hdrs.add(new String[]{"Host", host});
            String clen = null;
            Object raw = meta.get("headers");
            if (raw instanceof List) {
                for (Object o : (List<Object>) raw) {
                    List<Object> kv = (List<Object>) o;
                    String k = String.valueOf(kv.get(0));
                    String v = String.valueOf(kv.get(1));
                    String kl = k.toLowerCase();
                    if (kl.equals("content-length")) { clen = v; continue; }
                    if (kl.equals("host") || kl.equals("connection") || kl.equals("keep-alive")
                            || kl.equals("proxy-connection") || kl.equals("upgrade")
                            || kl.equals("transfer-encoding") || kl.equals("te") || kl.equals("trailer")) continue;
                    hdrs.add(new String[]{k, v});
                }
            }
            String method = String.valueOf(meta.getOrDefault("method", "GET"));
            if (forWs) {
                hdrs.add(new String[]{"Upgrade", "websocket"});
                hdrs.add(new String[]{"Connection", "Upgrade"});
            } else if (clen != null && clen.matches("\\d+")) {
                hdrs.add(new String[]{"Content-Length", clen});
            } else if (!method.equals("GET") && !method.equals("HEAD")
                    && !method.equals("DELETE") && !method.equals("OPTIONS")) {
                hdrs.add(new String[]{"Transfer-Encoding", "chunked"});
                bodyChunked = true;
            }
            String cip = String.valueOf(meta.getOrDefault("client_ip", ""));
            int ci = cip.lastIndexOf(':');
            if (ci > 0) cip = cip.substring(0, ci);
            if (!cip.isEmpty()) hdrs.add(new String[]{"X-Forwarded-For", cip});
            hdrs.add(new String[]{"X-Forwarded-Proto", "https"});
            if (!host.isEmpty()) hdrs.add(new String[]{"X-Forwarded-Host", host});
            if (!forWs) hdrs.add(new String[]{"Connection", "close"});

            StringBuilder sb = new StringBuilder();
            sb.append(method).append(' ').append(meta.getOrDefault("path", "/")).append(" HTTP/1.1\r\n");
            for (String[] kv : hdrs) sb.append(kv[0]).append(": ").append(kv[1]).append("\r\n");
            sb.append("\r\n");
            return sb.toString().getBytes(StandardCharsets.ISO_8859_1);
        }

        void doHttp() {
            try {
                Buffered r = new Buffered(sock.getInputStream());
                byte[] head = null;
                int status = 0;
                // 1xx are interim; the final status still follows.
                for (;;) {
                    head = r.until(CRLFCRLF);
                    if (head == null || head.length == 0) {
                        tunnel.send(F_END, sid, null);
                        close();
                        return;
                    }
                    try {
                        String[] parts = new String(head, StandardCharsets.ISO_8859_1)
                                .split("\r\n", 2)[0].split(" ");
                        if (parts.length > 1) status = Integer.parseInt(parts[1].trim());
                    } catch (Exception ignored) { status = 0; }
                    if (!(status >= 100 && status < 200 && status != 101)) break;
                }
                sendHead(head);
                headSent = true;
                String txt = new String(head, StandardCharsets.ISO_8859_1);
                String[] lines = txt.split("\r\n");
                Map<String, String> h = new LinkedHashMap<>();
                for (int i = 1; i < lines.length; i++) {
                    int c = lines[i].indexOf(':');
                    if (c > 0) {
                        String k = lines[i].substring(0, c).trim().toLowerCase();
                        h.putIfAbsent(k, lines[i].substring(c + 1).trim());
                    }
                }
                boolean noBody = status == 204 || status == 304 || status < 200 || status > 0 && status < 200
                        || "HEAD".equals(String.valueOf(meta.getOrDefault("method", "GET")));
                if (noBody) {
                    // no body
                } else if (String.valueOf(h.getOrDefault("transfer-encoding", "")).toLowerCase().contains("chunked")) {
                    for (;;) {
                        byte[] ln = r.until(CRLF);
                        if (ln == null) break;
                        String s = new String(ln, StandardCharsets.ISO_8859_1).trim();
                        int semi = s.indexOf(';');
                        if (semi >= 0) s = s.substring(0, semi);
                        int size = s.isEmpty() ? 0 : Integer.parseInt(s.trim(), 16);
                        if (size == 0) {
                            r.until(CRLF);
                            break;
                        }
                        int remaining = size;
                        while (remaining > 0) {
                            byte[] d = r.readUpTo(Math.min(CHUNK, remaining));
                            if (d == null || d.length == 0) throw new EOFException("truncated");
                            remaining -= d.length;
                            tunnel.send(F_DATA, sid, d);
                        }
                        r.until(CRLF);
                    }
                } else if (h.containsKey("content-length")) {
                    int remaining = Integer.parseInt(h.get("content-length").trim());
                    while (remaining > 0) {
                        byte[] d = r.readUpTo(Math.min(CHUNK, remaining));
                        if (d == null || d.length == 0) break;
                        remaining -= d.length;
                        tunnel.send(F_DATA, sid, d);
                    }
                } else {
                    for (;;) {
                        byte[] d = r.readUpTo(CHUNK);
                        if (d == null || d.length == 0) break;
                        tunnel.send(F_DATA, sid, d);
                    }
                }
            } catch (Exception ignored) {
            } finally {
                try { tunnel.send(F_END, sid, null); } catch (Exception ignored) { }
                close();
            }
        }

        void doWs() {
            try {
                Buffered r = new Buffered(sock.getInputStream());
                // A local service that accepts but never upgrades would hold
                // this stream (and its tunnel slot) open forever; upstream
                // sockets carry no read timeout, so arm one just for the head.
                int wsHead = tunnel.cfg.wsHead > 0 ? tunnel.cfg.wsHead : 60;
                try {
                    sock.setSoTimeout(wsHead * 1000);
                } catch (IOException ignored) { }
                byte[] head;
                try {
                    head = r.until(CRLFCRLF);
                } finally {
                    try { sock.setSoTimeout(0); } catch (IOException ignored) { }
                }
                if (head == null || head.length == 0) {
                    tunnel.send(F_CLOSE, sid, "ws upstream closed".getBytes(StandardCharsets.UTF_8));
                    close();
                    return;
                }
                sendHead(head);
                headSent = true;
                for (;;) {
                    byte[] d = r.readUpTo(CHUNK);
                    if (d == null || d.length == 0) break;
                    tunnel.send(F_DATA, sid, d);
                }
            } catch (Exception ignored) {
            } finally {
                try { tunnel.send(F_END, sid, null); } catch (Exception ignored) { }
                close();
            }
        }

        void close() {
            if (closed) return;
            closed = true;
            inbound.offer(END);
            try { if (sock != null) sock.close(); } catch (IOException ignored) { }
        }
    }

    // ------------------------------------------------------------------ tunnel

    static final class Tunnel {
        final Config cfg;
        final String defaultTarget;
        final Map<Integer, Stream> streams = new ConcurrentHashMap<>();
        volatile Ws ws;
        volatile boolean stopped;

        Tunnel(Config cfg, String defaultTarget) {
            this.cfg = cfg;
            this.defaultTarget = defaultTarget;
        }

        void send(int type, int sid, byte[] payload) throws IOException {
            Ws w = ws;
            if (w == null) throw new IOException("tunnel not connected");
            if (w.closed) throw new IOException("tunnel socket closed");
            int n = payload == null ? 0 : payload.length;
            byte[] f = new byte[5 + n];
            f[0] = (byte) type;
            f[1] = (byte) (sid >>> 24);
            f[2] = (byte) (sid >>> 16);
            f[3] = (byte) (sid >>> 8);
            f[4] = (byte) sid;
            if (n > 0) System.arraycopy(payload, 0, f, 5, n);
            w.send(OP_BIN, f);
        }

        /** True while the underlying websocket can still carry frames. */
        boolean canSend() {
            Ws w = ws;
            return w != null && !w.closed;
        }

        void run(int idx) {
            long backoff = 1000;
            for (;;) {
                if (stopped) return;
                try {
                    String path = cfg.path + "/ws/tunnel/" + cfg.client + "?token=" + cfg.token;
                    ws = wsConnect(cfg.server, cfg.port, path);
                    backoff = 1000;
                    log("conn %d connected to %s:%d", idx, cfg.server, cfg.port);
                    Map<String, Object> hello = new LinkedHashMap<>();
                    hello.put("t", "hello");
                    hello.put("version", VERSION);
                    hello.put("conns", cfg.conns);
                    hello.put("host", hostname());
                    ws.sendText(json(hello));
                    loop();
                } catch (Exception e) {
                    log("conn %d: %s", idx, String.valueOf(e.getMessage()));
                } finally {
                    // Give in-flight responses a moment to emit their F_HEAD
                    // before tearing the streams down.  A stream whose doHttp
                    // thread is between "read head" and "send F_HEAD" gets its
                    // send swallowed by close() otherwise, and the panel sees
                    // an END with no HEAD -> the bogus "502 upstream closed
                    // before sending a head: end".
                    for (Stream s : streams.values()) s.awaitHeadSent(2000);
                    for (Stream s : streams.values()) s.close();
                    streams.clear();
                    Ws w = ws;
                    if (w != null) w.close();
                    ws = null;
                }
                // stop() closes the socket, which lands us here with an exception.
                // Bail before the backoff sleep or shutdown drags on for up to 20s.
                if (stopped) return;
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    return;
                }
                backoff = Math.min((long) (backoff * 1.8), 20000L);
            }
        }

        void loop() throws Exception {
            Ws w = ws;
            Thread pinger = new Thread(() -> {
                while (!w.closed) {
                    try {
                        Thread.sleep(PING_EVERY_MS);
                        send(F_PING, 0, new byte[]{'1'});
                    } catch (Exception e) {
                        return;
                    }
                }
            }, "ctun-ping");
            pinger.setDaemon(true);
            pinger.start();

            for (;;) {
                Ws.Msg m = w.recv();
                if (m.op == OP_CLOSE) {
                    log("server closed the tunnel");
                    return;
                }
                if (m.op == OP_TEXT) {
                    String s = new String(m.data, StandardCharsets.UTF_8);
                    if (s.contains("\"welcome\"")) {
                        Map<String, Object> hello = parseJson(s);
                        Object routes = hello.get("routes");
                        log("welcome: client=%s cid=%s routes=%s",
                                hello.get("name"), hello.get("cid"),
                                routes instanceof List ? ((List<?>) routes).size() : "?");
                    }
                    continue;
                }
                if (m.op != OP_BIN || m.data.length < 5) continue;
                int type = m.data[0] & 0xff;
                int sid = ((m.data[1] & 0xff) << 24) | ((m.data[2] & 0xff) << 16)
                        | ((m.data[3] & 0xff) << 8) | (m.data[4] & 0xff);
                byte[] payload = new byte[m.data.length - 5];
                System.arraycopy(m.data, 5, payload, 0, payload.length);
                switch (type) {
                    case F_OPEN: {
                        Map<String, Object> meta = parseJson(new String(payload, StandardCharsets.UTF_8));
                        Stream s = new Stream(this, sid, meta);
                        streams.put(sid, s);
                        s.start();
                        break;
                    }
                    case F_DATA: {
                        Stream s = streams.get(sid);
                        if (s != null) s.feed(payload);
                        break;
                    }
                    case F_END: {
                        Stream s = streams.remove(sid);
                        if (s != null) s.feedEnd();
                        break;
                    }
                    case F_CLOSE: {
                        Stream s = streams.remove(sid);
                        if (s != null) s.close();
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    // ------------------------------------------------------------------ json

    /** Tiny JSON writer/reader — enough for the control plane, no dependencies. */
    @SuppressWarnings("unchecked")
    static String json(Object o) {
        StringBuilder sb = new StringBuilder();
        writeJson(sb, o);
        return sb.toString();
    }

    static void writeJson(StringBuilder sb, Object o) {
        if (o == null) { sb.append("null"); return; }
        if (o instanceof String) {
            sb.append('"');
            for (char c : ((String) o).toCharArray()) {
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default:
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                }
            }
            sb.append('"');
        } else if (o instanceof Number || o instanceof Boolean) {
            sb.append(o);
        } else if (o instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) o).entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeJson(sb, e.getKey());
                sb.append(':');
                writeJson(sb, e.getValue());
            }
            sb.append('}');
        } else if (o instanceof Iterable) {
            sb.append('[');
            boolean first = true;
            for (Object v : (Iterable<Object>) o) {
                if (!first) sb.append(',');
                first = false;
                writeJson(sb, v);
            }
            sb.append(']');
        } else {
            writeJson(sb, String.valueOf(o));
        }
    }

    static int skipWs(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    static Map<String, Object> parseJson(String s) {
        try {
            int[] pos = {0};
            Object v = parseValue(s, pos);
            return v instanceof Map ? (Map<String, Object>) v : new LinkedHashMap<>();
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    @SuppressWarnings("unchecked")
    static Object parseValue(String s, int[] pos) {
        int i = skipWs(s, pos[0]);
        if (i >= s.length()) return null;
        char c = s.charAt(i);
        if (c == '{') {
            Map<String, Object> m = new LinkedHashMap<>();
            i = skipWs(s, i + 1);
            if (i < s.length() && s.charAt(i) == '}') { pos[0] = i + 1; return m; }
            for (;;) {
                i = skipWs(s, i);
                if (i >= s.length() || s.charAt(i) != '"') { pos[0] = i; return m; }
                int[] kp = {i};
                String k = (String) parseValue(s, kp);
                i = skipWs(s, kp[0]);
                if (i < s.length() && s.charAt(i) == ':') i++;
                pos[0] = i;
                Object v = parseValue(s, pos);
                m.put(k, v);
                i = skipWs(s, pos[0]);
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == '}') { pos[0] = i + 1; return m; }
                pos[0] = i;
                return m;
            }
        }
        if (c == '[') {
            List<Object> l = new ArrayList<>();
            i = skipWs(s, i + 1);
            if (i < s.length() && s.charAt(i) == ']') { pos[0] = i + 1; return l; }
            for (;;) {
                pos[0] = i;
                Object v = parseValue(s, pos);
                l.add(v);
                i = skipWs(s, pos[0]);
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == ']') { pos[0] = i + 1; return l; }
                pos[0] = i;
                return l;
            }
        }
        if (c == '"') {
            StringBuilder sb = new StringBuilder();
            i++;
            while (i < s.length()) {
                char ch = s.charAt(i++);
                if (ch == '\\' && i < s.length()) {
                    char n = s.charAt(i++);
                    switch (n) {
                        case 'n': sb.append('\n'); break;
                        case 'r': sb.append('\r'); break;
                        case 't': sb.append('\t'); break;
                        case 'b': sb.append('\b'); break;
                        case 'f': sb.append('\f'); break;
                        case 'u':
                            sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                            break;
                        default: sb.append(n);
                    }
                } else if (ch == '"') {
                    break;
                } else sb.append(ch);
            }
            pos[0] = i;
            return sb.toString();
        }
        if (s.startsWith("true", i)) { pos[0] = i + 4; return Boolean.TRUE; }
        if (s.startsWith("false", i)) { pos[0] = i + 5; return Boolean.FALSE; }
        if (s.startsWith("null", i)) { pos[0] = i + 4; return null; }
        int start = i;
        while (i < s.length() && "-+.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
        pos[0] = i;
        String num = s.substring(start, i);
        if (num.isEmpty()) return null;
        if (num.contains(".") || num.contains("e") || num.contains("E")) return Double.parseDouble(num);
        return Long.parseLong(num);
    }


    static String hostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "java";
        }
    }

    // ------------------------------------------------------------------ config

    // ============================== EDIT ME ==============================
    // Everything below is filled in already - just upload and run it.
    //   javac CtunClient.java && java CtunClient
    // or without compiling (JDK 11+):
    //   java CtunClient.java
    static final String CFG_SERVER   = "tunnel.example.com";  // panel / public host
    static final int    CFG_PORT     = 65000;                 // panel listener port
    static final String CFG_TOKEN    = "ct_xxxxxxxxxxxxxxxx"; // client token from the panel
    static final String[] CFG_ROUTES = { "127.0.0.1:65534" }; // local services "[name:]host:port"
    static final int    CFG_CONNS    = 4;                     // parallel tunnel sockets
    static final String CFG_PATH     = "";                    // ws prefix, only behind a reverse proxy
    static final int    CFG_WS_HEAD  = 60;                    // seconds the local ws gets to answer the upgrade
    // ====================================================================

    static final class Config {
        String server = CFG_SERVER;
        String token = CFG_TOKEN;
        List<String> routes = new ArrayList<>();
        int conns = CFG_CONNS;
        int port = CFG_PORT;
        String client = "";
        String path = CFG_PATH;
        int wsHead = CFG_WS_HEAD;
    }

    static Config parseArgs(String[] argv) {
        Config c = new Config();
        for (int i = 0; i < argv.length; i++) {
            String k = argv[i];
            switch (k) {
                case "--server": c.server = argv[++i]; break;
                case "--token": c.token = argv[++i]; break;
                case "--route": c.routes.add(argv[++i]); break;
                case "--conns": c.conns = Integer.parseInt(argv[++i]); break;
                case "--port": c.port = Integer.parseInt(argv[++i]); break;
                case "--client": c.client = argv[++i]; break;
                case "--path": c.path = argv[++i]; break;
                case "-h":
                case "--help":
                    System.out.println("usage: java CtunClient  (runs on the CFG_* constants)\n" +
                            "  overrides: --server HOST --token TOKEN --route [name:]host:port " +
                            " [--route ...] [--conns 4] [--port 65000]");
                    System.exit(0);
                    break;
                default:
                    if (k.startsWith("--")) {
                        System.err.println("unknown option " + k);
                        System.exit(2);
                    }
            }
        }
        if (c.routes.isEmpty()) {
            for (String r : CFG_ROUTES) c.routes.add(r);
        }
        if (c.server.isEmpty() || c.token.isEmpty() || c.routes.isEmpty()) {
            System.err.println("CFG_SERVER, CFG_TOKEN and CFG_ROUTES are required");
            System.exit(2);
        }
        c.conns = Math.max(1, Math.min(64, c.conns));
        while (c.path.endsWith("/")) c.path = c.path.substring(0, c.path.length() - 1);
        return c;
    }

    /** [name:]host:port -> target only; the panel owns the real routing table. */
    static String routeTarget(String spec) {
        if (spec.startsWith("[")) return spec;
        int first = spec.indexOf(':');
        int count = 0;
        for (char ch : spec.toCharArray()) if (ch == ':') count++;
        if (count >= 2 && first > 0) {
            String head = spec.substring(0, first);
            if (head.matches("[A-Za-z0-9._-]+")) return spec.substring(first + 1);
        }
        return spec;
    }


    // ------------------------------------------------------------------ embed API
    //
    // CtunClient.Tunnel / Ws / Stream / Config are nested, so they never collide with
    // names in a host project — you only ever reference this one facade:
    //
    //   CtunClient.CtunTunnel t = new CtunClient.CtunTunnel("1.2.3.4", 65000, "ct_xxx");
    //   t.route("127.0.0.1:8080");
    //   t.route("api:127.0.0.1:3000");
    //   t.conns(4);
    //   t.start();
    //   ...
    //   t.close();
    //
    // close() is idempotent and safe from any thread.

    public static final class CtunTunnel {
        private final Config cfg = new Config();
        private final List<Thread> threads = new ArrayList<>();
        private final List<Tunnel> tunnels = new ArrayList<>();
        private volatile boolean closed = false;

        public CtunTunnel(String server, int port, String token) {
            cfg.server = server;
            cfg.port = port;
            cfg.token = token;
        }

        public CtunTunnel route(String spec) {
            cfg.routes.add(spec);
            return this;
        }

        public CtunTunnel conns(int n) {
            cfg.conns = Math.max(1, Math.min(64, n));
            return this;
        }

        public CtunTunnel client(String name) {
            cfg.client = name == null ? "" : name;
            return this;
        }

        public CtunTunnel path(String p) {
            cfg.path = p == null ? "" : p;
            return this;
        }

        public CtunTunnel start() {
            if (closed) throw new IllegalStateException("tunnel already closed");
            if (cfg.server.isEmpty() || cfg.token.isEmpty()) {
                throw new IllegalArgumentException("server and token are required");
            }
            if (cfg.routes.isEmpty()) {
                throw new IllegalArgumentException("at least one route() is required");
            }
            if (!threads.isEmpty()) return this;

            List<String> clean = new ArrayList<>();
            for (String r : cfg.routes) clean.add(routeTarget(r));
            cfg.routes = clean;
            final String dt = clean.get(0);

            for (int i = 0; i < cfg.conns; i++) {
                final int idx = i + 1;
                final Tunnel tun = new Tunnel(cfg, dt);
                tunnels.add(tun);
                Thread t = new Thread(() -> tun.run(idx), "ctun-conn-" + idx);
                t.setDaemon(true);
                threads.add(t);
                t.start();
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return this;
        }

        public boolean running() {
            if (closed) return false;
            for (Thread t : threads) if (t.isAlive()) return true;
            return false;
        }

        public void close() {
            if (closed) return;
            closed = true;
            for (Tunnel tun : tunnels) {
                try {
                    tun.stopped = true;
                    Ws w = tun.ws;
                    if (w != null) w.close();
                    for (Stream s : tun.streams.values()) s.close();
                    tun.streams.clear();
                } catch (Exception e) {
                    // best effort — a dead socket needs no cleanup
                }
            }
            for (Thread t : threads) t.interrupt();
            threads.clear();
            tunnels.clear();
        }
    }

    public static void main(String[] argv) throws Exception {
        Config cfg = parseArgs(argv);
        List<String> clean = new ArrayList<>();
        String defaultTarget = null;
        for (String r : cfg.routes) {
            String t = routeTarget(r);
            clean.add(t);
            if (defaultTarget == null) defaultTarget = t;
        }
        cfg.routes = clean;
        log("ctun client %s -> ws://%s:%d routes=%s conns=%d",
                VERSION, cfg.server, cfg.port, String.join(",", clean), cfg.conns);

        final String dt = defaultTarget;
        for (int i = 0; i < cfg.conns; i++) {
            final int idx = i + 1;
            Thread t = new Thread(() -> new Tunnel(cfg, dt).run(idx), "ctun-conn-" + idx);
            t.setDaemon(false);
            t.start();
            Thread.sleep(250);
        }
    }
}
