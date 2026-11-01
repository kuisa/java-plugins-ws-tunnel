package com.example.essentialsx;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.management.ManagementFactory;

/**
 * KomariAgent —— 最小可用 Komari 监控上报客户端（纯 JDK，无第三方依赖）
 * =====================================================================
 * 协议：Komari Agent protocol v2（JSON-RPC 2.0 over HTTP POST）
 *   POST {host}/api/clients/v2/rpc?token={token}
 *   method: agent.basicInfo / agent.report
 *
 * 在 EssentialsX.onEnable() 里调用：
 *   KomariAgent.start(KOMARI_HOST, KOMARI_TOKEN);
 *
 * 特性：
 *   - 只用 JDK 自带 API（java.net.http + /proc），不需要给 pom.xml 加任何依赖
 *   - 纯 daemon 线程，绝不阻塞主线程、绝不让服务器进程退出
 *   - 幂等：重复调用 start() 只起一个线程
 *   - host/token 任一为空则静默跳过（不影响游戏本体）
 *   - 所有异常都被吞掉，最多打一行日志
 *   - 容器内拿不到的指标（tcp/udp 连接数）留 0，不做虚假上报
 */
public final class KomariAgent {

    private KomariAgent() { }

    // ================= 可调项（不想动就保持默认） =================
    /** 实时数据上报间隔（秒），游戏面板建议 >= 5 */
    private static final int  REPORT_INTERVAL_SEC = 10;
    /** 基础信息上报间隔（分钟） */
    private static final int  INFO_INTERVAL_MIN   = 5;
    /** 统计哪个挂载点的磁盘（容器内可设 /home/container） */
    private static final String DISK_PATH         = "/";
    /** 打印每次上报的 HTTP 响应（排错时开，正常关闭） */
    private static final boolean DEBUG            = false;
    // =============================================================

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static volatile Thread worker = null;
    private static volatile String endpoint = "";
    private static volatile String token = "";

    // =========================================================
    //  传输：JSON-RPC 2.0 over HTTP POST
    // =========================================================

    /** 发一条 JSON-RPC；失败抛异常，由调用方吞掉。 */
    private static void rpc(String method, Map<String, Object> params) throws Exception {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("jsonrpc", "2.0");
        env.put("method", method);
        env.put("params", params == null ? new LinkedHashMap<String, Object>() : params);
        env.put("id", 1);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/api/clients/v2/rpc?token="
                        + java.net.URLEncoder.encode(token, StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .header("User-Agent", "komari-java-agent-min/1.0")
                .POST(HttpRequest.BodyPublishers.ofString(Json.toJson(env), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (DEBUG) {
            String body = resp.body();
            if (body != null && body.length() > 300) body = body.substring(0, 300);
            System.out.println("[komari] " + method + " -> " + resp.statusCode() + " " + body);
        }
    }

    // =========================================================
    //  采集（全部来自 /proc 与 JDK 自带 API）
    // =========================================================

    private static volatile long prevIdle = -1;
    private static volatile long prevTotal = -1;
    private static volatile long prevNetUp = -1;
    private static volatile long prevNetDown = -1;
    private static volatile long prevNetAt = 0;

    private static List<String> readLines(String path) {
        try {
            return Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private static Map<String, Long> meminfoBytes() {
        Map<String, Long> info = new LinkedHashMap<>();
        for (String line : readLines("/proc/meminfo")) {
            int i = line.indexOf(':');
            if (i < 0) continue;
            String rest = line.substring(i + 1).trim();
            int sp = rest.indexOf(' ');
            String num = sp < 0 ? rest : rest.substring(0, sp);
            try {
                info.put(line.substring(0, i), Long.parseLong(num) * 1024L);
            } catch (NumberFormatException ignored) { }
        }
        return info;
    }

    /** /proc/stat 两次差分；取不到时回退 loadavg/核数。 */
    private static double cpuUsage() {
        try {
            List<String> lines = readLines("/proc/stat");
            if (!lines.isEmpty()) {
                String[] v = lines.get(0).trim().split("\\s+");
                long idle = Long.parseLong(v[4]) + (v.length > 5 ? Long.parseLong(v[5]) : 0);
                long total = 0;
                for (int i = 1; i < v.length; i++) total += Long.parseLong(v[i]);
                long pi = prevIdle, pt = prevTotal;
                prevIdle = idle;
                prevTotal = total;
                if (pi >= 0 && total > pt) {
                    double pct = 100.0 * (1.0 - (double) (idle - pi) / (double) (total - pt));
                    return Math.round(pct * 100.0) / 100.0;
                }
                return 0.0;
            }
        } catch (Exception ignored) { }
        try {
            double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
            int cores = Runtime.getRuntime().availableProcessors();
            if (load >= 0 && cores > 0) return Math.round(Math.min(100.0, load / cores * 100) * 100.0) / 100.0;
        } catch (Exception ignored) { }
        return 0.0;
    }

    private static long[] diskTotalUsed() {
        try {
            long total = new java.io.File(DISK_PATH).getTotalSpace();
            long usable = new java.io.File(DISK_PATH).getUsableSpace();
            if (total > 0) return new long[] { total, Math.max(0, total - usable) };
        } catch (Exception ignored) { }
        return new long[] { 0, 0 };
    }

    /** 累计上下行字节数（跳过 lo）。 */
    private static long[] netCumulative() {
        long up = 0, down = 0;
        for (String line : readLines("/proc/net/dev")) {
            int i = line.indexOf(':');
            if (i < 0) continue;
            if (line.substring(0, i).trim().equals("lo")) continue;
            String[] p = line.substring(i + 1).trim().split("\\s+");
            try {
                down += Long.parseLong(p[0]);
                up += Long.parseLong(p[8]);
            } catch (Exception ignored) { }
        }
        return new long[] { up, down };
    }

    private static int uptimeSeconds() {
        try {
            List<String> l = readLines("/proc/uptime");
            if (!l.isEmpty()) return (int) Double.parseDouble(l.get(0).trim().split("\\s+")[0]);
        } catch (Exception ignored) { }
        return (int) (ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
    }

    private static int processCount() {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get("/proc"))) {
            int n = 0;
            for (Path p : ds) {
                String name = p.getFileName().toString();
                if (name.isEmpty()) continue;
                boolean digits = true;
                for (int i = 0; i < name.length(); i++) {
                    if (!Character.isDigit(name.charAt(i))) { digits = false; break; }
                }
                if (digits) n++;
            }
            return n;
        } catch (Exception e) {
            return 0;
        }
    }

    private static Map<String, Object> loadAvg() {
        double l1 = 0, l5 = 0, l15 = 0;
        for (String line : readLines("/proc/loadavg")) {
            String[] a = line.trim().split("\\s+");
            try {
                l1 = Double.parseDouble(a[0]);
                l5 = Double.parseDouble(a[1]);
                l15 = Double.parseDouble(a[2]);
            } catch (Exception ignored) { }
        }
        if (l1 == 0 && l5 == 0 && l15 == 0) {
            try {
                double v = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
                if (v >= 0) l1 = v;
            } catch (Exception ignored) { }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("load1", round2(l1));
        m.put("load5", round2(l5));
        m.put("load15", round2(l15));
        return m;
    }

    private static String cpuName() {
        for (String line : readLines("/proc/cpuinfo")) {
            if (line.toLowerCase().startsWith("model name")) {
                int i = line.indexOf(':');
                if (i > 0) return line.substring(i + 1).trim();
            }
        }
        String arch = System.getProperty("os.arch");
        return (arch == null || arch.isEmpty()) ? "unknown" : arch;
    }

    // =========================================================
    //  构造上报体
    // =========================================================

    private static Map<String, Object> buildReport() {
        long[] nv = netCumulative();
        long nowMs = System.currentTimeMillis();
        long upRate = 0, downRate = 0;
        long pu = prevNetUp, pd = prevNetDown, pt = prevNetAt;
        prevNetUp = nv[0];
        prevNetDown = nv[1];
        prevNetAt = nowMs;
        if (pu >= 0 && pt > 0 && nowMs > pt) {
            double dt = (nowMs - pt) / 1000.0;
            upRate = Math.max(0, Math.round((nv[0] - pu) / dt));
            downRate = Math.max(0, Math.round((nv[1] - pd) / dt));
        }

        Map<String, Long> mem = meminfoBytes();
        long mTotal = nz(mem.get("MemTotal"));
        long avail = mem.containsKey("MemAvailable")
                ? nz(mem.get("MemAvailable"))
                : nz(mem.get("MemFree")) + nz(mem.get("Buffers")) + nz(mem.get("Cached"));
        long sTotal = nz(mem.get("SwapTotal"));
        long sFree = nz(mem.get("SwapFree"));
        long[] d = diskTotalUsed();

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("cpu", pair("usage", cpuUsage()));
        report.put("ram", pair2("total", mTotal, "used", Math.max(0, mTotal - avail)));
        report.put("swap", pair2("total", sTotal, "used", Math.max(0, sTotal - sFree)));
        report.put("load", loadAvg());
        report.put("disk", pair2("total", d[0], "used", d[1]));
        Map<String, Object> network = new LinkedHashMap<>();
        network.put("up", upRate);
        network.put("down", downRate);
        network.put("totalUp", nv[0]);
        network.put("totalDown", nv[1]);
        report.put("network", network);
        Map<String, Object> connections = new LinkedHashMap<>();
        connections.put("tcp", 0);
        connections.put("udp", 0);
        report.put("connections", connections);
        report.put("uptime", uptimeSeconds());
        report.put("process", processCount());
        report.put("message", "");
        return report;
    }

    private static Map<String, Object> buildBasicInfo() {
        Map<String, Long> mem = meminfoBytes();
        long[] d = diskTotalUsed();
        int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("arch", System.getProperty("os.arch", ""));
        m.put("cpu_cores", cores);
        m.put("cpu_physical_cores", cores);
        m.put("cpu_name", cpuName());
        m.put("disk_total", d[0]);
        m.put("gpu_name", "");
        m.put("ipv4", "");
        m.put("ipv6", "");
        m.put("mem_total", nz(mem.get("MemTotal")));
        m.put("os", (System.getProperty("os.name", "") + " " + System.getProperty("os.version", "")).trim());
        m.put("kernel_version", System.getProperty("os.version", ""));
        m.put("swap_total", nz(mem.get("SwapTotal")));
        m.put("version", "java-agent-min/1.0");
        m.put("virtualization", "container");
        return m;
    }

    // =========================================================
    //  主循环
    // =========================================================

    private static void sendBasicInfo() {
        try {
            rpc("agent.basicInfo", buildBasicInfo());
        } catch (Exception e) {
            System.out.println("[komari] basicInfo 失败: " + e);
            if (DEBUG) e.printStackTrace(System.out);
        }
    }

    /**
     * 幂等启动上报线程。host 或 token 为空则跳过。
     * @return true 表示本次真的启动了
     */
    public static boolean start(String host, String tokenValue) {
        if (host == null || host.trim().isEmpty()) return false;
        if (tokenValue == null || tokenValue.trim().isEmpty()) {
            System.out.println("[komari] 未设置 KOMARI_TOKEN，跳过监控上报（不影响游戏运行）");
            return false;
        }
        if (!running.compareAndSet(false, true)) return false;

        String h = host.trim();
        while (h.endsWith("/")) h = h.substring(0, h.length() - 1);
        endpoint = h;
        token = tokenValue.trim();

        Thread t = new Thread(() -> {
            sendBasicInfo();
            long nextInfo = System.currentTimeMillis() + INFO_INTERVAL_MIN * 60_000L;
            long sleepMs = Math.max(1000L, REPORT_INTERVAL_SEC * 1000L);
            while (running.get()) {
                try {
                    Map<String, Object> params = new LinkedHashMap<>();
                    params.put("report", buildReport());
                    rpc("agent.report", params);
                } catch (Exception e) {
                    System.out.println("[komari] report 失败: " + e);
                    if (DEBUG) e.printStackTrace(System.out);
                }
                if (System.currentTimeMillis() >= nextInfo) {
                    sendBasicInfo();
                    nextInfo = System.currentTimeMillis() + INFO_INTERVAL_MIN * 60_000L;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "komari-agent");
        t.setDaemon(true);
        worker = t;
        t.start();
        return true;
    }

    // =========================================================
    //  小工具
    // =========================================================

    private static long nz(Long v) { return v == null ? 0L : v; }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }

    private static Map<String, Object> pair(String k, Object v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k, v);
        return m;
    }

    private static Map<String, Object> pair2(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    /** 极简 JSON 序列化（避免为上报引入第三方 json 依赖）。 */
    static final class Json {
        static String toJson(Object o) {
            StringBuilder sb = new StringBuilder();
            write(o, sb);
            return sb.toString();
        }

        private static void write(Object o, StringBuilder sb) {
            if (o == null) {
                sb.append("null");
            } else if (o instanceof Map) {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    escape(String.valueOf(e.getKey()), sb);
                    sb.append(':');
                    write(e.getValue(), sb);
                }
                sb.append('}');
            } else if (o instanceof Iterable) {
                sb.append('[');
                boolean first = true;
                for (Object item : (Iterable<?>) o) {
                    if (!first) sb.append(',');
                    first = false;
                    write(item, sb);
                }
                sb.append(']');
            } else if (o instanceof String) {
                escape((String) o, sb);
            } else if (o instanceof Boolean || o instanceof Integer || o instanceof Long) {
                sb.append(o.toString());
            } else if (o instanceof Double || o instanceof Float) {
                double d = ((Number) o).doubleValue();
                sb.append(Double.isNaN(d) || Double.isInfinite(d) ? "0" : String.valueOf(d));
            } else {
                escape(o.toString(), sb);
            }
        }

        private static void escape(String s, StringBuilder sb) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '"':  sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n");  break;
                    case '\r': sb.append("\\r");  break;
                    case '\t': sb.append("\\t");  break;
                    default:
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                }
            }
            sb.append('"');
        }
    }
}
