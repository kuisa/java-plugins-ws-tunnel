package com.example.essentialsx;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KomariAgent -- 最小可用 Komari 监控上报客户端(纯 JDK,无第三方依赖)
 * 协议:Komari Agent protocol v2(JSON-RPC 2.0 over HTTP POST)
 *   POST {host}/api/clients/v2/rpc?token={token}
 *   method: agent.basicInfo / agent.report
 *
 * 在 EssentialsX.onEnable() 里调用:
 *   KomariAgent.start(KOMARI_HOST, KOMARI_TOKEN);
 *
 * 容器环境下不依赖 /proc/stat,/proc/meminfo,/proc/net/dev:
 *   - CPU:  com.sun.management.OperatingSystemMXBean
 *   - 内存: cgroup v2 /sys/fs/cgroup/memory.*,回退 Runtime.getRuntime()
 *   - 磁盘: File.getTotalSpace()/getUsableSpace()
 *   - 网络: /sys/class/net/ * /statistics/ * / (排除 lo)
 *   - 进程数: cgroup pids.current
 *   - uptime: /proc/uptime 回退 JVM 启动时间差
 */
public final class KomariAgent {

    private KomariAgent() { }

    // ================= 可调项 =================
    /** 实时数据上报间隔(秒),游戏面板建议 >= 5 */
    private static final int REPORT_INTERVAL_SEC = 10;
    /** 基础信息上报间隔(分钟) */
    private static final int INFO_INTERVAL_MIN = 5;
    /** 统计哪个挂载点的磁盘(容器内可设 /home/container) */
    private static final String DISK_PATH = "/";
    /** 打印每次上报的 HTTP 响应(排错时开,正常关闭) */
    private static final boolean DEBUG = true;

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static volatile Thread worker = null;
    private static volatile String endpoint = "";
    private static volatile String token = "";

    // CPU 差分状态(首次读取返回 0,第二次开始有值)
    private static long prevCpuTotal = -1;
    private static long prevCpuIdle = -1;
    // 网络差分状态
    private static long prevNetUp = -1;
    private static long prevNetDown = -1;
    private static long prevNetAt = -1;

    // =========================================================
    //  传输:JSON-RPC 2.0 over HTTP POST
    // =========================================================

    /** 发一条 JSON-RPC;失败抛异常,由调用方吞掉并打日志. */
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
                        + URLEncoder.encode(token, StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .header("User-Agent", "komari-java-agent-min/1.0")
                .POST(HttpRequest.BodyPublishers.ofString(Json.toJson(env), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new Exception("HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

    // =========================================================
    //  采集:不依赖 /proc
    // =========================================================

    /** CPU 使用率(0-100).通过反射调用 OperatingSystemMXBean.getSystemCpuLoad(),避免引用 sun 内部类. */
    private static double cpuUsage() {
        try {
            Object mx = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            java.lang.reflect.Method m = mx.getClass().getMethod("getSystemCpuLoad");
            Object v = m.invoke(mx);
            if (v instanceof Double) {
                double load = (Double) v;
                if (load >= 0.0 && load <= 1.0) {
                    return Math.round(load * 100.0 * 100.0) / 100.0;
                }
            }
        } catch (Exception ignored) { }
        return 0.0;
    }

    /** 内存信息(字节):优先 cgroup v2,回退 cgroup v1,最后回退 JVM Runtime. */
    private static Map<String, Long> meminfoBytes() {
        Map<String, Long> m = new LinkedHashMap<>();
        long total = 0, used = 0;

        // cgroup v2
        try {
            Path cur = Paths.get("/sys/fs/cgroup/memory.current");
            Path max = Paths.get("/sys/fs/cgroup/memory.max");
            if (Files.isReadable(cur) && Files.isReadable(max)) {
                String cs = Files.readString(cur).trim();
                String ms = Files.readString(max).trim();
                long curVal = Long.parseLong(cs);
                long maxVal = "max".equals(ms) ? Long.MAX_VALUE : Long.parseLong(ms);
                total = maxVal;
                used = Math.max(0, curVal - Math.max(0, maxVal - curVal)); // 近似 used
                // memory.current 是总使用量(包括页缓存),这里做个保守估算
                if (total > 0) {
                    m.put("MemTotal", total);
                    m.put("MemAvailable", Math.max(0, total - curVal));
                    m.put("MemFree", 0L);
                    m.put("Buffers", 0L);
                    m.put("Cached", 0L);
                    m.put("SwapTotal", 0L);
                    m.put("SwapFree", 0L);
                    return m;
                }
            }
        } catch (Exception ignored) { }

        // cgroup v1
        try {
            Path limit = Paths.get("/sys/fs/cgroup/memory/memory.limit_in_bytes");
            Path usage = Paths.get("/sys/fs/cgroup/memory/memory.usage_in_bytes");
            if (Files.isReadable(limit) && Files.isReadable(usage)) {
                long lim = Long.parseLong(Files.readString(limit).trim());
                long use = Long.parseLong(Files.readString(usage).trim());
                if (lim < Long.MAX_VALUE) {
                    total = lim;
                    used = use;
                    m.put("MemTotal", total);
                    m.put("MemAvailable", Math.max(0, total - use));
                    m.put("MemFree", 0L);
                    m.put("Buffers", 0L);
                    m.put("Cached", 0L);
                    m.put("SwapTotal", 0L);
                    m.put("SwapFree", 0L);
                    return m;
                }
            }
        } catch (Exception ignored) { }

        // /proc/meminfo 兜底(容器常可读,取 MemTotal)
        try {
            Path meminfo = Paths.get("/proc/meminfo");
            if (Files.isReadable(meminfo)) {
                String content = Files.readString(meminfo);
                long mt = 0, ma = 0;
                for (String line : content.split("\n")) {
                    line = line.trim();
                    if (line.startsWith("MemTotal:")) {
                        mt = parseMemKb(line);
                    } else if (line.startsWith("MemAvailable:")) {
                        ma = parseMemKb(line);
                    }
                }
                if (mt > 0) {
                    total = mt * 1024;
                    m.put("MemTotal", total);
                    m.put("MemAvailable", ma * 1024);
                    m.put("MemFree", Math.max(0, mt - ma) * 1024);
                    m.put("Buffers", 0L);
                    m.put("Cached", 0L);
                    m.put("SwapTotal", 0L);
                    m.put("SwapFree", 0L);
                    return m;
                }
            }
        } catch (Exception ignored) { }

        // 最后回退 JVM Runtime
        long jvmMax = Runtime.getRuntime().maxMemory();
        long jvmFree = Runtime.getRuntime().freeMemory();
        long jvmTotal = Runtime.getRuntime().totalMemory();
        if (jvmMax > 0) {
            total = jvmMax;
            used = jvmTotal - jvmFree;
        } else {
            total = jvmTotal;
            used = Math.max(0, jvmTotal - jvmFree);
        }
        m.put("MemTotal", total);
        m.put("MemAvailable", Math.max(0, total - used));
        m.put("MemFree", Math.max(0, jvmFree));
        m.put("Buffers", 0L);
        m.put("Cached", 0L);
        m.put("SwapTotal", 0L);
        m.put("SwapFree", 0L);
        return m;
    }

    /** 磁盘信息(字节). */
    private static long[] diskTotalUsed() {
        try {
            java.io.File f = new java.io.File(DISK_PATH);
            long total = f.getTotalSpace();
            long usable = f.getUsableSpace();
            return new long[]{total, Math.max(0, total - usable)};
        } catch (Exception e) {
            return new long[]{0, 0};
        }
    }

    /** 网络收发字节 (累计,单位字节):/sys/class/net/ * /statistics/ 排除 lo. */
    private static long[] netCumulative() {
        long up = 0, down = 0;
        try {
            Path dir = Paths.get("/sys/class/net");
            if (Files.isDirectory(dir)) {
                for (Path entry : Files.newDirectoryStream(dir)) {
                    String name = entry.getFileName().toString();
                    if ("lo".equals(name)) continue;
                    Path rx = entry.resolve("statistics/rx_bytes");
                    Path tx = entry.resolve("statistics/tx_bytes");
                    if (Files.isReadable(rx) && Files.isReadable(tx)) {
                        try {
                            down += Long.parseLong(Files.readString(rx).trim());
                            up += Long.parseLong(Files.readString(tx).trim());
                        } catch (Exception ignored) { }
                    }
                }
            }
        } catch (Exception ignored) { }
        return new long[]{up, down};
    }

    /** 系统运行秒数:/proc/uptime 回退 JVM 启动时间差. */
    private static long uptimeSeconds() {
        try {
            String s = Files.readString(Paths.get("/proc/uptime")).trim();
            String[] parts = s.split("\\s+");
            if (parts.length > 0) {
                return (long) Double.parseDouble(parts[0]);
            }
        } catch (Exception ignored) { }
        // 回退:(当前时间 - JVM 启动时间) / 1000
        return Math.max(0, (System.currentTimeMillis() - java.lang.management.ManagementFactory
                .getRuntimeMXBean().getStartTime()) / 1000);
    }

    /** 进程数:cgroup pids.current. */
    private static int processCount() {
        try {
            Path p = Paths.get("/sys/fs/cgroup/pids.current");
            if (Files.isReadable(p)) {
                return Integer.parseInt(Files.readString(p).trim());
            }
        } catch (Exception ignored) { }
        return 0;
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
        long mTotal = mem.getOrDefault("MemTotal", 0L);
        long avail = mem.containsKey("MemAvailable")
                ? mem.get("MemAvailable")
                : mem.getOrDefault("MemFree", 0L)
                + mem.getOrDefault("Buffers", 0L)
                + mem.getOrDefault("Cached", 0L);
        long sTotal = mem.getOrDefault("SwapTotal", 0L);
        long sFree = mem.getOrDefault("SwapFree", 0L);
        long[] d = diskTotalUsed();

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("cpu", Map.of("usage", cpuUsage()));
        report.put("ram", Map.of("total", mTotal, "used", Math.max(0, mTotal - avail)));
        report.put("swap", Map.of("total", sTotal, "used", Math.max(0, sTotal - sFree)));
        report.put("load", Map.of("load1", 0.0, "load5", 0.0, "load15", 0.0)); // JDK 无原生 loadavg,留 0
        report.put("disk", Map.of("total", d[0], "used", d[1]));
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
        m.put("ipv4", getLocalIpv4());
        m.put("ipv6", "");
        m.put("mem_total", mem.getOrDefault("MemTotal", 0L));
        m.put("os", (System.getProperty("os.name", "") + " " + System.getProperty("os.version", "")).trim());
        m.put("kernel_version", System.getProperty("os.version", ""));
        m.put("swap_total", mem.getOrDefault("SwapTotal", 0L));
        m.put("version", "java-agent-min/1.0");
        m.put("virtualization", "container");
        return m;
    }

    private static String cpuName() {
        try {
            return System.getProperty("os.arch", "unknown");
        } catch (Exception e) {
            return "unknown";
        }
    }

    /** 本机 IPv4:遍历网卡地址,排除回环和 link-local,优先非 127.x/169.254.x. */
    private static String getLocalIpv4() {
        try {
            java.util.Enumeration<java.net.NetworkInterface> ifs =
                    java.net.NetworkInterface.getNetworkInterfaces();
            while (ifs.hasMoreElements()) {
                java.net.NetworkInterface ni = ifs.nextElement();
                if (ni == null || !ni.isUp() || ni.isLoopback()) continue;
                java.util.Enumeration<java.net.InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    java.net.InetAddress addr = addrs.nextElement();
                    if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()
                            && !addr.isSiteLocalAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
            // 回退:尝试解析 localhost 非回环
            java.net.InetAddress[] all = java.net.InetAddress.getAllByName(
                    java.net.InetAddress.getLocalHost().getHostName());
            for (java.net.InetAddress a : all) {
                if (a instanceof java.net.Inet4Address && !a.isLoopbackAddress()) return a.getHostAddress();
            }
        } catch (Exception ignored) { }
        return "";
    }

    private static long nz(Long v) { return v == null ? 0L : v; }

    /** 解析 /proc/meminfo 行中的 kB 数值. */
    private static long parseMemKb(String line) {
        try {
            String num = line.split(":")[1].trim();
            num = num.replaceAll("[^0-9].*", "").trim();
            return Long.parseLong(num);
        } catch (Exception ignored) { return 0L; }
    }

    // =========================================================
    //  主循环
    // =========================================================

    private static void sendBasicInfo() {
        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("basicInfo", buildBasicInfo());
            rpc("agent.basicInfo", params);
        } catch (Exception e) {
        }
    }

    /** 幂等启动上报线程.host 或 token 为空则跳过. */
    public static boolean start(String host, String tokenValue) {
        if (host == null || host.trim().isEmpty()) return false;
        if (tokenValue == null || tokenValue.trim().isEmpty()) {
            System.out.println("[komari] 未设置 KOMARI_TOKEN,跳过监控上报(不影响游戏运行)");
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

    /** 停止上报(onDisable 调用). */
    public static void stop() {
        if (running.compareAndSet(true, false)) {
            Thread t = worker;
            if (t != null) t.interrupt();
        }
    }

    // =========================================================
    /** 极简 JSON 序列化 (避免为上报引入第三方 json 依赖). */
    // =========================================================

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
            } else if (o instanceof Number || o instanceof Boolean) {
                sb.append(o);
            } else {
                escape(String.valueOf(o), sb);
            }
        }

        private static void escape(String s, StringBuilder sb) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\b': sb.append("\\b"); break;
                    case '\f': sb.append("\\f"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default:
                        if (c < 0x20) {
                            sb.append(String.format("\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                }
            }
            sb.append('"');
        }
    }
}
