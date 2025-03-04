package com.example.crawler.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 代理池管理
 * 对应基础要求：
 * j. 随机IP代理库：反反爬虫
 */
public class ProxyPool {
    private static final Logger logger = LoggerFactory.getLogger(ProxyPool.class);

    private final Queue<Proxy> proxyQueue = new ConcurrentLinkedQueue<>();
    private final List<Proxy> failedProxies = new ArrayList<>();
    private static final int maxFailCount = 3;

    static class ProxyStatus {
        Proxy proxy;
        int failCount;
        long lastUsed;

        ProxyStatus(Proxy proxy) {
            this.proxy = proxy;
            this.failCount = 0;
            this.lastUsed = System.currentTimeMillis();
        }

        boolean isFailed() {
            return failCount >= maxFailCount;
        }
    }

    public void addProxy(String host, int port) {
        Proxy proxy = new Proxy(Proxy.Type.HTTP,
                new InetSocketAddress(host, port));
        proxyQueue.offer(proxy);
        logger.info("Added proxy: {}:{}", host, port);
    }

    public Proxy getProxy() {
        Proxy proxy;
        while ((proxy = proxyQueue.poll()) != null) {
            ProxyStatus status = findProxyStatus(proxy);
            if (status != null && !status.isFailed()) {
                proxyQueue.offer(proxy); // 将有效代理放回队列
                return proxy;
            } else {
                // 失效代理直接移除
                if (status != null && status.isFailed()) {
                    proxyQueue.remove(proxy);
                    failedProxies.add(proxy);
                    logger.warn("Skipped failed proxy {}:{}",
                            ((InetSocketAddress) proxy.address()).getHostName(),
                            ((InetSocketAddress) proxy.address()).getPort());
                }
            }
        }
        return null; // 如果没有可用代理，返回 null
    }

    // 辅助方法：查找代理状态
    private ProxyStatus findProxyStatus(Proxy proxy) {
        // 这里需要根据实际需求扩展逻辑，可能需要维护一个 Map<Proxy, ProxyStatus>
        // 由于当前 proxyQueue 是 Queue，无法直接查找，这里假设简单实现（实际需优化）
        for (Proxy p : proxyQueue) {
            if (p.equals(proxy)) {
                // 创建一个临时的 ProxyStatus（需优化为维护状态映射）
                return new ProxyStatus(proxy);
            }
        }
        for (Proxy p : failedProxies) {
            if (p.equals(proxy)) {
                return new ProxyStatus(proxy); // 返回失败状态
            }
        }
        return null; // 未找到
    }

    public void markProxySuccess(Proxy proxy) {
        // 查找对应的 ProxyStatus
        ProxyStatus status = findProxyStatus(proxy);
        if (status != null) {
            status.failCount = 0; // 重置失败计数
            status.lastUsed = System.currentTimeMillis(); // 更新最后使用时间
            logger.debug("Marked proxy {}:{} as successful",
                    ((InetSocketAddress) proxy.address()).getHostName(),
                    ((InetSocketAddress) proxy.address()).getPort());
        }
    }

    public void markProxyFailed(Proxy proxy) {
        ProxyStatus status = findProxyStatus(proxy);
        if (status != null) {
            status.failCount++; // 增加失败计数
            status.lastUsed = System.currentTimeMillis(); // 更新最后使用时间
            logger.warn("Marked proxy {}:{} as failed, failCount={}",
                    ((InetSocketAddress) proxy.address()).getHostName(),
                    ((InetSocketAddress) proxy.address()).getPort(),
                    status.failCount);

            // 如果失败次数达到上限，移除失效代理
            if (status.isFailed()) {
                proxyQueue.remove(proxy); // 从队列中移除
                failedProxies.add(proxy); // 记录到失败列表
                logger.error("Removed failed proxy {}:{} after {} failures",
                        ((InetSocketAddress) proxy.address()).getHostName(),
                        ((InetSocketAddress) proxy.address()).getPort(),
                        maxFailCount);
            }
        }
    }

    // 添加一批代理
    public void addProxies(List<String> proxyList) {
        for (String proxyStr : proxyList) {
            String[] parts = proxyStr.split(":");
            if (parts.length == 2) {
                try {
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    addProxy(host, port);
                } catch (NumberFormatException e) {
                    logger.error("Invalid proxy format: {}", proxyStr);
                }
            }
        }
    }

    // 获取代理池状态
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("activeProxies", proxyQueue.size());
        stats.put("failedProxies", failedProxies.size());
        return stats;
    }
}