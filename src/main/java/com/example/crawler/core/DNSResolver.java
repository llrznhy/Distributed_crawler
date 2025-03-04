package com.example.crawler.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DNS解析模块
 * 对应基础要求：
 * e. DNS解析模块：将URL地址转换为网站服务器对应的IP地址
 */
public class DNSResolver {
    private static final Logger logger = LoggerFactory.getLogger(DNSResolver.class);

    // DNS缓存，key是域名，value是DNSEntry对象
    private final Map<String, DNSEntry> dnsCache = new ConcurrentHashMap<>();
    private final int cacheDuration; // 缓存时间（秒）
    private final ScheduledExecutorService cleanupService;

    private static class DNSEntry {
        final String ip;
        final long expirationTime;

        DNSEntry(String ip, long expirationTime) {
            this.ip = ip;
            this.expirationTime = expirationTime;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
    }

    public DNSResolver(int cacheDuration) {
        this.cacheDuration = cacheDuration;
        this.cleanupService = Executors.newSingleThreadScheduledExecutor();
        startCleanupTask();
    }

    public String resolve(String hostname) {
        // 检查缓存
        DNSEntry entry = dnsCache.get(hostname);
        if (entry != null && !entry.isExpired()) {
            logger.debug("DNS cache hit for {}: {}", hostname, entry.ip);
            return entry.ip;
        }

        // 缓存未命中或已过期，进行DNS解析
        try {
            InetAddress address = InetAddress.getByName(hostname);
            String ip = address.getHostAddress();

            // 更新缓存
            long expirationTime = System.currentTimeMillis() + (cacheDuration * 1000L);
            dnsCache.put(hostname, new DNSEntry(ip, expirationTime));

            logger.debug("Resolved DNS for {}: {}", hostname, ip);
            return ip;
        } catch (UnknownHostException e) {
            logger.error("Failed to resolve DNS for {}: {}", hostname, e.getMessage());
            return null;
        }
    }

    private void startCleanupTask() {
        cleanupService.scheduleAtFixedRate(() -> {
            try {
                cleanup();
            } catch (Exception e) {
                logger.error("Error during DNS cache cleanup", e);
            }
        }, cacheDuration, cacheDuration, TimeUnit.SECONDS);
    }

    private void cleanup() {
        int beforeSize = dnsCache.size();
        dnsCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        int removedCount = beforeSize - dnsCache.size();
        if (removedCount > 0) {
            logger.debug("Cleaned up {} expired DNS entries", removedCount);
        }
    }

    public void shutdown() {
        cleanupService.shutdown();
        try {
            if (!cleanupService.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupService.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // 获取DNS缓存统计信息
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("cacheSize", dnsCache.size());
        stats.put("cacheDuration", cacheDuration);
        return stats;
    }
}