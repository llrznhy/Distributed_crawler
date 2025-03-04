package com.example.crawler.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 爬虫监控类
 * 对应基础要求：
 * k. 爬虫进程(或节点)监控：监控爬虫运行的状态信息
 */
public class CrawlerMonitor {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerMonitor.class);

    // 监控指标
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger processedPages = new AtomicInteger(0);
    private final AtomicInteger failedPages = new AtomicInteger(0);
    private final AtomicInteger successPages = new AtomicInteger(0);
    private final Map<String, Integer> domainStats = new ConcurrentHashMap<>();
    private final long startTime;

    public CrawlerMonitor() {
        this.startTime = System.currentTimeMillis();
    }

    public void incrementActiveThreads() {
        activeThreads.incrementAndGet();
    }

    public void decrementActiveThreads() {
        activeThreads.decrementAndGet();
    }

    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("activeThreads", activeThreads.get());
        stats.put("processedPages", processedPages.get());
        stats.put("successPages", successPages.get());
        stats.put("failedPages", failedPages.get());
        stats.put("runningTimeSeconds", (System.currentTimeMillis() - startTime) / 1000);
        return stats;
    }

    public void recordPageProcessed(String url, boolean success) {
        processedPages.incrementAndGet();
        if (success) {
            successPages.incrementAndGet();
        } else {
            failedPages.incrementAndGet();
        }
    }

    public int getProcessedPages() {
        return processedPages.get();
    }

    public void logStatistics() {
        Map<String, Object> stats = getStatistics();
        logger.info("=== Crawler Statistics ===");
        logger.info("Active Threads: {}", stats.get("activeThreads"));
        logger.info("Total Processed: {}", stats.get("processedPages"));
        logger.info("Success: {}", stats.get("successPages"));
        logger.info("Failed: {}", stats.get("failedPages"));
        logger.info("Running Time: {} seconds", stats.get("runningTimeSeconds"));
    }
}