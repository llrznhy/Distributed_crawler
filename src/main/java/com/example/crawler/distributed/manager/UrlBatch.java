package com.example.crawler.distributed.manager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * URL批次类
 * 用于在分布式系统中批量传输URL
 */
public class UrlBatch implements Serializable {
    private final String batchId;
    private final List<String> urls;
    private final int depth;
    private final int priority;
    private final long timestamp;
    private final String sourceNodeId;

    public UrlBatch(String sourceNodeId, List<String> urls, int depth, int priority) {
        this.sourceNodeId = sourceNodeId;
        this.urls = new ArrayList<>(urls);
        this.depth = depth;
        this.priority = priority;
        this.timestamp = System.currentTimeMillis();
        this.batchId = generateBatchId();
    }

    private String generateBatchId() {
        return sourceNodeId + "_" + timestamp + "_" + Math.abs(urls.hashCode());
    }

    // Getters
    public String getBatchId() { return batchId; }
    public List<String> getUrls() { return new ArrayList<>(urls); }
    public int getDepth() { return depth; }
    public int getPriority() { return priority; }
    public long getTimestamp() { return timestamp; }
    public String getSourceNodeId() { return sourceNodeId; }
    public int getSize() { return urls.size(); }

    @Override
    public String toString() {
        return "UrlBatch{" +
                "batchId='" + batchId + '\'' +
                ", sourceNodeId='" + sourceNodeId + '\'' +
                ", urlCount=" + urls.size() +
                ", depth=" + depth +
                ", priority=" + priority +
                '}';
    }
}