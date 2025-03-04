package com.example.crawler.distributed.downloader;

import java.io.Serializable;

public class DownloadTask implements Serializable {
    private final String url;
    private final int depth;
    private final String sourceNodeId;
    private final long timestamp;

    public DownloadTask(String url, int depth, String sourceNodeId) {
        this.url = url;
        this.depth = depth;
        this.sourceNodeId = sourceNodeId;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters
    public String getUrl() { return url; }
    public int getDepth() { return depth; }
    public String getSourceNodeId() { return sourceNodeId; }
    public long getTimestamp() { return timestamp; }
}