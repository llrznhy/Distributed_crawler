// CrawlRequest.java
package com.example.crawler.model;

public class CrawlRequest {
    private String url;
    private int depth;
    private long timestamp;

    public CrawlRequest(String url, int depth) {
        this.url = url;
        this.depth = depth;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and setters
    public String getUrl() { return url; }
    public int getDepth() { return depth; }
    public long getTimestamp() { return timestamp; }
}