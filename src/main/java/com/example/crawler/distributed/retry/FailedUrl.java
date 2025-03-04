package com.example.crawler.distributed.retry;

import java.util.concurrent.atomic.AtomicInteger;

public class FailedUrl {
    private final String url;
    private final AtomicInteger retryCount;
    private volatile long lastAttempt;
    private volatile String lastError;
    private final int depth;
    private final int priority;

    public FailedUrl(String url, int depth, int priority) {
        this.url = url;
        this.depth = depth;
        this.priority = priority;
        this.retryCount = new AtomicInteger(0);
        this.lastAttempt = System.currentTimeMillis();
    }

    public String getUrl() {
        return url;
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public int incrementRetryCount() {
        return retryCount.incrementAndGet();
    }

    public long getLastAttempt() {
        return lastAttempt;
    }

    public void updateLastAttempt() {
        this.lastAttempt = System.currentTimeMillis();
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
        updateLastAttempt();
    }

    public int getDepth() {
        return depth;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "FailedUrl{" +
                "url='" + url + '\'' +
                ", retryCount=" + retryCount +
                ", lastAttempt=" + lastAttempt +
                ", depth=" + depth +
                ", priority=" + priority +
                '}';
    }
}