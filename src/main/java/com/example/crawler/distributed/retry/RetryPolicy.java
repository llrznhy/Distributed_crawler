package com.example.crawler.distributed.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryPolicy {
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);

    private final int maxRetries;
    private final long retryInterval;

    public RetryPolicy(int maxRetries, long retryInterval) {
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
    }

    public boolean shouldRetry(FailedUrl failedUrl) {
        boolean should = failedUrl.getRetryCount() < maxRetries &&
                System.currentTimeMillis() - failedUrl.getLastAttempt() > retryInterval;

        if (should) {
            logger.debug("URL {} will be retried. Attempt {}/{}",
                    failedUrl.getUrl(), failedUrl.getRetryCount() + 1, maxRetries);
        } else {
            logger.debug("URL {} will not be retried. Max retries reached or too soon",
                    failedUrl.getUrl());
        }

        return should;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}