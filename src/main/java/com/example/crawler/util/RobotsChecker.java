// RobotsChecker.java
package com.example.crawler.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

public class RobotsChecker {
    private static final Logger logger = LoggerFactory.getLogger(RobotsChecker.class);
    private final ConcurrentHashMap<String, RobotsCache> robotsCache = new ConcurrentHashMap<>();

    public boolean isAllowed(String url) {
        try {
            URL urlObj = new URL(url);
            String host = urlObj.getHost();
            String robotsUrl = urlObj.getProtocol() + "://" + host + "/robots.txt";

            RobotsCache cache = robotsCache.computeIfAbsent(host,
                    k -> new RobotsCache(robotsUrl));

            return cache.isAllowed(url);
        } catch (Exception e) {
            logger.error("Error checking robots.txt for {}: {}", url, e.getMessage());
            return false;
        }
    }

    private static class RobotsCache {
        private final long timestamp;
        private final boolean allowAll;

        public RobotsCache(String robotsUrl) {
            this.timestamp = System.currentTimeMillis();
            this.allowAll = parseRobotsTxt(robotsUrl);
        }

        private boolean parseRobotsTxt(String robotsUrl) {
            // 简化版实现，实际应该解析robots.txt文件
            return true;
        }

        public boolean isAllowed(String url) {
            // 检查缓存是否过期（24小时）
            if (System.currentTimeMillis() - timestamp > 24 * 60 * 60 * 1000) {
                return false;
            }
            return allowAll;
        }
    }
}