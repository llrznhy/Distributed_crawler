package com.example.crawler.core;

import com.example.crawler.model.CrawlRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.net.URL;
import java.util.regex.Pattern;
// 在 UrlManager 类顶部添加必要的导入
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.regex.Pattern;


/**
 * URL管理器
 * 对应基础要求：
 * a. 种子URL管理模块：通过优先级队列实现种子URL管理
 * b. 待爬取URL管理：维护待爬取的URL队列
 * c. URL分发模块：实现URL的分发策略
 * d. URL分发策略：通过优先级和深度实现多种分发策略
 */

public abstract class UrlManager {
    private static final Logger logger = LoggerFactory.getLogger(UrlManager.class);

    // URL优先级队列
    private final PriorityBlockingQueue<UrlSeed> priorityQueue;
    // 已访问的URL集合
    private final Set<String> visitedUrls;
    // URL过滤规则
    private final List<Pattern> urlFilters;
    // 最大深度限制
    private final int maxDepth;

    public abstract void close();

    public static class UrlSeed implements Comparable<UrlSeed> {
        // 将 private 改为 package-private
        String url;
        int depth;
        int priority;
        long timestamp;

        public UrlSeed(String url, int depth, int priority) {
            this.url = url;
            this.depth = depth;
            this.priority = priority;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public int compareTo(UrlSeed other) {
            int result = Integer.compare(other.priority, this.priority);
            if (result == 0) {
                result = Integer.compare(this.depth, other.depth);
            }
            return result;
        }
    }

    // 修改构造函数
    public UrlManager(int maxDepth, int queueSize) {
        this.maxDepth = maxDepth;
        this.priorityQueue = new PriorityBlockingQueue<>(queueSize);
        this.visitedUrls = Collections.synchronizedSet(new HashSet<>());
        this.urlFilters = new ArrayList<>();
        addDefaultFilters();
    }

    // 添加分发策略接口
    public interface DistributionStrategy {
        void distribute(UrlSeed seed);
    }

    // 添加按域名分发策略
    public class DomainBasedStrategy implements DistributionStrategy {
        private final Map<String, Queue<UrlSeed>> domainQueues = new HashMap<>();

        @Override
        public void distribute(UrlSeed seed) {
            String domain = getDomain(seed.url);
            domainQueues.computeIfAbsent(domain,
                    k -> new LinkedBlockingQueue<>()).offer(seed);
        }

        private String getDomain(String url) {
            try {
                return new URL(url).getHost();
            } catch (MalformedURLException e) {
                return url;
            }
        }
    }

    // 修改字段定义
    private Map<String, Queue<UrlSeed>> domainQueues = new HashMap<>();

    private void addDefaultFilters() {
        // 排除常见的二进制文件、图片等
        addUrlFilter(".*\\.(jpg|jpeg|png|gif|bmp|zip|rar|exe|pdf|doc|docx)$");
        // 排除常见的无用页面
        addUrlFilter(".*(logout|login|signin|signup|register).*");
    }

    public void addUrlFilter(String regex) {
        urlFilters.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }

    public void addUrl(String url, int depth) {
        addUrl(url, depth, 1); // 默认优先级为1
    }

    public void addUrl(String url, int depth, int priority) {
        try {
            if (!isValidUrl(url) || depth > maxDepth) {
                return;
            }

            // 规范化URL
            url = normalizeUrl(url);

            if (!visitedUrls.contains(url) && !isFiltered(url)) {
                priorityQueue.offer(new UrlSeed(url, depth, priority));
                logger.debug("Added URL to queue: {} (depth: {}, priority: {})",
                        url, depth, priority);
            }
        } catch (Exception e) {
            logger.error("Error adding URL: {}", url, e);
        }
    }

    public UrlSeed getNextUrl() {
        try {
            UrlSeed seed = priorityQueue.poll(1, TimeUnit.SECONDS);
            if (seed != null) {
                visitedUrls.add(seed.url);
                logger.debug("Retrieved URL from queue: {}", seed.url);
            }
            return seed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public CrawlRequest getNext() {
        UrlSeed seed = getNextUrl();
        if (seed == null) {
            return null;
        }
        return new CrawlRequest(seed.url, seed.depth);
    }

    private boolean isValidUrl(String urlString) {
        try {
            new URL(urlString);
            return urlString.startsWith("http://") || urlString.startsWith("https://");
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isFiltered(String url) {
        return urlFilters.stream().anyMatch(pattern -> pattern.matcher(url).matches());
    }

    private String normalizeUrl(String url) {
        // 移除URL中的锚点
        int anchorIndex = url.indexOf('#');
        if (anchorIndex > 0) {
            url = url.substring(0, anchorIndex);
        }

        // 移除末尾的斜杠
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        return url;
    }

    public boolean hasMore() {
        return !priorityQueue.isEmpty();
    }

    public int getQueueSize() {
        return priorityQueue.size();
    }

    public int getVisitedCount() {
        return visitedUrls.size();
    }

    // 获取URL统计信息
    public Map<String, Integer> getStatistics() {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("queueSize", getQueueSize());
        stats.put("visitedCount", getVisitedCount());
        return stats;
    }
}