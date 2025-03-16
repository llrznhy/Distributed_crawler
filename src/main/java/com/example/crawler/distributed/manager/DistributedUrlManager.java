//package com.example.crawler.distributed.manager;
//
//import com.example.crawler.config.CrawlerConfig;
//import com.example.crawler.core.UrlManager;
//import com.example.crawler.distributed.balancer.LoadBalancer;
//import com.example.crawler.distributed.node.CrawlerNode;
//import com.example.crawler.distributed.node.NodeStatus;
//import com.example.crawler.distributed.queue.Message;
//import com.example.crawler.distributed.queue.MessageQueue;
//import com.example.crawler.distributed.retry.FailedUrl;
//import com.example.crawler.distributed.retry.RetryPolicy;
//import com.example.crawler.model.CrawlRequest;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.*;
//
///**
// * 分布式URL管理器
// * 扩展原有的UrlManager，添加分布式功能
// */
//public class DistributedUrlManager extends UrlManager {
//    private static final Logger logger = LoggerFactory.getLogger(DistributedUrlManager.class);
//
//    private final String nodeId;
//    private final LoadBalancer loadBalancer;
//    private final MessageQueue messageQueue;
//    private final Map<String, CrawlerNode> nodes;
//    private final ScheduledExecutorService scheduler;
//    private final int batchSize;
//    private final long distributionInterval;
//
//    // 在类的字段声明部分添加
//    private final RetryPolicy retryPolicy;
//    private final Map<String, FailedUrl> failedUrls = new ConcurrentHashMap<>();
//
//    public DistributedUrlManager(String nodeId, int maxDepth, MessageQueue messageQueue,
//                                 int batchSize, long distributionInterval) {
//        super(maxDepth, 1000);
//        this.nodeId = nodeId;
//        this.loadBalancer = new LoadBalancer(10, 1000);
//        this.messageQueue = messageQueue;
//        this.nodes = new ConcurrentHashMap<>();
//        this.batchSize = batchSize;
//        this.distributionInterval = distributionInterval;
//        this.scheduler = Executors.newScheduledThreadPool(2);
//        // 初始化重试策略
//        this.retryPolicy = new RetryPolicy(
//                CrawlerConfig.getInt("url.retry.max", 3),
//                CrawlerConfig.getLong("url.retry.interval", 60000)
//        );
//
//        initializeDistributedSystem();
//    }
//
//    private void initializeDistributedSystem() {
//        // 注册消息处理器
//        messageQueue.subscribe("node_heartbeat", this::handleHeartbeat);
//        messageQueue.subscribe("url_batch", this::handleUrlBatch);
//
//        // 启动定时任务
//        scheduler.scheduleAtFixedRate(
//                this::distributeUrls,
//                distributionInterval,
//                distributionInterval,
//                TimeUnit.MILLISECONDS
//        );
//
//        scheduler.scheduleAtFixedRate(
//                this::checkNodes,
//                5000,
//                5000,
//                TimeUnit.MILLISECONDS
//        );
//
//        // 发送节点注册消息
//        sendNodeRegistration();
//    }
//
////    private void handleHeartbeat(Message message) {
////        try {
////            // 由于消息内容是 String，需要先转换成 NodeStatus 对象
////            if (message.getContent() instanceof String) {
////                // 如果是字符串，需要转换成 NodeStatus 对象
////                String content = (String) message.getContent();
////                // 这里可以添加转换逻辑
////                // 可以使用 Jackson 或其他 JSON 工具进行转换
////                ObjectMapper mapper = new ObjectMapper();
////                NodeStatus status = mapper.readValue(content, NodeStatus.class);
////                updateNodeStatus(status);
////            } else {
////                // 直接是 NodeStatus 对象的情况
////                NodeStatus status = (NodeStatus) message.getContent();
////                updateNodeStatus(status);
////            }
////        } catch (Exception e) {
////            logger.error("Error handling heartbeat: {}", e.getMessage());
////        }
////    }
////
////    private void updateNodeStatus(NodeStatus status) {
////        String sourceNodeId = status.getNodeId();
////        nodes.computeIfAbsent(sourceNodeId, id -> new CrawlerNode(id))
////                .updateStatus(status);
////        logger.debug("Received heartbeat from node: {}", sourceNodeId);
////    }
//
//    private void handleHeartbeat(Message message) {
//        logger.debug("Received heartbeat message: {}", message);
//        try {
//            if (message.getContent() instanceof String) {
//                String content = (String) message.getContent();
//                ObjectMapper mapper = new ObjectMapper();
//                NodeStatus status = mapper.readValue(content, NodeStatus.class);
//                updateNodeStatus(status);
//            } else if (message.getContent() instanceof NodeStatus) {
//                NodeStatus status = (NodeStatus) message.getContent();
//                updateNodeStatus(status);
//            } else {
//                logger.error("Unexpected heartbeat message content: {}", message.getContent());
//            }
//        } catch (Exception e) {
//            logger.error("Error handling heartbeat: {}", e.getMessage(), e);
//        }
//    }
//
//    private void updateNodeStatus(NodeStatus status) {
//        String sourceNodeId = status.getNodeId();
//        nodes.computeIfAbsent(sourceNodeId, id -> new CrawlerNode(id))
//                .updateStatus(status);
//        logger.debug("Updated status for node {}: {}", sourceNodeId, status);
//    }
//
//    private void handleUrlBatch(Message message) {
//        try {
//            UrlBatch batch = (UrlBatch) message.getContent();
//            if (!nodeId.equals(batch.getSourceNodeId())) {
//                for (String url : batch.getUrls()) {
//                    try {
//                        super.addUrl(url, batch.getDepth(), batch.getPriority());
//                    } catch (Exception e) {
//                        handleFailedUrl(url, batch.getDepth(), batch.getPriority(), e.getMessage());
//                    }
//                }
//                logger.debug("Processed URL batch: {}", batch.getBatchId());
//            }
//        } catch (Exception e) {
//            logger.error("Error handling URL batch: {}", e.getMessage());
//        }
//    }
//
//    // 添加处理失败URL的方法
//    private void handleFailedUrl(String url, int depth, int priority, String error) {
//        FailedUrl failedUrl = failedUrls.computeIfAbsent(
//                url,
//                k -> new FailedUrl(url, depth, priority)
//        );
//        failedUrl.setLastError(error);
//
//        if (retryPolicy.shouldRetry(failedUrl)) {
//            failedUrl.incrementRetryCount();
//
//            // 安排重试
//            scheduler.schedule(() -> {
//                try {
//                    super.addUrl(failedUrl.getUrl(), failedUrl.getDepth(), failedUrl.getPriority());
//                    failedUrls.remove(url);
//                    logger.info("Successfully retried URL: {}", url);
//                } catch (Exception e) {
//                    logger.error("Retry failed for URL: {}", url, e);
//                }
//            }, retryPolicy.getRetryInterval(), TimeUnit.MILLISECONDS);
//
//            logger.info("Scheduled retry {} for URL: {}",
//                    failedUrl.getRetryCount(), url);
//        } else {
//            logger.error("Permanently failed URL: {} after {} retries. Last error: {}",
//                    url, failedUrl.getRetryCount(), error);
//            failedUrls.remove(url);
//        }
//    }
//
//
//    @Override
//    public void addUrl(String url, int depth, int priority) {
//        if (nodes.size() <= 1) {
//            // 如果只有一个节点，直接使用本地队列
//            super.addUrl(url, depth, priority);
//            return;
//        }
//
//        // 确定URL应该由哪个节点处理
//        Set<String> activeNodeIds = getActiveNodeIds();
//        Map<String, List<String>> distribution =
//                loadBalancer.distributeUrls(Collections.singletonList(url), activeNodeIds);
//
//        distribution.forEach((targetNodeId, urls) -> {
//            if (urls.isEmpty()) return;
//
//            if (nodeId.equals(targetNodeId)) {
//                // 本节点处理
//                super.addUrl(url, depth, priority);
//            } else {
//                // 发送到目标节点
//                UrlBatch batch = new UrlBatch(nodeId, urls, depth, priority);
//                messageQueue.send("url_batch", batch);
//            }
//        });
//    }
//
//    private void distributeUrls() {
//        if (nodes.size() <= 1) return;
//
//        List<String> pendingUrls = new ArrayList<>();
//        while (pendingUrls.size() < batchSize && hasMore()) {
//            CrawlRequest request = super.getNext();
//            if (request != null) {
//                pendingUrls.add(request.getUrl());
//            }
//        }
//
//        if (!pendingUrls.isEmpty()) {
//            Set<String> activeNodeIds = getActiveNodeIds();
//            Map<String, List<String>> distribution =
//                    loadBalancer.distributeUrls(pendingUrls, activeNodeIds);
//
//            distribution.forEach((targetNodeId, urls) -> {
//                if (!urls.isEmpty() && !nodeId.equals(targetNodeId)) {
//                    UrlBatch batch = new UrlBatch(nodeId, urls, 0, 1);
//                    messageQueue.send("url_batch", batch);
//                }
//            });
//        }
//    }
//
//    private void checkNodes() {
//        // 清理不活跃的节点
//        nodes.entrySet().removeIf(entry -> !entry.getValue().isActive());
//
//        // 发送心跳
//        sendHeartbeat();
//    }
//
//    private Set<String> getActiveNodeIds() {
//        return new HashSet<>(nodes.keySet());
//    }
//
//    private void sendNodeRegistration() {
//        NodeStatus status = new NodeStatus(nodeId, NodeStatus.NodeState.RUNNING,
//                0, getQueueSize(), 0, 0, 0.0, 0.0);
//        messageQueue.send("node_registration", status);
//    }
//
//    private void sendHeartbeat() {
//        NodeStatus status = new NodeStatus(nodeId, NodeStatus.NodeState.RUNNING,
//                0, getQueueSize(), 0, 0, 0.0, 0.0);
//        messageQueue.send("node_heartbeat", status);
//    }
//
//    @Override
//    public void close() {
//        scheduler.shutdown();
//        try {
//            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
//                scheduler.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            scheduler.shutdownNow();
//            Thread.currentThread().interrupt();
//        }
//    }
//}

package com.example.crawler.distributed.manager;

import com.example.crawler.config.CrawlerConfig;
import com.example.crawler.core.UrlManager;
import com.example.crawler.distributed.balancer.LoadBalancer;
import com.example.crawler.distributed.node.CrawlerNode;
import com.example.crawler.distributed.node.NodeStatus;
import com.example.crawler.distributed.queue.Message;
import com.example.crawler.distributed.queue.MessageQueue;
import com.example.crawler.distributed.retry.FailedUrl;
import com.example.crawler.distributed.retry.RetryPolicy;
import com.example.crawler.model.CrawlRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class DistributedUrlManager extends UrlManager {
    private static final Logger logger = LoggerFactory.getLogger(DistributedUrlManager.class);

    private final String nodeId;
    private final LoadBalancer loadBalancer;
    private final MessageQueue messageQueue;
    private final Map<String, CrawlerNode> nodes;
    private final ScheduledExecutorService scheduler;
    private final int batchSize;
    private final long distributionInterval;

    private final RetryPolicy retryPolicy;
    private final Map<String, FailedUrl> failedUrls = new ConcurrentHashMap<>();
    private Set<String> urlSet = ConcurrentHashMap.newKeySet();
    private final Queue<CrawlRequest> urlQueue = new LinkedList<>();


    public DistributedUrlManager(String nodeId, int maxDepth, MessageQueue messageQueue,
                                 int batchSize, long distributionInterval) {
        super(maxDepth, 1000);
        this.nodeId = nodeId;
        this.loadBalancer = new LoadBalancer(10, 1000);
        this.messageQueue = messageQueue;
        this.nodes = new ConcurrentHashMap<>();
        this.batchSize = batchSize;
        this.distributionInterval = distributionInterval;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.retryPolicy = new RetryPolicy(
                CrawlerConfig.getInt("url.retry.max", 3),
                CrawlerConfig.getLong("url.retry.interval", 60000)
        );
        this.urlSet = new HashSet<>();

        initializeDistributedSystem();
    }

    private void initializeDistributedSystem() {
        messageQueue.subscribe("node_heartbeat", this::handleHeartbeat);
        messageQueue.subscribe("url_batch", this::handleUrlBatch);

        scheduler.scheduleAtFixedRate(
                this::distributeUrls,
                distributionInterval,
                distributionInterval,
                TimeUnit.MILLISECONDS
        );

        scheduler.scheduleAtFixedRate(
                this::checkNodes,
                5000,
                5000,
                TimeUnit.MILLISECONDS
        );

        sendNodeRegistration();
    }

    private void handleHeartbeat(Message message) {
        try {
            if (message.getContent() instanceof String) {
                String content = (String) message.getContent();
                // 解析 toString() 格式的心跳消息，例如 "NodeStatus(nodeId='node-1', state=RUNNING, ...)"
                NodeStatus status = parseNodeStatusString(content);
                updateNodeStatus(status);
            } else {
                logger.warn("Unexpected heartbeat message type: {}", message.getContent());
            }
        } catch (Exception e) {
            logger.error("Error handling heartbeat: {}", e.getMessage());
        }
    }

    private NodeStatus parseNodeStatusString(String statusString) {
        // 解析简单的字符串格式，例如 "NodeStatus(nodeId='node-1', state=RUNNING, ...)"
        String trimmed = statusString.replace("NodeStatus(", "").replace(")", "");
        String[] parts = trimmed.split(", ");

        String nodeId = null;
        NodeStatus.NodeState state = NodeStatus.NodeState.STARTING;
        int activeThreads = 0;
        int queueSize = 0;
        long processedUrls = 0;
        long failedUrls = 0;
        double cpuUsage = 0.0;
        double memoryUsage = 0.0;

        for (String part : parts) {
            String[] keyValue = part.split("=");
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim().replace("'", "");
                switch (key) {
                    case "nodeId":
                        nodeId = value;
                        break;
                    case "state":
                        state = NodeStatus.NodeState.valueOf(value);
                        break;
                    case "activeThreads":
                        activeThreads = Integer.parseInt(value);
                        break;
                    case "queueSize":
                        queueSize = Integer.parseInt(value);
                        break;
                    case "processedUrls":
                        processedUrls = Long.parseLong(value);
                        break;
                    case "failedUrls":
                        failedUrls = Long.parseLong(value);
                        break;
                    case "cpuUsage":
                        cpuUsage = Double.parseDouble(value);
                        break;
                    case "memoryUsage":
                        memoryUsage = Double.parseDouble(value);
                        break;
                }
            }
        }
        return new NodeStatus(nodeId, state, activeThreads, queueSize, processedUrls, failedUrls, cpuUsage, memoryUsage);
    }

    private void updateNodeStatus(NodeStatus status) {
        String sourceNodeId = status.getNodeId();
        nodes.computeIfAbsent(sourceNodeId, id -> new CrawlerNode(id))
                .updateStatus(status);
        logger.debug("Received heartbeat from node: {}", sourceNodeId);
    }

    private void handleUrlBatch(Message message) {
        try {
            UrlBatch batch = (UrlBatch) message.getContent();
            if (!nodeId.equals(batch.getSourceNodeId())) {
                for (String url : batch.getUrls()) {
                    try {
                        super.addUrl(url, batch.getDepth(), batch.getPriority());
                    } catch (Exception e) {
                        handleFailedUrl(url, batch.getDepth(), batch.getPriority(), e.getMessage());
                    }
                }
                logger.debug("Processed URL batch: {}", batch.getBatchId());
            }
        } catch (Exception e) {
            logger.error("Error handling URL batch: {}", e.getMessage());
        }
    }

    private void handleFailedUrl(String url, int depth, int priority, String error) {
        FailedUrl failedUrl = failedUrls.computeIfAbsent(
                url,
                k -> new FailedUrl(url, depth, priority)
        );
        failedUrl.setLastError(error);

        if (retryPolicy.shouldRetry(failedUrl)) {
            failedUrl.incrementRetryCount();

            scheduler.schedule(() -> {
                try {
                    super.addUrl(failedUrl.getUrl(), failedUrl.getDepth(), failedUrl.getPriority());
                    failedUrls.remove(url);
                    logger.info("Successfully retried URL: {}", url);
                } catch (Exception e) {
                    logger.error("Retry failed for URL: {}", url, e);
                }
            }, retryPolicy.getRetryInterval(), TimeUnit.MILLISECONDS);

            logger.info("Scheduled retry {} for URL: {}",
                    failedUrl.getRetryCount(), url);
        } else {
            logger.error("Permanently failed URL: {} after {} retries. Last error: {}",
                    url, failedUrl.getRetryCount(), error);
            failedUrls.remove(url);
        }
    }

    @Override
    public void addUrl(String url, int depth, int priority) {
        if (nodes.size() <= 1) {
            super.addUrl(url, depth, priority);
            return;
        }

        Set<String> activeNodeIds = getActiveNodeIds();
        Map<String, List<String>> distribution =
                loadBalancer.distributeUrls(Collections.singletonList(url), activeNodeIds);

        logger.info("Adding URL: {} with depth {} to queue", url, depth);
        logger.info("Active nodes: {}", activeNodeIds);
        logger.info("URL distribution result: {}", distribution);

        if (!urlSet.contains(url)) {
            urlSet.add(url);
            urlQueue.add(new CrawlRequest(url, depth));
        }


        distribution.forEach((targetNodeId, urls) -> {
            if (urls.isEmpty()) return;

            if (nodeId.equals(targetNodeId)) {
                super.addUrl(url, depth, priority);
            } else {
                UrlBatch batch = new UrlBatch(nodeId, urls, depth, priority);
                messageQueue.send("url_batch", batch);
            }
        });
    }

    private void distributeUrls() {
        if (nodes.size() <= 1) return;

        List<String> pendingUrls = new ArrayList<>();
        while (pendingUrls.size() < batchSize && hasMore()) {
            CrawlRequest request = super.getNext();
            if (request != null) {
                pendingUrls.add(request.getUrl());
            }
        }

        if (!pendingUrls.isEmpty()) {
            Set<String> activeNodeIds = getActiveNodeIds();
            Map<String, List<String>> distribution =
                    loadBalancer.distributeUrls(pendingUrls, activeNodeIds);

            distribution.forEach((targetNodeId, urls) -> {
                if (!urls.isEmpty() && !nodeId.equals(targetNodeId)) {
                    UrlBatch batch = new UrlBatch(nodeId, urls, 0, 1);
                    messageQueue.send("url_batch", batch);
                }
            });
        }
    }

    private void checkNodes() {
        nodes.entrySet().removeIf(entry -> !entry.getValue().isActive());
        sendHeartbeat();
    }

    private Set<String> getActiveNodeIds() {
        return new HashSet<>(nodes.keySet());
    }

    private void sendNodeRegistration() {
        NodeStatus status = new NodeStatus(nodeId, NodeStatus.NodeState.RUNNING,
                0, getQueueSize(), 0, 0, 0.0, 0.0);
        messageQueue.send("node_registration", status);
    }

    public boolean hasUrl(String url) {
        return urlSet.contains(url);
    }


    private void sendHeartbeat() {
        NodeStatus status = new NodeStatus(nodeId, NodeStatus.NodeState.RUNNING,
                0, getQueueSize(), 0, 0, 0.0, 0.0);
        messageQueue.send("node_heartbeat", status.toString()); // 使用 toString() 发送字符串格式
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}