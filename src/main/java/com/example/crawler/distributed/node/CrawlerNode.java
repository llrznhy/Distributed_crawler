package com.example.crawler.distributed.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 爬虫节点类
 * 管理单个爬虫节点的状态和操作
 */
public class CrawlerNode {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerNode.class);

    private final String nodeId;
    private final AtomicReference<NodeStatus> status;
    private final AtomicLong lastHeartbeat;

    // 统计计数器
    private final AtomicLong processedUrlCount;
    private final AtomicLong failedUrlCount;
    private final AtomicLong activeThreads;
    private final AtomicLong queueSize;

    public CrawlerNode(String nodeId) {
        this.nodeId = nodeId;
        this.status = new AtomicReference<>(createInitialStatus());
        this.lastHeartbeat = new AtomicLong(System.currentTimeMillis());
        this.processedUrlCount = new AtomicLong(0);
        this.failedUrlCount = new AtomicLong(0);
        this.activeThreads = new AtomicLong(0);
        this.queueSize = new AtomicLong(0);
    }

    /**
     * 创建初始状态
     */
    private NodeStatus createInitialStatus() {
        return NodeStatus.builder(nodeId)
                .state(NodeStatus.NodeState.STARTING)
                .build();
    }

    /**
     * 更新节点状态
     */
    public void updateStatus(NodeStatus newStatus) {
        status.set(newStatus);
        lastHeartbeat.set(System.currentTimeMillis());
        logger.debug("Node {} status updated: {}", nodeId, newStatus);
    }

    /**
     * 更新心跳时间
     */
    public void heartbeat() {
        lastHeartbeat.set(System.currentTimeMillis());
        logger.debug("Node {} heartbeat received", nodeId);
    }

    /**
     * 检查节点是否活跃
     */
    public boolean isActive() {
        long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeat.get();
        return timeSinceLastHeartbeat < 30000 && // 30秒超时
                status.get().getState() == NodeStatus.NodeState.RUNNING;
    }

    /**
     * 更新统计信息
     */
    public void incrementProcessedUrls() {
        processedUrlCount.incrementAndGet();
    }

    public void incrementFailedUrls() {
        failedUrlCount.incrementAndGet();
    }

    public void setActiveThreads(long count) {
        activeThreads.set(count);
    }

    public void setQueueSize(long size) {
        queueSize.set(size);
    }

    /**
     * 获取当前状态快照
     */
    public NodeStatus getCurrentStatus() {
        return NodeStatus.builder(nodeId)
                .state(status.get().getState())
                .activeThreads((int) activeThreads.get())
                .queueSize((int) queueSize.get())
                .processedUrls(processedUrlCount.get())
                .failedUrls(failedUrlCount.get())
                .cpuUsage(getCpuUsage())
                .memoryUsage(getMemoryUsage())
                .build();
    }

    /**
     * 获取CPU使用率
     */
    private double getCpuUsage() {
        // 这里可以使用JMX或其他方式获取实际的CPU使用率
        return java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                .getSystemLoadAverage();
    }

    /**
     * 获取内存使用率
     */
    private double getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        return (double) (totalMemory - freeMemory) / totalMemory;
    }

    // Getters
    public String getNodeId() {
        return nodeId;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat.get();
    }

    public long getProcessedUrlCount() {
        return processedUrlCount.get();
    }

    public long getFailedUrlCount() {
        return failedUrlCount.get();
    }

    @Override
    public String toString() {
        return "CrawlerNode{" +
                "nodeId='" + nodeId + '\'' +
                ", status=" + status.get() +
                ", lastHeartbeat=" + lastHeartbeat.get() +
                ", active=" + isActive() +
                '}';
    }
}