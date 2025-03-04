//package com.example.crawler.distributed.node;
//
//import com.fasterxml.jackson.annotation.JsonProperty;
//import java.io.Serializable;
//import java.time.Instant;
//
//public class NodeStatus implements Serializable {
//    @JsonProperty("nodeId")
//    private final String nodeId;
//
//    @JsonProperty("state")
//    private final NodeState state;
//
//    @JsonProperty("activeThreads")
//    private final int activeThreads;
//
//    @JsonProperty("queueSize")
//    private final int queueSize;
//
//    @JsonProperty("processedUrls")
//    private final long processedUrls;
//
//    @JsonProperty("failedUrls")
//    private final long failedUrls;
//
//    @JsonProperty("cpuUsage")
//    private final double cpuUsage;
//
//    @JsonProperty("memoryUsage")
//    private final double memoryUsage;
//
//    @JsonProperty("lastUpdateTime")
//    private final long lastUpdateTime;
//
//    public enum NodeState {
//        STARTING, RUNNING, PAUSED, STOPPED, ERROR
//    }
//
//    public NodeStatus(String nodeId, NodeState state, int activeThreads, int queueSize,
//                      long processedUrls, long failedUrls, double cpuUsage, double memoryUsage) {
//        this.nodeId = nodeId;
//        this.state = state;
//        this.activeThreads = activeThreads;
//        this.queueSize = queueSize;
//        this.processedUrls = processedUrls;
//        this.failedUrls = failedUrls;
//        this.cpuUsage = cpuUsage;
//        this.memoryUsage = memoryUsage;
//        this.lastUpdateTime = System.currentTimeMillis();
//    }
//
//    // Getters 保持不变
//    public String getNodeId() { return nodeId; }
//    public NodeState getState() { return state; }
//    public int getActiveThreads() { return activeThreads; }
//    public int getQueueSize() { return queueSize; }
//    public long getProcessedUrls() { return processedUrls; }
//    public long getFailedUrls() { return failedUrls; }
//    public double getCpuUsage() { return cpuUsage; }
//    public double getMemoryUsage() { return memoryUsage; }
//    public long getLastUpdateTime() { return lastUpdateTime; }
//    public Instant getLastUpdateTimeAsInstant() { return Instant.ofEpochMilli(lastUpdateTime); }
//
//    public boolean isActive() {
//        return state == NodeState.RUNNING && System.currentTimeMillis() - lastUpdateTime < 30000;
//    }
//
//    public static Builder builder(String nodeId) { return new Builder(nodeId); }
//
//    public static class Builder {
//        private final String nodeId;
//        private NodeState state = NodeState.STARTING;
//        private int activeThreads = 0;
//        private int queueSize = 0;
//        private long processedUrls = 0;
//        private long failedUrls = 0;
//        private double cpuUsage = 0.0;
//        private double memoryUsage = 0.0;
//
//        private Builder(String nodeId) { this.nodeId = nodeId; }
//
//        public Builder state(NodeState state) { this.state = state; return this; }
//        public Builder activeThreads(int activeThreads) { this.activeThreads = activeThreads; return this; }
//        public Builder queueSize(int queueSize) { this.queueSize = queueSize; return this; }
//        public Builder processedUrls(long processedUrls) { this.processedUrls = processedUrls; return this; }
//        public Builder failedUrls(long failedUrls) { this.failedUrls = failedUrls; return this; }
//        public Builder cpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; return this; }
//        public Builder memoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; return this; }
//        public NodeStatus build() {
//            return new NodeStatus(nodeId, state, activeThreads, queueSize, processedUrls, failedUrls, cpuUsage, memoryUsage);
//        }
//    }
//
//    @Override
//    public String toString() {
//        return "NodeStatus{" +
//                "nodeId='" + nodeId + '\'' +
//                ", state=" + state +
//                ", activeThreads=" + activeThreads +
//                ", queueSize=" + queueSize +
//                ", processedUrls=" + processedUrls +
//                ", failedUrls=" + failedUrls +
//                ", cpuUsage=" + cpuUsage +
//                ", memoryUsage=" + memoryUsage +
//                ", lastUpdateTime=" + getLastUpdateTimeAsInstant() +
//                '}';
//    }
//}


package com.example.crawler.distributed.node;

import java.io.Serializable;
import java.time.Instant;

public class NodeStatus implements Serializable {
    private final String nodeId;
    private final NodeState state;
    private final int activeThreads;
    private final int queueSize;
    private final long processedUrls;
    private final long failedUrls;
    private final double cpuUsage;
    private final double memoryUsage;
    private final long lastUpdateTime;

    public enum NodeState {
        STARTING, RUNNING, PAUSED, STOPPED, ERROR
    }

    public NodeStatus(String nodeId, NodeState state, int activeThreads, int queueSize,
                      long processedUrls, long failedUrls, double cpuUsage, double memoryUsage) {
        this.nodeId = nodeId;
        this.state = state;
        this.activeThreads = activeThreads;
        this.queueSize = queueSize;
        this.processedUrls = processedUrls;
        this.failedUrls = failedUrls;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public NodeState getState() { return state; }
    public int getActiveThreads() { return activeThreads; }
    public int getQueueSize() { return queueSize; }
    public long getProcessedUrls() { return processedUrls; }
    public long getFailedUrls() { return failedUrls; }
    public double getCpuUsage() { return cpuUsage; }
    public double getMemoryUsage() { return memoryUsage; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    public Instant getLastUpdateTimeAsInstant() { return Instant.ofEpochMilli(lastUpdateTime); }

    public boolean isActive() {
        return state == NodeState.RUNNING && System.currentTimeMillis() - lastUpdateTime < 30000;
    }

    public static Builder builder(String nodeId) { return new Builder(nodeId); }

    public static class Builder {
        private final String nodeId;
        private NodeState state = NodeState.STARTING;
        private int activeThreads = 0;
        private int queueSize = 0;
        private long processedUrls = 0;
        private long failedUrls = 0;
        private double cpuUsage = 0.0;
        private double memoryUsage = 0.0;

        private Builder(String nodeId) { this.nodeId = nodeId; }

        public Builder state(NodeState state) { this.state = state; return this; }
        public Builder activeThreads(int activeThreads) { this.activeThreads = activeThreads; return this; }
        public Builder queueSize(int queueSize) { this.queueSize = queueSize; return this; }
        public Builder processedUrls(long processedUrls) { this.processedUrls = processedUrls; return this; }
        public Builder failedUrls(long failedUrls) { this.failedUrls = failedUrls; return this; }
        public Builder cpuUsage(double cpuUsage) { this.cpuUsage = cpuUsage; return this; }
        public Builder memoryUsage(double memoryUsage) { this.memoryUsage = memoryUsage; return this; }
        public NodeStatus build() {
            return new NodeStatus(nodeId, state, activeThreads, queueSize, processedUrls, failedUrls, cpuUsage, memoryUsage);
        }
    }

    @Override
    public String toString() {
        return "NodeStatus(nodeId='" + nodeId + "', state=" + state +
                ", activeThreads=" + activeThreads +
                ", queueSize=" + queueSize +
                ", processedUrls=" + processedUrls +
                ", failedUrls=" + failedUrls +
                ", cpuUsage=" + cpuUsage +
                ", memoryUsage=" + memoryUsage +
                ", lastUpdateTime=" + getLastUpdateTimeAsInstant() + ")";
    }
}