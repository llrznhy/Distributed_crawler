package com.example.crawler.distributed.balancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 负载均衡器
 * 用于在分布式系统中实现URL的负载均衡分发
 */
public class LoadBalancer {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer.class);

    private final ConsistentHash<String> consistentHash;
    private final Map<String, Integer> nodeLoadCount;
    private final int virtualNodes;
    private final int maxLoadPerNode;

    public LoadBalancer(int virtualNodes, int maxLoadPerNode) {
        this.virtualNodes = virtualNodes;
        this.maxLoadPerNode = maxLoadPerNode;
        this.consistentHash = new ConsistentHash<>(virtualNodes);
        this.nodeLoadCount = new ConcurrentHashMap<>();
    }

    /**
     * 分发URL到各个节点
     * @param urls 要分发的URL列表
     * @param activeNodes 活跃节点列表
     * @return 分发结果，key为节点ID，value为分配到的URL列表
     */
    public Map<String, List<String>> distributeUrls(Collection<String> urls, Set<String> activeNodes) {
        if (urls == null || urls.isEmpty() || activeNodes == null || activeNodes.isEmpty()) {
            return Collections.emptyMap();
        }

        // 更新一致性哈希中的节点
        consistentHash.updateNodes(activeNodes);

        // 初始化结果Map
        Map<String, List<String>> distribution = new HashMap<>();
        activeNodes.forEach(node -> distribution.put(node, new ArrayList<>()));

        // 分发URL
        for (String url : urls) {
            String targetNode = getTargetNode(url, activeNodes);
            if (targetNode != null) {
                distribution.get(targetNode).add(url);
                incrementNodeLoad(targetNode);
            }
        }

        // 记录分发结果
        logDistribution(distribution);

        return distribution;
    }

    /**
     * 获取目标节点
     */
    private String getTargetNode(String url, Set<String> activeNodes) {
        // 首先使用一致性哈希选择节点
        String targetNode = consistentHash.getNode(url);

        // 如果节点负载过高，尝试选择其他节点
        if (isNodeOverloaded(targetNode)) {
            targetNode = findLeastLoadedNode(activeNodes);
        }

        return targetNode;
    }

    /**
     * 检查节点是否过载
     */
    private boolean isNodeOverloaded(String nodeId) {
        return nodeLoadCount.getOrDefault(nodeId, 0) >= maxLoadPerNode;
    }

    /**
     * 查找负载最小的节点
     */
    private String findLeastLoadedNode(Set<String> activeNodes) {
        return activeNodes.stream()
                .min(Comparator.comparingInt(node -> nodeLoadCount.getOrDefault(node, 0)))
                .orElse(null);
    }

    /**
     * 增加节点负载计数
     */
    private void incrementNodeLoad(String nodeId) {
        nodeLoadCount.merge(nodeId, 1, Integer::sum);
    }

    /**
     * 重置节点负载
     */
    public void resetNodeLoad(String nodeId) {
        nodeLoadCount.remove(nodeId);
    }

    /**
     * 记录分发结果
     */
    private void logDistribution(Map<String, List<String>> distribution) {
        distribution.forEach((node, urls) ->
                logger.debug("Node {}: assigned {} URLs", node, urls.size())
        );
    }

    /**
     * 获取节点当前负载
     */
    public int getNodeLoad(String nodeId) {
        return nodeLoadCount.getOrDefault(nodeId, 0);
    }

    /**
     * 获取所有节点的负载情况
     */
    public Map<String, Integer> getAllNodeLoads() {
        return new HashMap<>(nodeLoadCount);
    }
}