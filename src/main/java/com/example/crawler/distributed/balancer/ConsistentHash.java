package com.example.crawler.distributed.balancer;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 一致性哈希实现类
 * 用于在分布式系统中实现负载均衡
 */
public class ConsistentHash<T> {
    // 虚拟节点数量
    private final int virtualNodes;
    // 使用跳表实现的有序Map，保证线程安全和高效的范围查询
    private final SortedMap<Integer, T> circle = new ConcurrentSkipListMap<>();

    public ConsistentHash(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    /**
     * 添加节点
     * @param node 实际节点
     */
    public void addNode(T node) {
        if (node == null) {
            return;
        }

        // 为每个实际节点创建虚拟节点
        for (int i = 0; i < virtualNodes; i++) {
            // 虚拟节点的hash值 = 实际节点hashCode + 虚拟节点编号
            String virtualNodeName = node.toString() + "#" + i;
            int hash = getHash(virtualNodeName);
            circle.put(hash, node);
        }
    }

    /**
     * 移除节点
     * @param node 实际节点
     */
    public void removeNode(T node) {
        if (node == null) {
            return;
        }

        for (int i = 0; i < virtualNodes; i++) {
            String virtualNodeName = node.toString() + "#" + i;
            int hash = getHash(virtualNodeName);
            circle.remove(hash);
        }
    }

    /**
     * 获取数据所在的节点
     * @param key 数据键
     * @return 节点实例
     */
    public T getNode(String key) {
        if (circle.isEmpty()) {
            return null;
        }

        int hash = getHash(key);

        // 如果没有找到对应的虚拟节点，则返回第一个虚拟节点
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }

        return circle.get(hash);
    }

    /**
     * 更新节点列表
     * @param nodes 新的节点列表
     */
    public void updateNodes(Collection<T> nodes) {
        circle.clear();
        if (nodes != null) {
            for (T node : nodes) {
                addNode(node);
            }
        }
    }

    /**
     * 获取哈希值
     * 使用 FNV1_32_HASH 算法计算 hash 值
     */
    private int getHash(String key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash ^ key.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash);
    }

    /**
     * 获取当前节点数量
     */
    public int getNodeCount() {
        return circle.size() / virtualNodes;
    }
}