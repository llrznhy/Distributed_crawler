package com.example.crawler.distributed.queue;

/**
 * 消息队列接口
 * 定义消息队列的基本操作
 */
public interface MessageQueue {
    /**
     * 发送消息到指定主题
     * @param topic 消息主题
     * @param message 消息内容
     */
    void send(String topic, Object message);

    /**
     * 订阅指定主题的消息
     * @param topic 消息主题
     * @param handler 消息处理器
     */
    void subscribe(String topic, MessageHandler handler);

    /**
     * 关闭消息队列连接
     */
    void close();
}