package com.example.crawler.distributed.queue;

/**
 * 消息处理器接口
 * 用于处理从消息队列接收到的消息
 */
public interface MessageHandler {
    /**
     * 处理接收到的消息
     * @param message 接收到的消息
     */
    void handle(Message message);
}