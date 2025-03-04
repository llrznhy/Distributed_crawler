package com.example.crawler.distributed.queue;

import java.io.Serializable;
import java.time.Instant;

/**
 * 消息实体类
 * 用于在分布式系统中传递消息
 */
public class Message implements Serializable {
    private final String topic;      // 消息主题
    private final Object content;    // 消息内容
    private final String sourceId;   // 消息源ID
    private final long timestamp;    // 消息时间戳
    private final String messageId;  // 消息唯一标识

    /**
     * 构造函数
     * @param topic 消息主题
     * @param content 消息内容
     * @param sourceId 消息源ID
     */
    public Message(String topic, Object content, String sourceId) {
        this.topic = topic;
        this.content = content;
        this.sourceId = sourceId;
        this.timestamp = System.currentTimeMillis();
        this.messageId = generateMessageId();
    }

    /**
     * 生成消息唯一标识
     */
    private String generateMessageId() {
        return sourceId + "_" + timestamp + "_" + Math.abs(content.hashCode());
    }

    // Getters
    public String getTopic() {
        return topic;
    }

    public Object getContent() {
        return content;
    }

    public String getSourceId() {
        return sourceId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getMessageId() {
        return messageId;
    }

    public Instant getTimestampAsInstant() {
        return Instant.ofEpochMilli(timestamp);
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", topic='" + topic + '\'' +
                ", sourceId='" + sourceId + '\'' +
                ", timestamp=" + getTimestampAsInstant() +
                ", content=" + content +
                '}';
    }
}