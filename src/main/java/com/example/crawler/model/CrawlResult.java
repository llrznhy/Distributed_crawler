package com.example.crawler.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.*;
import java.time.Instant;

public class CrawlResult {
    private String url;
    private String title;
    private String content;
    private Map<String, String> metadata;
    private Set<String> links;
    private int statusCode;
    private long crawlTime;
    private int depth;
    private String contentType;
    private long contentLength;

    // 构造函数
    public CrawlResult() {
        this.metadata = new HashMap<>();
        this.links = new HashSet<>();
        this.crawlTime = System.currentTimeMillis();
    }

    // 创建构建器模式的静态方法
    public static CrawlResult.Builder builder() {
        return new CrawlResult.Builder();
    }

    // Getters
    public String getUrl() { return url; }
    public String getTitle() { return title; }
    public String getContent() { return content; }
    public Map<String, String> getMetadata() { return Collections.unmodifiableMap(metadata); }
    public Set<String> getLinks() { return Collections.unmodifiableSet(links); }
    public int getStatusCode() { return statusCode; }
    public long getCrawlTime() { return crawlTime; }
    public int getDepth() { return depth; }
    public String getContentType() { return contentType; }
    public long getContentLength() { return contentLength; }

    // Setters
    public void setUrl(String url) { this.url = url; }
    public void setTitle(String title) { this.title = title; }
    public void setContent(String content) {
        this.content = content;
        this.contentLength = content != null ? content.length() : 0;
    }
    public void setStatusCode(int statusCode) { this.statusCode = statusCode; }
    public void setDepth(int depth) { this.depth = depth; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    // 元数据操作方法
    public void addMetadata(String key, String value) {
        if (key != null && value != null) {
            this.metadata.put(key, value);
        }
    }

    public void addMetadata(Map<String, String> metadata) {
        if (metadata != null) {
            this.metadata.putAll(metadata);
        }
    }

    // 链接操作方法
    public void addLink(String link) {
        if (link != null && !link.isEmpty()) {
            this.links.add(link);
        }
    }

    public void addLinks(Collection<String> links) {
        if (links != null) {
            links.forEach(this::addLink);
        }
    }

    // 便利方法
    @JsonIgnore
    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300;
    }

    @JsonIgnore
    public boolean hasContent() {
        return content != null && !content.isEmpty();
    }

    @JsonIgnore
    public Instant getCrawlTimeInstant() {
        return Instant.ofEpochMilli(crawlTime);
    }

    // 构建器类
    public static class Builder {
        private final CrawlResult result;

        private Builder() {
            result = new CrawlResult();
        }

        public Builder url(String url) {
            result.setUrl(url);
            return this;
        }

        public Builder title(String title) {
            result.setTitle(title);
            return this;
        }

        public Builder content(String content) {
            result.setContent(content);
            return this;
        }

        public Builder statusCode(int statusCode) {
            result.setStatusCode(statusCode);
            return this;
        }

        public Builder depth(int depth) {
            result.setDepth(depth);
            return this;
        }

        public Builder contentType(String contentType) {
            result.setContentType(contentType);
            return this;
        }

        public Builder addMetadata(String key, String value) {
            result.addMetadata(key, value);
            return this;
        }

        public Builder addLink(String link) {
            result.addLink(link);
            return this;
        }

        public CrawlResult build() {
            return result;
        }
    }

    @Override
    public String toString() {
        return "CrawlResult{" +
                "url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", statusCode=" + statusCode +
                ", contentLength=" + contentLength +
                ", linksCount=" + links.size() +
                ", crawlTime=" + getCrawlTimeInstant() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrawlResult that = (CrawlResult) o;
        return Objects.equals(url, that.url) &&
                Objects.equals(crawlTime, that.crawlTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, crawlTime);
    }
}