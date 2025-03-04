package com.example.crawler.distributed.downloader;

import com.example.crawler.model.PageContent;
import java.io.Serializable;

public class DownloadResult implements Serializable {
    private final String url;
    private final PageContent content;
    private final boolean success;
    private final String error;
    private final long downloadTime;
    private final String nodeId;

    private DownloadResult(Builder builder) {
        this.url = builder.url;
        this.content = builder.content;
        this.success = builder.success;
        this.error = builder.error;
        this.downloadTime = builder.downloadTime;
        this.nodeId = builder.nodeId;
    }

    // Getters
    public String getUrl() { return url; }
    public PageContent getContent() { return content; }
    public boolean isSuccess() { return success; }
    public String getError() { return error; }
    public long getDownloadTime() { return downloadTime; }
    public String getNodeId() { return nodeId; }

    public static class Builder {
        private String url;
        private PageContent content;
        private boolean success;
        private String error;
        private long downloadTime;
        private String nodeId;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder content(PageContent content) {
            this.content = content;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder downloadTime(long downloadTime) {
            this.downloadTime = downloadTime;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public DownloadResult build() {
            return new DownloadResult(this);
        }
    }
}