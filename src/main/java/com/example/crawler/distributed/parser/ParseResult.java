package com.example.crawler.distributed.parser;

import com.example.crawler.model.CrawlResult;
import java.io.Serializable;

public class ParseResult implements Serializable {
    private final CrawlResult result;
    private final String sourceNodeId;
    private final long parseTime;
    private final boolean success;
    private final String error;

    private ParseResult(Builder builder) {
        this.result = builder.result;
        this.sourceNodeId = builder.sourceNodeId;
        this.parseTime = builder.parseTime;
        this.success = builder.success;
        this.error = builder.error;
    }

    // Getters
    public CrawlResult getResult() { return result; }
    public String getSourceNodeId() { return sourceNodeId; }
    public long getParseTime() { return parseTime; }
    public boolean isSuccess() { return success; }
    public String getError() { return error; }


    public static class Builder {
        private CrawlResult result;
        private String sourceNodeId;
        private long parseTime;
        private boolean success;
        private String error;

        public Builder result(CrawlResult result) {
            this.result = result;
            return this;
        }

        public Builder sourceNodeId(String sourceNodeId) {
            this.sourceNodeId = sourceNodeId;
            return this;
        }

        public Builder parseTime(long parseTime) {
            this.parseTime = parseTime;
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

        public ParseResult build() {
            return new ParseResult(this);
        }
    }
}