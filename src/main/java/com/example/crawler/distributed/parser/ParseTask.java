package com.example.crawler.distributed.parser;

import com.example.crawler.model.PageContent;
import java.io.Serializable;

public class ParseTask implements Serializable {
    private final PageContent content;
    private final int depth;
    private final String sourceNodeId;
    private final long timestamp;

    public ParseTask(PageContent content, int depth, String sourceNodeId) {
        this.content = content;
        this.depth = depth;
        this.sourceNodeId = sourceNodeId;
        this.timestamp = System.currentTimeMillis();
    }

    public PageContent getContent() { return content; }
    public int getDepth() { return depth; }
    public String getSourceNodeId() { return sourceNodeId; }
    public long getTimestamp() { return timestamp; }
}