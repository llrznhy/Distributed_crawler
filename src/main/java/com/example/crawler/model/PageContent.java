// PageContent.java
package com.example.crawler.model;

public class PageContent {
    private String url;
    private String content;
    private int statusCode;
    private String contentType;

    public PageContent(String url, String content, int statusCode, String contentType) {
        this.url = url;
        this.content = content;
        this.statusCode = statusCode;
        this.contentType = contentType;
    }

    // Getters
    public String getUrl() { return url; }
    public String getContent() { return content; }
    public int getStatusCode() { return statusCode; }
    public String getContentType() { return contentType; }
}