// PageContent.java
package com.example.crawler.model;

public class PageContent {
    private String url;
    private String content;
    private int statusCode;
    private String contentType;
    private String html = "";

    public PageContent(String url, String content, int statusCode, String contentType) {
        this.url = url;
        this.content = content;
        this.statusCode = statusCode;
        this.contentType = contentType;
        this.html = html != null ? html : "";
    }

    // Getters
    public String getUrl() { return url; }
    public String getContent() { return content; }
    public int getStatusCode() { return statusCode; }
    public String getContentType() { return contentType; }
    public String getHtml() {
        return html;
    }
    // 添加 isEmpty 方法，检查 html 是否为空
    public boolean isEmpty() {
        return html.isEmpty();
    }

}