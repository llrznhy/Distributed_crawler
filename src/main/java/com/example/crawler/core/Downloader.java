package com.example.crawler.core;

import com.example.crawler.model.PageContent;
import com.example.crawler.util.RobotsChecker;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.io.IOException;

public class Downloader {
    private static final Logger logger = LoggerFactory.getLogger(Downloader.class);

    private final RobotsChecker robotsChecker;
    private final RequestConfig requestConfig;
    private final String userAgent;
    private final ProxyPool proxyPool;
    private final DNSResolver dnsResolver;

    public Downloader(String userAgent, int connectTimeout, int readTimeout) {
        this.robotsChecker = new RobotsChecker();
        this.userAgent = userAgent;
        this.proxyPool = new ProxyPool();
        this.dnsResolver = new DNSResolver(300);

        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(readTimeout)
                .setConnectionRequestTimeout(connectTimeout)
                .build();

        // 禁用 cookie 警告
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        java.util.logging.Logger.getLogger("org.apache.http.client.protocol.ResponseProcessCookies")
                .setLevel(java.util.logging.Level.OFF);
    }

    public PageContent download(String url) {
        if (!robotsChecker.isAllowed(url)) {
            logger.info("URL not allowed by robots.txt: {}", url);
            return null;
        }

        try {
            // 解析 DNS
            URL urlObj = new URL(url);
            String ip = dnsResolver.resolve(urlObj.getHost());
            if (ip == null) {
                logger.error("Failed to resolve DNS for: {}", url);
                return null;
            }

            // 获取代理
            Proxy proxy = proxyPool.getProxy();
            HttpHost proxyHost = null;
            if (proxy != null) {
                InetSocketAddress addr = (InetSocketAddress) proxy.address();
                proxyHost = new HttpHost(addr.getHostName(), addr.getPort());
            }

            // 创建 HTTP 客户端
            RequestConfig config = RequestConfig.custom()
                    .setProxy(proxyHost)
                    .setConnectTimeout(requestConfig.getConnectTimeout())
                    .setSocketTimeout(requestConfig.getSocketTimeout())
                    .setConnectionRequestTimeout(requestConfig.getConnectionRequestTimeout())
                    .build();

            try (CloseableHttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(config)
                    .build()) {

                HttpGet request = new HttpGet(url);
                request.setHeader("User-Agent", userAgent);
                request.setConfig(config);

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode == 200) {
                        HttpEntity entity = response.getEntity();
                        String contentType = entity.getContentType().getValue();

                        if (contentType.contains("text/html")) {
                            String content = EntityUtils.toString(entity);
                            if (proxy != null) {
                                proxyPool.markProxySuccess(proxy);
                            }
                            return new PageContent(url, content, statusCode, contentType);
                        }
                    }

                    if (proxy != null) {
                        proxyPool.markProxyFailed(proxy);
                    }
                    logger.warn("Failed to download {}, status code: {}", url, statusCode);
                    return new PageContent(url, null, statusCode, null);
                }
            }
        } catch (Exception e) {
            logger.error("Error downloading {}: {}", url, e.getMessage());
            return null;
        }
    }

    public void close() {

    }
}