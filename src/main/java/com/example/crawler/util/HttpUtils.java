// HttpUtils.java
package com.example.crawler.util;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.config.RequestConfig;

public class HttpUtils {
    public static CloseableHttpClient createHttpClient(int timeout) {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeout)
                .setSocketTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .build();

        return HttpClients.custom()
                .setDefaultRequestConfig(config)
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(20)
                .build();
    }
}