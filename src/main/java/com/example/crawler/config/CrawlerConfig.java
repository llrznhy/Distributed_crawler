// CrawlerConfig.java
package com.example.crawler.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CrawlerConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = CrawlerConfig.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IllegalStateException("Unable to find config.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading configuration", e);
        }
    }

    // 获取配置项，提供默认值
    public static String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        try {
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        try {
            return value != null ? Long.parseLong(value) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    // 常用配置项的getter方法
    public static String getUserAgent() {
        return getString("crawler.user.agent",
                "Mozilla/5.0 (compatible; MyCrawler/1.0; +http://example.com/bot)");
    }

    public static int getMaxDepth() {
        return getInt("crawler.max.depth", 3);
    }

    public static int getMaxPages() {
        return getInt("crawler.max.pages", 1000);
    }

    public static int getCrawlDelay() {
        return getInt("crawler.delay.ms", 1000);
    }
}