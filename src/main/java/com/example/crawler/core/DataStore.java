package com.example.crawler.core;

import com.example.crawler.model.CrawlResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据存储类
 * 对应基础要求：
 * h. 数据存储模块：将解析好的数据按照一定格式进行存储并构建索引
 * i. 数据查询模块：可以进行所需数据的查询
 * - 支持JSON格式存储
 * - 支持文本格式存储
 * - 实现文件系统存储
 * - 构建简单的文件索引
 */

public class DataStore {
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    private final ObjectMapper objectMapper;
    private final String baseDir;
    private final String format;
//    private final String storagePath;

    public DataStore(String baseDir, String format) {
        this.baseDir = baseDir;
        this.format = format;
        this.objectMapper = new ObjectMapper();
        createStorageDirectory();
    }


//    private void createStorageDirectory() {
//        try {
//            Path dir = Paths.get(baseDir);
//            if (!Files.exists(dir)) {
//                Files.createDirectories(dir);
//                logger.info("Created storage directory: {}", baseDir);
//            }
//        } catch (IOException e) {
//            logger.error("Failed to create storage directory: {}", e.getMessage());
//        }
//    }

    private void createStorageDirectory() {
        try {
            Path dir = Paths.get(baseDir);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                logger.info("Created storage directory: {}", dir.toAbsolutePath());
            }
        } catch (IOException e) {
            logger.error("Failed to create storage directory: {}", e.getMessage());
        }
    }


    /**
     * 生成文件名
     * 将URL转换为有效的文件名，移除非法字符并添加时间戳
     */
    private String generateFileName(String url) {
        try {
            // 1. 从URL中获取路径部分
            String path = new URL(url).getPath();

            // 2. 移除开头的斜杠
            if (path.startsWith("/")) {
                path = path.substring(1);
            }

            // 3. 如果路径为空，使用域名
            if (path.isEmpty()) {
                path = new URL(url).getHost();
            }

            // 4. 替换非法字符
            String safeName = path.replaceAll("[^a-zA-Z0-9.-]", "_");

            // 5. 添加时间戳
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

            // 6. 组合文件名
            return safeName + "_" + timestamp + "." + format.toLowerCase();

        } catch (MalformedURLException e) {
            // 如果URL解析失败，使用URL的哈希值作为文件名
            String hash = String.valueOf(url.hashCode());
            return "page_" + hash + "_" + System.currentTimeMillis() + "." + format.toLowerCase();
        }
    }

    public void store(CrawlResult result, String filename) {
        if (result == null) {
            return;
        }

        try {
            String fileName = generateFileName(result.getUrl());
            Path filePath = Paths.get(baseDir, fileName);

            switch (format.toLowerCase()) {
                case "json":
                    storeAsJson(result, filePath);
                    break;
                case "txt":
                    storeAsText(result, filePath);
                    break;
                default:
                    logger.warn("Unsupported format: {}, defaulting to JSON", format);
                    storeAsJson(result, filePath);
            }
        } catch (Exception e) {
            logger.error("Failed to store result for {}: {}",
                    result.getUrl(), e.getMessage());
        }
    }

//    private void storeAsJson(CrawlResult result, Path filePath) throws IOException {
//        objectMapper.writeValue(filePath.toFile(), result);
//        logger.info("Stored JSON result to {}", filePath);
//    }

    private void storeAsJson(CrawlResult result, Path filePath) throws IOException {
        objectMapper.writeValue(filePath.toFile(), result);
        logger.info("Successfully stored JSON to: {}", filePath.toAbsolutePath());
    }


    private String formatTimestamp(long timestamp) {
        try {
            // 将毫秒时间戳转换为 LocalDateTime
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp),
                    ZoneId.systemDefault()
            );
            // 格式化为易读的日期时间字符串
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (Exception e) {
            // 如果转换失败，返回时间戳的字符串形式
            return String.valueOf(timestamp);
        }
    }

    // 在 DataStore 类中添加以下方法

    /**
     * 根据关键词搜索爬取结果
     * @param keyword 搜索关键词
     * @return 包含关键词的爬取结果列表
     */
    public List<CrawlResult> searchByKeyword(String keyword) {
        List<CrawlResult> results = new ArrayList<>();
        try {
            Files.walk(Paths.get(baseDir))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(format.toLowerCase()))
                    .forEach(path -> {
                        try {
                            CrawlResult result;
                            if (format.equalsIgnoreCase("json")) {
                                result = objectMapper.readValue(path.toFile(), CrawlResult.class);
                            } else {
                                result = readTextFile(path);
                            }

                            if (matchesKeyword(result, keyword)) {
                                results.add(result);
                            }
                        } catch (IOException e) {
                            logger.error("Error reading file: " + path, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Error searching files", e);
        }
        return results;
    }

    /**
     * 检查爬取结果是否匹配关键词
     */
    private boolean matchesKeyword(CrawlResult result, String keyword) {
        if (result == null || keyword == null) return false;

        keyword = keyword.toLowerCase();

        // 检查标题
        if (result.getTitle() != null &&
                result.getTitle().toLowerCase().contains(keyword)) {
            return true;
        }

        // 检查内容
        if (result.getContent() != null &&
                result.getContent().toLowerCase().contains(keyword)) {
            return true;
        }

        // 检查元数据
        String finalKeyword = keyword;
        return result.getMetadata().values().stream()
                .anyMatch(value -> value.toLowerCase().contains(finalKeyword));
    }

    /**
     * 从文本文件读取爬取结果
     */
    private CrawlResult readTextFile(Path path) throws IOException {
        CrawlResult result = new CrawlResult();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("URL: ")) {
                    result.setUrl(line.substring(5));
                } else if (line.startsWith("Title: ")) {
                    result.setTitle(line.substring(7));
                } else {
                    content.append(line).append("\n");
                }
            }
            result.setContent(content.toString());
        }
        return result;
    }

    private void storeAsText(CrawlResult result, Path filePath) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath.toFile(), StandardCharsets.UTF_8))) {
            writer.println("URL: " + result.getUrl());
            writer.println("Title: " + result.getTitle());
            writer.println("Crawl Time: " + formatTimestamp(result.getCrawlTime()));
            writer.println("Status Code: " + result.getStatusCode());
            writer.println("\nMetadata:");

            // 写入元数据
            for (Map.Entry<String, String> entry : result.getMetadata().entrySet()) {
                writer.println("  " + entry.getKey() + ": " + entry.getValue());
            }

            // 写入链接
            writer.println("\nLinks:");
            for (String link : result.getLinks()) {
                writer.println("  " + link);
            }

            // 写入内容
            writer.println("\nContent:");
            writer.println(result.getContent());
        }
        logger.info("Stored text result to {}", filePath);
    }
}