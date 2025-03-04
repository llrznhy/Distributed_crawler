package com.example.crawler.core;

import com.example.crawler.model.CrawlResult;
import com.example.crawler.model.PageContent;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 解析器类
 * 对应基础要求：
 * g. 数据解析模块：对爬取的网页数据进行清洗、文本分词等
 * - 提取页面标题
 * - 提取页面正文
 * - 提取页面链接
 * - 提取页面元数据
 * - 清理HTML标签
 */
public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    // URL验证的正则表达式
    private static final Pattern URL_PATTERN = Pattern.compile(
            "^(https?://)[a-zA-Z0-9\\-._~:/?#\\[\\]@!$&'()*+,;=]+$"
    );

    // 要排除的文件扩展名
    private static final Set<String> EXCLUDED_EXTENSIONS = Set.of(
            ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".pdf",
            ".zip", ".rar", ".doc", ".docx", ".xls", ".xlsx"
    );

    // 配置选项
    private final boolean extractImages;
    private final boolean extractMetadata;
    private final int maxLinkCount;

    public Parser() {
        this(true, true, 1000);  // 默认配置
    }

    public Parser(boolean extractImages, boolean extractMetadata, int maxLinkCount) {
        this.extractImages = extractImages;
        this.extractMetadata = extractMetadata;
        this.maxLinkCount = maxLinkCount;
    }

    public CrawlResult parse(PageContent pageContent) {
        if (pageContent == null || pageContent.getContent() == null) {
            return null;
        }

        try {
            Document doc = Jsoup.parse(pageContent.getContent(), pageContent.getUrl());
            CrawlResult result = CrawlResult.builder()
                    .url(pageContent.getUrl())
                    .statusCode(pageContent.getStatusCode())
                    .title(doc.title())
                    .build();

            // 提取meta标签信息
            if (extractMetadata) {
                extractMetadata(doc, result);
            }

            // 提取正文内容
            extractContent(doc, result);

            // 提取链接
            extractLinks(doc, result);

            // 提取图片
            if (extractImages) {
                extractImages(doc, result);
            }

            return result;
        } catch (Exception e) {
            logger.error("Error parsing content from {}: {}",
                    pageContent.getUrl(), e.getMessage());
            return null;
        }
    }

    private void extractMetadata(Document doc, CrawlResult result) {
        // 提取标准meta标签
        Elements metaTags = doc.select("meta");
        for (Element meta : metaTags) {
            String name = meta.attr("name");
            String content = meta.attr("content");
            if (name.isEmpty()) {
                name = meta.attr("property"); // 处理OpenGraph标签
            }
            if (!name.isEmpty() && !content.isEmpty()) {
                result.addMetadata(name, content);
            }
        }

        // 提取OpenGraph特定标签
        Elements ogTags = doc.select("meta[property^=og:]");
        for (Element og : ogTags) {
            String property = og.attr("property");
            String content = og.attr("content");
            if (!property.isEmpty() && !content.isEmpty()) {
                result.addMetadata("og:" + property, content);
            }
        }
    }

//    private void extractContent(Document doc, CrawlResult result) {
//        Element body = doc.body();
//        if (body != null) {
//            // 移除不需要的元素
//            body.select("script, style, iframe, nav, footer, .ads, .comments").remove();
//
//            // 提取主要文本内容
//            String mainContent = body.text();
//
//            // 清理文本内容
//            mainContent = mainContent.replaceAll("\\s+", " ").trim();
//            result.setContent(mainContent);
//
//            // 尝试提取文章主要内容
//            Element article = doc.select("article, .article, .post, .content, main").first();
//            if (article != null) {
//                result.addMetadata("main_content", article.text());
//            }
//
//            // 添加文本分词逻辑
//            try {
//                List<String> tokens = tokenizeText(mainContent);
//                result.addMetadata("tokenized_content", String.join(" ", tokens));
//            } catch (IOException e) {
//                logger.error("Error tokenizing content from {}: {}", result.getUrl(), e.getMessage());
//            }
//        }
//    }
//
//    // 新增辅助方法：使用 Lucene 进行文本分词
//    private List<String> tokenizeText(String text) throws IOException {
//        List<String> tokens = new ArrayList<>();
//        Analyzer analyzer = new StandardAnalyzer(); // 可以使用 SmartChineseAnalyzer 针对中文
//        try (TokenStream tokenStream = analyzer.tokenStream("content", text)) {
//            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
//            tokenStream.reset();
//            while (tokenStream.incrementToken()) {
//                tokens.add(charTermAttribute.toString());
//            }
//            tokenStream.end();
//        }
//        return tokens;
//    }

    private void extractContent(Document doc, CrawlResult result) {
        Element body = doc.body();
        if (body != null) {
            // 移除不需要的元素
            body.select("script, style, iframe, nav, footer, .ads, .comments").remove();

            // 提取主要文本内容
            String mainContent = body.text();

            // 清理文本内容
            mainContent = mainContent.replaceAll("\\s+", " ").trim();
            result.setContent(mainContent);

            // 尝试提取文章主要内容
            Element article = doc.select("article, .article, .post, .content, main").first();
            if (article != null) {
                result.addMetadata("main_content", article.text());
            }
        }
    }

    private void extractLinks(Document doc, CrawlResult result) {
        Elements links = doc.select("a[href]");
        int count = 0;
        for (Element link : links) {
            if (count >= maxLinkCount) break;

            String href = link.attr("abs:href").trim();
            if (isValidUrl(href)) {
                result.addLink(href);
                count++;

                // 添加链接文本作为元数据
                String linkText = link.text().trim();
                if (!linkText.isEmpty()) {
                    result.addMetadata("link_text_" + href, linkText);
                }
            }
        }
    }

    private void extractImages(Document doc, CrawlResult result) {
        Elements images = doc.select("img[src]");
        for (Element img : images) {
            String src = img.attr("abs:src");
            if (isValidUrl(src)) {
                result.addMetadata("image", src);
                // 添加图片的alt文本
                String alt = img.attr("alt");
                if (!alt.isEmpty()) {
                    result.addMetadata("image_alt_" + src, alt);
                }
            }
        }
    }

    private boolean isValidUrl(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }

        // 检查URL格式
        if (!URL_PATTERN.matcher(url).matches()) {
            return false;
        }

        // 检查是否为排除的文件类型
        String lowerUrl = url.toLowerCase();
        for (String ext : EXCLUDED_EXTENSIONS) {
            if (lowerUrl.endsWith(ext)) {
                return false;
            }
        }

        // 排除非HTTP/HTTPS链接
        return url.startsWith("http://") || url.startsWith("https://");
    }
}