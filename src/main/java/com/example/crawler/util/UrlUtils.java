// UrlUtils.java
package com.example.crawler.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

public class UrlUtils {
    private static final Pattern URL_PATTERN = Pattern.compile(
            "^(https?://)[a-zA-Z0-9\\-._~:/?#\\[\\]@!$&'()*+,;=]+$"
    );

    public static boolean isValidUrl(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }

        if (!URL_PATTERN.matcher(url).matches()) {
            return false;
        }

        try {
            new URL(url);
            return true;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    public static String normalizeUrl(String url) {
        if (url == null) return null;

        // 移除URL中的fragment
        int fragmentIndex = url.indexOf('#');
        if (fragmentIndex > 0) {
            url = url.substring(0, fragmentIndex);
        }

        // 确保URL以斜杠结尾
        if (!url.endsWith("/")) {
            url += "/";
        }

        return url;
    }
}
