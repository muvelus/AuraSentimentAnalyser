package com.lit.fire.flame;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SentimentAnalysis {

    private static final String DB_URL;
    private static final String USER;
    private static final String PASS;
    private static final String LLM_URL;
    private static final String PROMPT_MEDIA_MOVIE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE;
    private static final String PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE;
    private static final String PROMPT_MEDIA_CELEBRITY_TEMPLATE;
    private static final String PROMPT_POLITICS_POLITICIAN_TEMPLATE;
    private static final String PROMPT_POLITICS_CAMPAIGN_TEMPLATE;
    private static final String PROMPT_POLITICS_PARTY_TEMPLATE;

    static {
        Properties props = new Properties();
        try (InputStream input = SentimentAnalysis.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                throw new RuntimeException("application.properties not found in the classpath");
            }
            props.load(input);
            DB_URL = props.getProperty("db.url");
            USER = props.getProperty("db.user");
            PASS = props.getProperty("db.password");
            LLM_URL = props.getProperty("llm.url");
            PROMPT_MEDIA_MOVIE_TEMPLATE = props.getProperty("prompt.media.movie");
            PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE = props.getProperty("prompt.media.movie.invalid");
            PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE = props.getProperty("prompt.media.movie.positive");
            PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE = props.getProperty("prompt.media.movie.negative");
            PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE = props.getProperty("prompt.media.movie.neutral");
            PROMPT_MEDIA_CELEBRITY_TEMPLATE = props.getProperty("prompt.media.celebrity");
            PROMPT_POLITICS_POLITICIAN_TEMPLATE = props.getProperty("prompt.politics.politician");
            PROMPT_POLITICS_CAMPAIGN_TEMPLATE = props.getProperty("prompt.politics.campaign");
            PROMPT_POLITICS_PARTY_TEMPLATE = props.getProperty("prompt.politics.party");
        } catch (IOException ex) {
            throw new RuntimeException("Error loading application.properties", ex);
        }
    }

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTable(conn, "x_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTable(conn, "instagram_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing instagram_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTable(conn, "youtube_comments");
                } catch (SQLException e) {
                    System.err.println("Error processing youtube_comments: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processRedditPosts(conn);
                } catch (SQLException e) {
                    System.err.println("Error processing reddit_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterInvalidPosts(conn, "x_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterPositivePosts(conn, "x_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterNegativePosts(conn, "x_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
            executor.submit(() -> {
                try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                    processTableFilterNeutralPosts(conn, "x_posts");
                } catch (SQLException e) {
                    System.err.println("Error processing x_posts: " + e.getMessage());
                }
            });
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void processRedditPosts(Connection conn) throws SQLException {
        String sql = "SELECT id, title, text, keyword, sentiment_category FROM reddit_posts WHERE sentiment_score IS NULL OR sentiment_score = 0";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        SentimentResponse sentimentResponse;
                        if (text != null && !text.trim().isEmpty()) {
                            SentimentResponse titleSentiment = getAverageSentimentScore(title, keyword, category);
                            SentimentResponse textSentiment = getAverageSentimentScore(text, keyword, category);
                            sentimentResponse = new SentimentResponse();
                            sentimentResponse.setPositivityScore((titleSentiment.getPositivityScore() + textSentiment.getPositivityScore()) / 2);
                            sentimentResponse.setCategory(textSentiment.getCategory());
                        } else {
                            sentimentResponse = getAverageSentimentScore(title, keyword, category);
                        }
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTable(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NULL OR sentiment_score > -1 AND sentiment_category IS NULL";//v2
//Orig        String sql = "SELECT id, text, keyword FROM " + tableName + " WHERE sentiment_score IS NULL OR sentiment_score != 0";
//        String sql = "SELECT id, text, keyword FROM " + tableName + " WHERE keyword = 'GDN' OR keyword = 'GDNaidu'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, category);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        updateSentimentScore(conn, tableName, id, sentimentResponse);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterInvalidPosts(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score > 0";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".invalid";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 0) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterPositivePosts(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score >= 75";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".positive";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() >= 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterNegativePosts(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score <= 50";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".negative";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() <= 50) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void processTableFilterNeutralPosts(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword, sentiment_category FROM " + tableName + " WHERE sentiment_score IS NOT NULL AND sentiment_score > 50 AND sentiment_score < 75";//v2
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");
                String category = rs.getString("sentiment_category");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        String tempCategory = category + ".neutral";
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword, tempCategory);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        if (sentimentResponse.getPositivityScore() > 50 && sentimentResponse.getPositivityScore() < 75) {
                            sentimentResponse.setCategory(category);
                            updateSentimentScore(conn, tableName, id, sentimentResponse);
                        }
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id + ". " + e.getMessage());
                    }
                }
            }
        }
    }

    private static SentimentResponse getAverageSentimentScore(String text, String keyword, String category) throws IOException {
        int totalScore = 0;
        int validScoreCount = 0;
        SentimentResponse sentimentResponse = new SentimentResponse();
        HashMap<String, String> prompts = new HashMap<>();
        prompts.put("media.movie", PROMPT_MEDIA_MOVIE_TEMPLATE);
        prompts.put("media.movie.invalid", PROMPT_MEDIA_MOVIE_INVALID_TEMPLATE);
        prompts.put("media.movie.positive", PROMPT_MEDIA_MOVIE_POSITIVE_TEMPLATE);
        prompts.put("media.movie.negative", PROMPT_MEDIA_MOVIE_NEGATIVE_TEMPLATE);
        prompts.put("media.movie.neutral", PROMPT_MEDIA_MOVIE_NEUTRAL_TEMPLATE);
        prompts.put("media.celebrity", PROMPT_MEDIA_CELEBRITY_TEMPLATE);
        prompts.put("politics.politician", PROMPT_POLITICS_POLITICIAN_TEMPLATE);
        prompts.put("politics.campaign", PROMPT_POLITICS_CAMPAIGN_TEMPLATE);
        prompts.put("politics.party", PROMPT_POLITICS_PARTY_TEMPLATE);

        if (category == null) {
            if (keyword.equalsIgnoreCase("GDN") || keyword.equalsIgnoreCase("GDNaidu")
                    || keyword.equalsIgnoreCase("Dhurandhar") || keyword.equalsIgnoreCase("Dhurandhar2")
                    || keyword.equalsIgnoreCase("GDNTheFilm") || keyword.equalsIgnoreCase("DhurandharTheRevenge")
                    || keyword.equalsIgnoreCase("tereishqmein") || keyword.equalsIgnoreCase("Parasakthi")
                    || keyword.equalsIgnoreCase("Assi") || keyword.equalsIgnoreCase("WithLove")
                    || keyword.equalsIgnoreCase("tereishqmein")) {
                category = "media.movie";
            } else if (keyword.equalsIgnoreCase("Madhavan") || keyword.equalsIgnoreCase("Dushara")
                    || keyword.equalsIgnoreCase("Sathyaraj") || keyword.equalsIgnoreCase("Jayaram")
                    || keyword.equalsIgnoreCase("DusharaVijayan") || keyword.equalsIgnoreCase("RMadhavan")
                    || keyword.equalsIgnoreCase("VinayRai") || keyword.equalsIgnoreCase("KrishnakumarRamakumar")
                    || keyword.equalsIgnoreCase("VijayMoolan") || keyword.equalsIgnoreCase("YogiBabu")) {
                category = "media.celebrity";
            } else {
                category = "politics.party";
            }
        } else if (category.equalsIgnoreCase("invalid")) {
            category = "media.movie";
        }

        String prompt = prompts.get(category);
        System.out.println("Category: " + category);

        for (int i=0; i<3; i++) {
            sentimentResponse = callSentimentApi(prompt, text, keyword);
            int score = (int) Math.round(sentimentResponse.getPositivityScore());

            if (score >= 0) {
                totalScore += score;
                validScoreCount++;
            } else {
                validScoreCount = 0;
                break;
            }
        }

        if (validScoreCount == 0) {
            return sentimentResponse;
        }

        SentimentResponse avgSentimentResponse = new SentimentResponse();
        int score = Math.round((float) totalScore / validScoreCount);
        avgSentimentResponse.setCategory(category);
        avgSentimentResponse.setPositivityScore(score);

        return avgSentimentResponse;
    }

    private static SentimentResponse callSentimentApi(String promptTemplate, String text, String keyword) throws IOException {
        int maxRetries = 20;
        int retryCount = 0;
        while (true) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost httpPost = new HttpPost(LLM_URL);
                Gson gson = new Gson();

                if (promptTemplate == null) {
                    System.err.println("Prompt template is null for keyword: " + keyword);
                    SentimentResponse errorResponse = new SentimentResponse();
                    errorResponse.setPositivityScore(-1.0);
                    return errorResponse;
                }

                String promptString = promptTemplate.replace("{keyword}", keyword).replace("{text}", text);
                PromptRequest payload = new PromptRequest(promptString);
                String jsonPayload = gson.toJson(payload);

                StringEntity entity = new StringEntity(jsonPayload, "UTF-8");
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    String responseString = EntityUtils.toString(response.getEntity());
                    System.out.println("Raw response from LLM: " + responseString);
                    SentimentResponse sentimentResponse;
                    String jsonResponse = null;
                    int firstBrace = responseString.indexOf('{');
                    int lastBrace = responseString.lastIndexOf('}');

                    if (firstBrace != -1 && lastBrace != -1 && lastBrace > firstBrace) {
                        jsonResponse = responseString.substring(firstBrace, lastBrace + 1);
                    }

                    if (jsonResponse == null) {
                        System.err.println("Could not find a valid JSON object in the response.");
                        sentimentResponse = new SentimentResponse();
                        sentimentResponse.setPositivityScore(-1.0);
                        return sentimentResponse;
                    }

                    try {
                        sentimentResponse = gson.fromJson(jsonResponse, SentimentResponse.class);
                    } catch (JsonSyntaxException e) {
                        System.err.println("Failed to parse extracted JSON: " + jsonResponse);
                        sentimentResponse = new SentimentResponse();
                        sentimentResponse.setPositivityScore(-1.0);
                        return sentimentResponse;
                    }

                    if (sentimentResponse == null) {
                        System.err.println("Failed to parse JSON response: " + jsonResponse);
                        return new SentimentResponse();
                    }
                    return sentimentResponse;
                }
            } catch (IOException e) {
                if (++retryCount >= maxRetries) {
                    throw e;
                }
                System.err.println("LLM service is not reachable. Retrying in 30 seconds...");
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to retry", ie);
                }
            }
        }
    }

    private static void updateSentimentScore(Connection conn, String tableName, String id, SentimentResponse sentimentResponse) throws SQLException {
        int sentimentScore = (int) Math.round(sentimentResponse.getPositivityScore());
        sentimentScore = Math.max(sentimentScore, 0);
        String category = sentimentResponse.getCategory();
        String sql = "UPDATE " + tableName + " SET sentiment_score = ?, sentiment_category = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, sentimentScore);
            pstmt.setString(2, category);
            pstmt.setString(3, id);
            pstmt.executeUpdate();
        }
    }

    private static class PromptRequest {
        private final String prompt;

        public PromptRequest(String prompt) {
            this.prompt = prompt;
        }

        public String getPrompt() {
            return prompt;
        }
    }

    private static class SentimentResponse {
        private String category;
        private double positivity_score;

        SentimentResponse() {
            this.category = "invalid";
            this.positivity_score = 0;
        }

        public double getPositivityScore() {
            return positivity_score;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public void setPositivityScore(double positivityScore) {
            this.positivity_score = positivityScore;
        }
    }
}
