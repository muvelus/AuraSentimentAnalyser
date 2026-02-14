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
import java.util.Properties;

public class SentimentAnalysis {

    private static final String DB_URL;
    private static final String USER;
    private static final String PASS;
    private static final String LLM_URL;
    private static final String PROMPT_TEMPLATE;

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
            PROMPT_TEMPLATE = props.getProperty("prompt");
        } catch (IOException ex) {
            throw new RuntimeException("Error loading application.properties", ex);
        }
    }

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            processTable(conn, "x_posts");
            processTable(conn, "instagram_posts");
            processTable(conn, "youtube_comments");
            processRedditPosts(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void processRedditPosts(Connection conn) throws SQLException {
//        String sql = "SELECT id, title, text, keyword FROM reddit_posts WHERE sentiment_score IS NULL OR sentiment_score = 0";
        String sql = "SELECT id, title, text, keyword FROM reddit_posts WHERE sentiment_score IS NULL OR sentiment_score != -1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        int score;
                        if (text != null && !text.trim().isEmpty()) {
                            int titleScore = getAverageSentimentScore(title, keyword);
                            int textScore = getAverageSentimentScore(text, keyword);
                            score = (titleScore + textScore) / 2;
                        } else {
                            score = getAverageSentimentScore(title, keyword);
                        }
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + score);
                        updateSentimentScore(conn, "reddit_posts", id, score);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static void processTable(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword FROM " + tableName + " WHERE sentiment_score IS NULL OR sentiment_score = 0";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        int averageSentimentScore = getAverageSentimentScore(text, keyword);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + averageSentimentScore);
                        updateSentimentScore(conn, tableName, id, averageSentimentScore);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static int getAverageSentimentScore(String text, String keyword) throws IOException {
        int totalScore = 0;
        int validScoreCount = 0;
        int numberOfCalls = 3;
        for (int i = 0; i < numberOfCalls; i++) {
            int score = callSentimentApi(text, keyword);
            if (score >= 0) {
                totalScore += score;
                validScoreCount++;
            } else {
                return 0;
            }
        }

        if (validScoreCount == 0) {
            return 0;
        }

        return Math.round((float) totalScore / validScoreCount);
    }

    private static int callSentimentApi(String text, String keyword) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(LLM_URL);
            Gson gson = new Gson();

            String promptString = PROMPT_TEMPLATE.replace("{keyword}", keyword).replace("{text}", text);
            PromptRequest payload = new PromptRequest(promptString);
            String jsonPayload = gson.toJson(payload);

            StringEntity entity = new StringEntity(jsonPayload, "UTF-8");
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json; charset=UTF-8");

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                String responseString = EntityUtils.toString(response.getEntity());
                System.out.println("Raw response from LLM: " + responseString);

                String jsonResponse = null;
                int firstBrace = responseString.indexOf('{');
                int lastBrace = responseString.lastIndexOf('}');

                if (firstBrace != -1 && lastBrace != -1 && lastBrace > firstBrace) {
                    jsonResponse = responseString.substring(firstBrace, lastBrace + 1);
                }

                if (jsonResponse == null) {
                    System.err.println("Could not find a valid JSON object in the response.");
                    return 0;
                }

                SentimentResponse sentimentResponse = null;
                try {
                    sentimentResponse = gson.fromJson(jsonResponse, SentimentResponse.class);
                } catch (JsonSyntaxException e) {
                    System.err.println("Failed to parse extracted JSON: " + jsonResponse);
                    return 0;
                }

                if (sentimentResponse == null) {
                    System.err.println("Failed to parse JSON response: " + jsonResponse);
                    return 0;
                }
                return (int) Math.round(sentimentResponse.getPositivityScore());
            }
        }
    }

    private static void updateSentimentScore(Connection conn, String tableName, String id, int sentimentScore) throws SQLException {
        String sql = "UPDATE " + tableName + " SET sentiment_score = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, sentimentScore);
            pstmt.setString(2, id);
            pstmt.executeUpdate();
        }
    }

    private static class PromptRequest {
        private final String prompt;

        public PromptRequest(String prompt) {
            this.prompt = prompt;
        }
    }

    private static class SentimentResponse {
        private double positivity_score;

        public double getPositivityScore() {
            return positivity_score;
        }
    }
}
