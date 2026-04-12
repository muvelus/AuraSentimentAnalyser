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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SentimentAnalysis {

    private static int NUMBER_OR_SENTIMENT_ANALYSIS_RETRIES = 3;
    private static final String DB_URL;
    private static final String USER;
    private static final String PASS;
    private static final String LLM_URL;
    private static final String PROMPT_MEDIA_MOVIE_TEMPLATE;
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
            NUMBER_OR_SENTIMENT_ANALYSIS_RETRIES = Integer.parseInt(props.getProperty("sentiment.analysis.retries", "3"));
            DB_URL = props.getProperty("db.url");
            USER = props.getProperty("db.user");
            PASS = props.getProperty("db.password");
            LLM_URL = props.getProperty("llm.url");
            PROMPT_MEDIA_MOVIE_TEMPLATE = props.getProperty("prompt.media.movie");
            PROMPT_MEDIA_CELEBRITY_TEMPLATE = props.getProperty("prompt.media.celebrity");
            PROMPT_POLITICS_POLITICIAN_TEMPLATE = props.getProperty("prompt.politics.politician");
            PROMPT_POLITICS_CAMPAIGN_TEMPLATE = props.getProperty("prompt.politics.campaign");
            PROMPT_POLITICS_PARTY_TEMPLATE = props.getProperty("prompt.politics.party");
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
        String sql = "SELECT id, title, text, keyword FROM reddit_posts WHERE sentiment_score IS NULL OR sentiment_score = 0";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");

                if (keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        SentimentResponse sentimentResponse;
                        if (text != null && !text.trim().isEmpty()) {
                            SentimentResponse titleSentiment = getAverageSentimentScore(title, keyword);
                            SentimentResponse textSentiment = getAverageSentimentScore(text, keyword);
                            sentimentResponse = new SentimentResponse();
                            sentimentResponse.setPositivityScore((titleSentiment.getPositivityScore() + textSentiment.getPositivityScore()) / 2);
                            sentimentResponse.setCategory(textSentiment.getCategory());
                        } else {
                            sentimentResponse = getAverageSentimentScore(title, keyword);
                        }
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        updateSentimentScore(conn, "reddit_posts", id, sentimentResponse);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for reddit_posts ID: " + id);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static void processTable(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT id, text, keyword FROM " + tableName + " WHERE sentiment_score IS NULL OR sentiment_score > -1";
//        String sql = "SELECT id, text, keyword FROM " + tableName + " WHERE sentiment_score IS NULL OR sentiment_score != 0";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String text = rs.getString("text");
                String keyword = rs.getString("keyword");

                if (text != null && !text.trim().isEmpty() && keyword != null && !keyword.trim().isEmpty()) {
                    try {
                        SentimentResponse sentimentResponse = getAverageSentimentScore(text, keyword);
                        System.out.println("Updating sentiment score for id: " + id + ", keyword: " + keyword + ", score: " + sentimentResponse.getPositivityScore());
                        updateSentimentScore(conn, tableName, id, sentimentResponse);
                    } catch (IOException e) {
                        System.err.println("Error calling sentiment analysis API for " + tableName + " ID: " + id);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static SentimentResponse getAverageSentimentScore(String text, String keyword) throws IOException {
        SentimentResponse sentimentResponse = new SentimentResponse();
        List<String> prompts = Arrays.asList(
                PROMPT_MEDIA_MOVIE_TEMPLATE,
                PROMPT_MEDIA_CELEBRITY_TEMPLATE,
                PROMPT_POLITICS_POLITICIAN_TEMPLATE,
                PROMPT_POLITICS_CAMPAIGN_TEMPLATE,
                PROMPT_POLITICS_PARTY_TEMPLATE
        );

        for (String prompt : prompts) {
            int totalScore = 0;
            int validScoreCount = 0;

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
                continue;
            }

            SentimentResponse avgSentimentResponse = new SentimentResponse();
            avgSentimentResponse.setCategory(sentimentResponse.getCategory());
            avgSentimentResponse.setPositivityScore(Math.round((float) totalScore / validScoreCount));

            return avgSentimentResponse;
        }

        // Invalid for ALL categories i.e. movie, celebrity, politician, political campaign or political party
        return sentimentResponse;
    }

    private static SentimentResponse callSentimentApi(String promptTemplate, String text, String keyword) throws IOException {
        int maxRetries = 20;
        int retryCount = 0;
        while (true) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost httpPost = new HttpPost(LLM_URL);
                Gson gson = new Gson();

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

                    String jsonResponse = null;
                    int firstBrace = responseString.indexOf('{');
                    int lastBrace = responseString.lastIndexOf('}');

                    if (firstBrace != -1 && lastBrace != -1 && lastBrace > firstBrace) {
                        jsonResponse = responseString.substring(firstBrace, lastBrace + 1);
                    }

                    if (jsonResponse == null) {
                        System.err.println("Could not find a valid JSON object in the response.");
                        return new SentimentResponse();
                    }

                    SentimentResponse sentimentResponse = null;
                    try {
                        sentimentResponse = gson.fromJson(jsonResponse, SentimentResponse.class);
                    } catch (JsonSyntaxException e) {
                        System.err.println("Failed to parse extracted JSON: " + jsonResponse);
                        return new SentimentResponse();
                    }

                    if (sentimentResponse == null) {
                        System.err.println("Failed to parse JSON response: " + jsonResponse);
                        return new SentimentResponse();
                    }
//                    return (int) Math.round(sentimentResponse.getPositivityScore());
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
        sentimentScore = sentimentScore < 0 ? 0 : sentimentScore;
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
