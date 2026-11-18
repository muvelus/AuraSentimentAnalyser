package com.lit.fire.flame;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// One-off full-backlog reprocessing of youtube_comments now that sentiment scoring and
// classification include video_title context (see processTableYoutube/processClassificationYoutube
// in SentimentAnalysis). Resets the ~14k rows that were already scored context-blind, then
// rescores/reclassifies every eligible row (both those and the already-unscored ~48k) using the
// same video_title + text combination the fixed pipeline now uses going forward.
public class YoutubeBacklogReclassifyBatch {
    private static final String URL = "jdbc:postgresql://localhost:5432/aura";
    private static final String USER = "mukundv";
    private static final String PASS = "";
    private static final int WORKERS = 8;

    private static Method getAverageSentimentScore;
    private static Method updateSentimentScore;
    private static Method getContentClassification;
    private static Method updateClassification;
    private static Method markPassDone;
    private static Class<?> sentimentResponseClass;
    private static Class<?> classificationResponseClass;

    private static final AtomicInteger scoredCount = new AtomicInteger();
    private static final AtomicInteger classifiedCount = new AtomicInteger();
    private static final AtomicInteger errorCount = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        sentimentResponseClass = Class.forName("com.lit.fire.flame.SentimentAnalysis$SentimentResponse");
        classificationResponseClass = Class.forName("com.lit.fire.flame.SentimentAnalysis$ClassificationResponse");

        getAverageSentimentScore = SentimentAnalysis.class.getDeclaredMethod(
                "getAverageSentimentScore", String.class, String.class, String.class);
        getAverageSentimentScore.setAccessible(true);
        updateSentimentScore = SentimentAnalysis.class.getDeclaredMethod(
                "updateSentimentScore", Connection.class, String.class, String.class, sentimentResponseClass);
        updateSentimentScore.setAccessible(true);
        getContentClassification = SentimentAnalysis.class.getDeclaredMethod(
                "getContentClassification", String.class, String.class, String.class);
        getContentClassification.setAccessible(true);
        updateClassification = SentimentAnalysis.class.getDeclaredMethod(
                "updateClassification", Connection.class, String.class, String.class, classificationResponseClass);
        updateClassification.setAccessible(true);
        markPassDone = SentimentAnalysis.class.getDeclaredMethod(
                "markPassDone", Connection.class, String.class, String.class, String.class);
        markPassDone.setAccessible(true);

        List<String[]> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
            try (Statement stmt = conn.createStatement()) {
                int reset = stmt.executeUpdate(
                        "UPDATE youtube_comments SET sentiment_score = NULL, sentiment_category = NULL, " +
                                "author_type = NULL, content_intent = NULL, predicted_region = NULL, topic_category = NULL, " +
                                "sentiment_passes = '{}' " +
                                "WHERE keyword IS NOT NULL AND TRIM(keyword) <> '' AND text IS NOT NULL AND TRIM(text) <> '' " +
                                "AND sentiment_score IS NOT NULL AND sentiment_score <> 0");
                System.out.println("Reset " + reset + " previously context-blind rows");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT id, text, video_title, keyword, author FROM youtube_comments" +
                                 " WHERE text IS NOT NULL AND TRIM(text) <> ''" +
                                 " AND keyword IS NOT NULL AND TRIM(keyword) <> ''" +
                                 " AND (sentiment_score IS NULL OR sentiment_score = 0)" +
                                 " ORDER BY id")) {
                while (rs.next()) {
                    rows.add(new String[]{
                            rs.getString("id"),
                            rs.getString("text"),
                            rs.getString("video_title"),
                            rs.getString("keyword"),
                            rs.getString("author")
                    });
                }
            }
        }

        System.out.println("Total rows to process: " + rows.size() + " across " + WORKERS + " workers");

        List<List<String[]>> partitions = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) partitions.add(new ArrayList<>());
        for (int i = 0; i < rows.size(); i++) partitions.get(i % WORKERS).add(rows.get(i));

        long start = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(WORKERS);
        for (int w = 0; w < WORKERS; w++) {
            final int workerId = w;
            final List<String[]> partition = partitions.get(w);
            executor.submit(() -> processPartition(workerId, partition, start));
        }
        executor.shutdown();
        executor.awaitTermination(48, TimeUnit.HOURS);

        long elapsed = (System.currentTimeMillis() - start) / 1000;
        System.out.println("ALL DONE: scored=" + scoredCount.get() + " classified=" + classifiedCount.get() +
                " errors=" + errorCount.get() + " elapsed=" + elapsed + "s");
    }

    private static void processPartition(int workerId, List<String[]> partition, long start) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
            int done = 0;
            for (String[] row : partition) {
                String id = row[0];
                String text = row[1];
                String videoTitle = row[2];
                String keyword = row[3];
                String author = row[4];
                String combinedText = (videoTitle != null && !videoTitle.trim().isEmpty()) ? videoTitle + "\n" + text : text;

                try {
                    Object sentimentResponse = getAverageSentimentScore.invoke(null, combinedText, keyword, null);
                    updateSentimentScore.invoke(null, conn, "youtube_comments", id, sentimentResponse);
                    markPassDone.invoke(null, conn, "youtube_comments", id, "initial");
                    scoredCount.incrementAndGet();

                    Method getCategory = sentimentResponseClass.getMethod("getCategory");
                    Object category = getCategory.invoke(sentimentResponse);

                    if ("media.movie".equals(category)) {
                        Object classificationResponse = getContentClassification.invoke(null, combinedText, keyword, author);
                        updateClassification.invoke(null, conn, "youtube_comments", id, classificationResponse);
                        classifiedCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("[worker" + workerId + "] error on id " + id + ": " + e.getMessage());
                }

                done++;
                if (done % 200 == 0) {
                    long elapsed = (System.currentTimeMillis() - start) / 1000;
                    System.out.println("[worker" + workerId + "] " + done + "/" + partition.size() +
                            " done, totals scored=" + scoredCount.get() + " classified=" + classifiedCount.get() +
                            " errors=" + errorCount.get() + " elapsed=" + elapsed + "s");
                }
            }
            System.out.println("[worker" + workerId + "] FINISHED partition of " + partition.size());
        } catch (SQLException e) {
            System.err.println("[worker" + workerId + "] fatal SQL error: " + e.getMessage());
        }
    }
}
