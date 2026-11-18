package com.lit.fire.flame;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TamilClassifyBatch {
    private static final String URL = "jdbc:postgresql://localhost:5432/aura";
    private static final String USER = "mukundv";
    private static final String PASS = "";

    // Tamil movies in managed_entities released in the last 2 months (release_date >= 2026-06-09,
    // relative to run date 2026-08-09), normalized to the hashtag/keyword forms actually present in
    // the data: Habeebi, Valluvan, Lakshmikanthan Kolai Vazhakku, Nooru Saami, Irumunai, Idhayam
    // Murali, Black Gold, Anbe Diana, Arjunan Per Paththu, Jana Nayagan, GD Naidu. Older Tamil
    // releases (Naa Naa, Parasakthi, With Love, Karuppu, Kara, Blast, Parimala And Co) are excluded
    // to shrink the backlog and finish faster.
    private static final String TAMIL_FILTER =
            "(keyword ILIKE '%habeebi%' OR keyword ILIKE '%valluvan%' " +
            "OR keyword ILIKE '%lakshmikanthankolaivazhakku%' OR keyword ILIKE '%lakshmikanthan kolai vazhakku%' " +
            "OR keyword ILIKE '%noorusaami%' OR keyword ILIKE '%irumunai%' " +
            "OR keyword ILIKE '%idhayammurali%' OR keyword ILIKE '%idhayam murali%' " +
            "OR keyword ILIKE '%blackgold%' OR keyword ILIKE '%black gold%' OR keyword ILIKE '%anbe%diana%' " +
            "OR keyword ILIKE '%arjunanperpaththu%' OR keyword ILIKE '%arjunan per paththu%' " +
            "OR keyword ILIKE '%jana%nayagan%' OR keyword ILIKE '%gdn%' OR keyword ILIKE '%gd naidu%')";

    private static Method getContentClassification;
    private static Method updateClassification;

    public static void main(String[] args) throws Exception {
        getContentClassification = SentimentAnalysis.class.getDeclaredMethod(
                "getContentClassification", String.class, String.class, String.class);
        getContentClassification.setAccessible(true);
        Class<?> classificationResponseClass = Class.forName("com.lit.fire.flame.SentimentAnalysis$ClassificationResponse");
        updateClassification = SentimentAnalysis.class.getDeclaredMethod(
                "updateClassification", Connection.class, String.class, String.class, classificationResponseClass);
        updateClassification.setAccessible(true);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        executor.submit(() -> classifyTamil("x_posts", false));
        executor.submit(() -> classifyTamil("instagram_posts", false));
        executor.submit(() -> classifyTamil("youtube_comments", false));
        executor.submit(() -> classifyTamil("reddit_posts", true));
        executor.shutdown();
        executor.awaitTermination(48, TimeUnit.HOURS);
        System.out.println("ALL DONE");
    }

    private static void classifyTamil(String tableName, boolean isReddit) {
        String tag = "tamil-classify:" + tableName;
        String selectCols = isReddit ? "id, title, text, keyword, author" : "id, text, keyword, author";
        String sql = "SELECT " + selectCols + " FROM " + tableName +
                " WHERE sentiment_category = 'media.movie' AND author_type IS NULL AND " + TAMIL_FILTER +
                (isReddit ? "" : " AND text IS NOT NULL AND TRIM(text) <> ''");

        long start = System.currentTimeMillis();
        int processed = 0;
        int errors = 0;
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String id = rs.getString("id");
                String keyword = rs.getString("keyword");
                String author = rs.getString("author");
                String text;
                if (isReddit) {
                    String title = rs.getString("title");
                    String body = rs.getString("text");
                    text = (body != null && !body.trim().isEmpty()) ? title + "\n" + body : title;
                } else {
                    text = rs.getString("text");
                }
                if (text == null || text.trim().isEmpty()) {
                    continue;
                }
                try {
                    Object response = getContentClassification.invoke(null, text, keyword, author);
                    updateClassification.invoke(null, conn, tableName, id, response);
                    processed++;
                    if (processed % 25 == 0) {
                        long elapsed = System.currentTimeMillis() - start;
                        System.out.println("[" + tag + "] processed " + processed + " rows, " + errors + " errors, " + (elapsed / 1000) + "s elapsed");
                    }
                } catch (Exception e) {
                    errors++;
                    System.err.println("[" + tag + "] error on id " + id + ": " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println("[" + tag + "] fatal SQL error: " + e.getMessage());
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("[" + tag + "] FINISHED: " + processed + " rows processed, " + errors + " errors, " + (elapsed / 1000) + "s total");
    }
}
