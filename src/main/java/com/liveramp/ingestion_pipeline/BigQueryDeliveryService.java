package com.liveramp.ingestion_pipeline;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;

@Service
public class BigQueryDeliveryService {

    private BigQuery bigquery;
    private static final String PROJECT = "liveramp-ingestion-project";
    private static final String DATASET = "ingestion_data";
    private static final String TABLE = "clean_records";

    public BigQueryDeliveryService() {
        try {
            GoogleCredentials credentials = GoogleCredentials
                .fromStream(new FileInputStream("gcp-key.json"));
            this.bigquery = BigQueryOptions.newBuilder()
                .setProjectId(PROJECT)
                .setCredentials(credentials)
                .build()
                .getService();
            System.out.println("✅ BigQuery connected!");
        } catch (Exception e) {
            System.out.println("❌ BigQuery connection failed: " 
                + e.getMessage());
        }
    }

    @KafkaListener(topics = "clean-records", groupId = "bigquery-group")
    public void deliverToBigQuery(String message) {
        System.out.println("📦 Delivering to BigQuery: " + message);
        try {
            // Extract all fields
            String id = extractField(message, "id");
            String name = extractField(message, "name");
            String email = extractField(message, "email");
            String source = extractField(message, "source");
            String timestamp = extractField(message, "timestamp");

            // Build the row
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            row.put("name_anon", "ANON_" + id);
            row.put("email_hash", email);
            row.put("source", source);
            row.put("timestamp", timestamp);
            row.put("status", "CLEAN");

            // Insert into BigQuery
            TableId tableId = TableId.of(DATASET, TABLE);
            InsertAllRequest request = InsertAllRequest
                .newBuilder(tableId)
                .addRow(id + "_" + System.currentTimeMillis(), row)
                .build();

            InsertAllResponse response = bigquery.insertAll(request);

            if (response.hasErrors()) {
                System.out.println("❌ BigQuery errors: " 
                    + response.getInsertErrors());
            } else {
                System.out.println("✅ Record saved! id=" + id 
                    + " source=" + source);
            }
        } catch (Exception e) {
            System.out.println("❌ Error: " + e.getMessage());
        }
    }

    private String extractField(String json, String field) {
        try {
            String search = "\"" + field + "\":\"";
            if (json.contains(search)) {
                return json.split(search)[1].split("\"")[0];
            }
            // Try without quotes (for numbers)
            String searchNum = "\"" + field + "\":";
            if (json.contains(searchNum)) {
                return json.split(searchNum)[1]
                    .split("[,}]")[0].trim();
            }
            return "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
