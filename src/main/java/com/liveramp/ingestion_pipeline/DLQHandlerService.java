package com.liveramp.ingestion_pipeline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.FileInputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class DLQHandlerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Storage storage;
    private static final String BUCKET = "liveramp-ingestion-raw";
    private static final int MAX_RETRIES = 3;

    // Track retry counts in memory
    private Map<String, Integer> retryCount = new HashMap<>();

    public DLQHandlerService() {
        try {
            GoogleCredentials credentials = GoogleCredentials
                .fromStream(new FileInputStream("gcp-key.json"));
            this.storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
            System.out.println("✅ DLQ Handler connected to GCS!");
        } catch (Exception e) {
            System.out.println("❌ DLQ GCS connection failed: " 
                + e.getMessage());
        }
    }

    @KafkaListener(topics = "dead-letter-queue", 
                   groupId = "dlq-handler-group")
    public void handleFailedRecord(String message) {
        System.out.println("🚨 DLQ received: " + message);

        // Get retry count for this message
        String key = String.valueOf(message.hashCode());
        int retries = retryCount.getOrDefault(key, 0);

        if (retries < MAX_RETRIES) {
            // Retry — send back to raw-records
            retryCount.put(key, retries + 1);
            System.out.println("🔄 Retrying... attempt " 
                + (retries + 1) + " of " + MAX_RETRIES);
            kafkaTemplate.send("raw-records", message);

        } else {
            // Max retries reached — save to GCS
            System.out.println("💾 Max retries reached. "
                + "Saving to GCS...");
            saveToGCS(message);
            retryCount.remove(key);
        }
    }

    private void saveToGCS(String message) {
        try {
            // Create folder structure: failed/YYYY/MM/DD/
            String date = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            String fileName = "failed/" + date + "/record_" 
                + System.currentTimeMillis() + ".json";

            BlobId blobId = BlobId.of(BUCKET, fileName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("application/json")
                .build();

            storage.create(blobInfo, message.getBytes());
            System.out.println("✅ Saved to GCS: " + fileName);

        } catch (Exception e) {
            System.out.println("❌ GCS save failed: " 
                + e.getMessage());
        }
    }
}