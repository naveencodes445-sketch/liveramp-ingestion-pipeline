package com.liveramp.ingestion_pipeline;

import java.security.MessageDigest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ValidationService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "raw-records", groupId = "validation-group")
    public void validate(String message) {
        System.out.println("Received: " + message);
        if (!message.contains("\"id\"")) {
            sendToDLQ(message, "Missing id");
            return;
        }
        if (!message.contains("\"email\"")) {
            sendToDLQ(message, "Missing email");
            return;
        }
        if (!message.contains("@")) {
            sendToDLQ(message, "Bad email");
            return;
        }
        String cleaned = anonymize(message);
        kafkaTemplate.send("clean-records", cleaned);
        System.out.println("Valid record sent to clean-records");
    }

    private void sendToDLQ(String msg, String reason) {
        kafkaTemplate.send("dead-letter-queue", reason);
        System.out.println("Invalid! Reason: " + reason);
    }

    private String anonymize(String message) {
        try {
            String email = message.split("\"email\":\"")[1].split("\"")[0];
            String hashed = hashSHA256(email);
            return message.replace(email, hashed);
        } catch (Exception e) {
            return message;
        }
    }

    private String hashSHA256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes());
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            return "hash-error";
        }
    }
}