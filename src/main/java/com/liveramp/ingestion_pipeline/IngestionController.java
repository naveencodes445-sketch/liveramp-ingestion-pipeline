package com.liveramp.ingestion_pipeline;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

@RestController
@RequestMapping("/ingest")
public class IngestionController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private PubSubPublisherService pubSubPublisher;

    private static final String TOPIC = "raw-records";

    // Source 1 — CSV File
    @PostMapping("/file")
    public ResponseEntity<String> ingestFile(@RequestBody String data) {
        kafkaTemplate.send(TOPIC, "FILE", data);
        return ResponseEntity.ok("✅ File data sent to pipeline!");
    }

    // Source 2 — REST API
    @PostMapping("/api")
    public ResponseEntity<String> ingestApi(@RequestBody String data) {
        kafkaTemplate.send(TOPIC, "API", data);
        return ResponseEntity.ok("✅ API data sent to pipeline!");
    }

    // Source 3 — Webhook
    @PostMapping("/webhook")
    public ResponseEntity<String> ingestWebhook(@RequestBody String data) {
        kafkaTemplate.send(TOPIC, "WEBHOOK", data);
        return ResponseEntity.ok("✅ Webhook data sent to pipeline!");
    }

    // Health check
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("🚀 LiveRamp Ingestion Pipeline is running!");
    }
}