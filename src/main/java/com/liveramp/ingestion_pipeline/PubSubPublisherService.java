package com.liveramp.ingestion_pipeline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.springframework.stereotype.Service;
import java.io.FileInputStream;

@Service
public class PubSubPublisherService {

    private static final String PROJECT = "liveramp-ingestion-project";
    private static final String TOPIC = "raw-records-pubsub";

    public void publish(String message) {
        try {
            GoogleCredentials credentials = GoogleCredentials
                .fromStream(new FileInputStream("gcp-key.json"));

            TopicName topicName = TopicName.of(PROJECT, TOPIC);
            Publisher publisher = Publisher.newBuilder(topicName)
                .setCredentialsProvider(() -> credentials)
                .build();

            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage
                .newBuilder()
                .setData(data)
                .build();

            publisher.publish(pubsubMessage);
            publisher.shutdown();

            System.out.println("✅ Published to Pub/Sub: " + message);

        } catch (Exception e) {
            System.out.println("❌ Pub/Sub publish failed: " + e.getMessage());
        }
    }
}