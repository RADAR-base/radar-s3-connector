package org.radarbase.connect.s3;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;
import okhttp3.Response;
import org.apache.avro.SchemaValidationException;
import org.junit.jupiter.api.Test;
import org.radarbase.config.ServerConfig;
import org.radarbase.producer.KafkaTopicSender;
import org.radarbase.producer.rest.RestClient;
import org.radarbase.producer.rest.RestSender;
import org.radarbase.producer.rest.RestSender.Builder;
import org.radarbase.producer.rest.SchemaRetriever;
import org.radarbase.topic.AvroTopic;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneLight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

    @Test()
    public void integrationTest() throws IOException, InterruptedException,
            SchemaValidationException {

        RestClient restClient =
                RestClient.global().server(new ServerConfig("http://localhost:8082")).build();

        RestSender sender = new Builder().httpClient(restClient)
                .schemaRetriever(new SchemaRetriever(new ServerConfig("http://localhost:8081"), 5))
                .build();

        AvroTopic<ObservationKey, PhoneLight> test1 =
                new AvroTopic<>("test", ObservationKey.getClassSchema(),
                        PhoneLight.getClassSchema(), ObservationKey.class, PhoneLight.class);

        for (int i = 0; i < 50; i++) {
            try (Response response = restClient.request("topics")) {
                String responseBody = RestClient.responseBody(response);
                if (response.code() == 200) {
                    if (responseBody == null || responseBody.length() <= 2) {
                        logger.warn("Kafka not ready (no topics available yet)");
                    } else {
                        logger.info("Kafka ready");
                        break;
                    }
                } else {
                    logger.warn("Kafka not ready (HTTP code {}): {}", response.code(),
                            responseBody);
                }
            } catch (IOException ex) {
                logger.error("Kafka not ready (failed to connect): {}", ex.toString());
            }
            Thread.sleep(1_000L);
        }

        for (int i = 0; i < 50; i++) {
            try (KafkaTopicSender<ObservationKey, PhoneLight> topicSender = sender.sender(test1)) {
                topicSender.send(new ObservationKey("a", "b", "c"), new PhoneLight(1d, 1d, 1f));
            }
        }

        Thread.sleep(10_000L);
        validateObjects();

    }

    private void validateObjects() {
        String bucketName = "uploads";
        String accessKey = "minio";
        String secretKey = "minio123";
        try {
            MinioClient client = new MinioClient("http://localhost:9000", accessKey, secretKey);

            boolean isBucketAvailable = false;
            int retry = 10;
            do {
                isBucketAvailable = client.bucketExists(bucketName);
                retry--;
                Thread.sleep(5_000L);
            } while (!isBucketAvailable || retry < 0);

            Iterable<Result<Item>> objects = client.listObjects(bucketName, "topics/test/", true);
            for (Result<Item> itemResult : objects) {
                String objectName = itemResult.get().objectName();
                logger.info("ObjectName {}", objectName);
                assertTrue(objectName
                        .matches("^topics/test/partition=0/test\\+0\\+00000000[0-9]0.avro$"));
            }
        } catch (InvalidEndpointException | InvalidPortException | InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | InvalidKeyException | XmlParserException | ErrorResponseException | InternalException | InvalidResponseException | IOException e) {
            logger.error("Could not access minio", e);
            fail("Could not access minio");
        } catch (InterruptedException e) {
            logger.error("Test intruppted", e);
            fail("Could not access minio");
        }
    }
}