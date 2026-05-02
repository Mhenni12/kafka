package tn.utm.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Partie 4.2 — Premier producteur Kafka.
 * Envoie 10 messages dans le topic "ventes" avec une clé client-X
 * pour démontrer le partitionnement par clé.
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleProducer"
 */
public class SimpleProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Garanties de durabilité
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Optimisation latence vs débit
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String key = "client-" + (i % 3);
                String value = "Achat numero " + i + " (" + (50 + i * 17) + " DT)";

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("ventes", key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Echec d'envoi : " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "OK envoye -> partition=%d, offset=%d, key=%s%n",
                                metadata.partition(), metadata.offset(), key
                        );
                    }
                });
            }
            producer.flush();
            System.out.println("Tous les messages ont ete flushes.");
        }
    }
}
