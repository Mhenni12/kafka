package tn.utm.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Exercice 4.C — Producteur qui envoie des Commande en JSON
 * via le serialiseur custom CommandeSerializer.
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeProducer"
 */
public class CommandeProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandeSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Producer<String, Commande> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                Commande c = new Commande(
                        UUID.randomUUID().toString(),
                        Instant.now(),
                        List.of("pain", "lait", "fromage-" + i),
                        12.50 * i
                );
                ProducerRecord<String, Commande> record =
                        new ProducerRecord<>("commandes", c.getId(), c);

                producer.send(record, (md, ex) -> {
                    if (ex != null) ex.printStackTrace();
                    else System.out.printf("OK envoye -> partition=%d, offset=%d%n",
                            md.partition(), md.offset());
                });
            }
            producer.flush();
        }
    }
}
