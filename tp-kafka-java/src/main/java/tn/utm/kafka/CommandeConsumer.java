package tn.utm.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Exercice 4.C — Consommateur qui desserialise des Commande JSON.
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeConsumer"
 */
public class CommandeConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupe-commandes");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommandeDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Consumer<String, Commande> consumer = new KafkaConsumer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(Collections.singletonList("commandes"));
            System.out.println("En attente de commandes...");

            while (true) {
                ConsumerRecords<String, Commande> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Commande> r : records) {
                    Commande c = r.value();
                    System.out.printf(
                            ">> [p=%d, off=%d] id=%s, total=%.2f DT, articles=%s, date=%s%n",
                            r.partition(), r.offset(), c.getId(), c.getTotal(),
                            c.getArticles(), c.getDate()
                    );
                }
                if (!records.isEmpty()) consumer.commitSync();
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
        } finally {
            consumer.close();
        }
    }
}
