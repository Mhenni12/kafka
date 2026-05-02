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
 * Partie 4.3 — Premier consommateur Kafka.
 * Lit le topic "ventes" et affiche partition / offset / clé / valeur.
 * Commit MANUEL des offsets (commitSync) après traitement de chaque batch.
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleConsumer"
 */
public class SimpleConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupe-java-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Au premier lancement (aucun offset commite), on lit depuis le debut.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Commit manuel = traitement fiable.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Fermeture propre quand on tape Ctrl+C
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(Collections.singletonList("ventes"));
            System.out.println("En attente de messages sur le topic 'ventes'... (Ctrl+C pour arreter)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            ">> partition=%d, offset=%d, key=%s, value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value()
                    );
                    // Ici, traitement metier (insertion BDD, appel API, ...)
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            // attendu lors du Ctrl+C
        } finally {
            consumer.close();
            System.out.println("Consumer ferme proprement.");
        }
    }
}
