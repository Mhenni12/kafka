package tn.utm.kafka.projet;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Mini-projet — Etape 4 : DetecteurAnomalies
 *
 *  - Lit le meme topic "pos-events" mais dans un groupe DIFFERENT (alerte-1)
 *    => regle d'or des consommateurs concurrents : un message est lu une fois par groupe.
 *  - Pour chaque RETOUR de plus de 200 DT, ecrit dans le topic "alertes-retours".
 *  - Le commit n'a lieu qu'APRES l'envoi de l'alerte (cohérence at-least-once).
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.DetecteurAnomalies"
 */
public class DetecteurAnomalies {

    private static final String TOPIC_IN  = "pos-events";
    private static final String TOPIC_OUT = "alertes-retours";
    private static final String GROUP     = "alerte-1";
    private static final double SEUIL_DT  = 200.0;

    public static void main(String[] args) {
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consProps);
             Producer<String, String> producer = new KafkaProducer<>(prodProps)) {

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(Collections.singletonList(TOPIC_IN));
            System.out.println("[Anomalies] Surveillance des retours > " + SEUIL_DT + " DT");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    try {
                        Evenement e = JsonUtil.MAPPER.readValue(r.value(), Evenement.class);
                        if (e.getType() == Evenement.Type.RETOUR && e.getMontant() > SEUIL_DT) {
                            String alerte = JsonUtil.MAPPER.writeValueAsString(e);
                            producer.send(new ProducerRecord<>(TOPIC_OUT, e.getVille(), alerte));
                            System.out.printf("[Anomalies] /!\\ Retour suspect %.2f DT a %s (caisse %s)%n",
                                    e.getMontant(), e.getVille(), e.getIdCaisse());
                        }
                    } catch (Exception ex) {
                        System.err.println("[Anomalies] Message invalide : " + ex.getMessage());
                    }
                }
                if (!records.isEmpty()) {
                    producer.flush();        // garantie : alertes ecrites avant commit
                    consumer.commitSync();
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
