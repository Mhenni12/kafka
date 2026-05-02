package tn.utm.kafka.projet;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mini-projet — Etape 3 : ChiffreAffairesParVille
 *
 *  - Lit "pos-events" dans le groupe "ca-1"
 *  - Tient en memoire un cumul VENTE - RETOUR par ville
 *  - Affiche un tableau toutes les 5 secondes
 *  - Commit MANUEL apres chaque batch
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.ChiffreAffairesParVille"
 */
public class ChiffreAffairesParVille {

    private static final String TOPIC = "pos-events";
    private static final String GROUP = "ca-1";

    private final Map<String, Double> ca = new ConcurrentHashMap<>();
    private long dernierAffichage = System.currentTimeMillis();

    public static void main(String[] args) {
        new ChiffreAffairesParVille().run();
    }

    private void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Permet de detecter rapidement la mort d'un consumer (rebalance)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30_000);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("[CA] Demarrage. Groupe=" + GROUP);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    traiter(r);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
                afficherSiNecessaire();
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("[CA] Ferme proprement.");
        }
    }

    private void traiter(ConsumerRecord<String, String> r) {
        try {
            Evenement evt = JsonUtil.MAPPER.readValue(r.value(), Evenement.class);
            switch (evt.getType()) {
                case VENTE  -> ca.merge(evt.getVille(),  evt.getMontant(), Double::sum);
                case RETOUR -> ca.merge(evt.getVille(), -evt.getMontant(), Double::sum);
                case OUVERTURE -> { /* ignore pour le CA */ }
            }
        } catch (Exception e) {
            System.err.println("[CA] Message illisible : " + e.getMessage());
        }
    }

    private void afficherSiNecessaire() {
        long now = System.currentTimeMillis();
        if (now - dernierAffichage >= 5_000) {
            System.out.println("================ Chiffre d'affaires par ville ================");
            new TreeMap<>(ca).forEach((ville, total) ->
                    System.out.printf("  %-10s : %10.2f DT%n", ville, total));
            System.out.println("===============================================================");
            dernierAffichage = now;
        }
    }
}
