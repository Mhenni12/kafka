package tn.utm.kafka.projet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mini-projet — Etape 2 : Producteur de simulation.
 *
 * Genere en continu (toutes les 100 a 500 ms) des evenements POS et les
 * envoie au topic "pos-events". La cle du message est la VILLE pour garantir
 * que tous les evenements d'une meme ville arrivent dans la meme partition
 * et restent ordonnes.
 *
 * Lancement :
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.SimulateurCaisse"
 *   # Optionnel : passer un identifiant de caisse
 *   mvn exec:java -Dexec.mainClass="tn.utm.kafka.projet.SimulateurCaisse" -Dexec.args="CAISSE-02"
 */
public class SimulateurCaisse {

    private static final List<String> VILLES =
            List.of("Tunis", "Sousse", "Sfax", "Bizerte", "Gabes");

    private static final List<String> PRODUITS =
            List.of("pain", "lait", "fromage", "huile", "cafe", "the", "sucre", "riz");

    private static final String TOPIC = "pos-events";

    public static void main(String[] args) throws InterruptedException {
        String idCaisseBase = (args.length > 0) ? args[0] : "CAISSE-01";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        Random rnd = ThreadLocalRandom.current();

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("[SimulateurCaisse] " + idCaisseBase + " demarre. Ctrl+C pour arreter.");

            int compteur = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String ville = VILLES.get(rnd.nextInt(VILLES.size()));
                String idCaisse = idCaisseBase + "-" + ville.toUpperCase();
                Evenement.Type type = tirerType(rnd);
                double montant = (type == Evenement.Type.OUVERTURE) ? 0.0
                        : 5 + rnd.nextDouble() * 495;          // [5, 500[
                List<String> produits = (type == Evenement.Type.OUVERTURE) ? null
                        : tirerProduits(rnd);

                Evenement evt = new Evenement(type, idCaisse, ville, Instant.now(),
                        Math.round(montant * 100.0) / 100.0, produits);

                String json = JsonUtil.MAPPER.writeValueAsString(evt);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, ville, json);

                final int n = ++compteur;
                producer.send(record, (md, ex) -> {
                    if (ex != null) {
                        System.err.println("[SimulateurCaisse] Echec #" + n + " : " + ex.getMessage());
                    } else {
                        System.out.printf("[SimulateurCaisse] #%d %-9s ville=%-7s p=%d off=%d montant=%.2f%n",
                                n, evt.getType(), evt.getVille(), md.partition(), md.offset(), evt.getMontant());
                    }
                });

                Thread.sleep(100 + rnd.nextInt(401));   // 100..500 ms
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** 70% VENTE, 10% RETOUR, 20% OUVERTURE. */
    private static Evenement.Type tirerType(Random rnd) {
        int n = rnd.nextInt(100);
        if (n < 70) return Evenement.Type.VENTE;
        if (n < 80) return Evenement.Type.RETOUR;
        return Evenement.Type.OUVERTURE;
    }

    private static List<String> tirerProduits(Random rnd) {
        int n = 1 + rnd.nextInt(4);
        return rnd.ints(n, 0, PRODUITS.size())
                .mapToObj(PRODUITS::get)
                .distinct()
                .toList();
    }
}
