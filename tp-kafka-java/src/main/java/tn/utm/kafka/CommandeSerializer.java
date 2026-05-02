package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Exercice 4.C bonus — Serialiseur Kafka custom pour Commande -> JSON bytes.
 */
public class CommandeSerializer implements Serializer<Commande> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, Commande data) {
        if (data == null) return null;
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Impossible de serialiser la Commande", e);
        }
    }
}
