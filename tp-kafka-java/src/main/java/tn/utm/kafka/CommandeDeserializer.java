package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Pendant du CommandeSerializer cote consumer.
 */
public class CommandeDeserializer implements Deserializer<Commande> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Commande deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return MAPPER.readValue(data, Commande.class);
        } catch (Exception e) {
            throw new SerializationException("Impossible de deserialiser la Commande", e);
        }
    }
}
