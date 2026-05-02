package tn.utm.kafka.projet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * ObjectMapper unique partage par tous les composants du mini-projet.
 */
public final class JsonUtil {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private JsonUtil() {}
}
