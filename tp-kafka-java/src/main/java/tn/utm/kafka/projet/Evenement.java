package tn.utm.kafka.projet;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.List;

/**
 * Mini-projet — modele d'un evenement caisse (POS).
 * Type : VENTE | RETOUR | OUVERTURE
 */
public class Evenement {

    public enum Type { VENTE, RETOUR, OUVERTURE }

    private Type type;
    private String idCaisse;
    private String ville;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    private double montant;          // 0 pour OUVERTURE
    private List<String> produits;   // null pour OUVERTURE

    public Evenement() {}

    public Evenement(Type type, String idCaisse, String ville,
                     Instant timestamp, double montant, List<String> produits) {
        this.type = type;
        this.idCaisse = idCaisse;
        this.ville = ville;
        this.timestamp = timestamp;
        this.montant = montant;
        this.produits = produits;
    }

    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }

    public String getIdCaisse() { return idCaisse; }
    public void setIdCaisse(String idCaisse) { this.idCaisse = idCaisse; }

    public String getVille() { return ville; }
    public void setVille(String ville) { this.ville = ville; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public double getMontant() { return montant; }
    public void setMontant(double montant) { this.montant = montant; }

    public List<String> getProduits() { return produits; }
    public void setProduits(List<String> produits) { this.produits = produits; }

    @Override
    public String toString() {
        return type + "[" + idCaisse + "/" + ville + ", " + montant + " DT, " + timestamp + "]";
    }
}
