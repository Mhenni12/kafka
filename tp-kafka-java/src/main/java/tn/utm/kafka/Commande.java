package tn.utm.kafka;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.List;

/**
 * Exercice 4.C — Classe metier pour la serialisation JSON.
 * 4 champs : id, date, articles, total.
 */
public class Commande {

    private String id;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant date;

    private List<String> articles;
    private double total;

    public Commande() { /* requis par Jackson */ }

    public Commande(String id, Instant date, List<String> articles, double total) {
        this.id = id;
        this.date = date;
        this.articles = articles;
        this.total = total;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Instant getDate() { return date; }
    public void setDate(Instant date) { this.date = date; }

    public List<String> getArticles() { return articles; }
    public void setArticles(List<String> articles) { this.articles = articles; }

    public double getTotal() { return total; }
    public void setTotal(double total) { this.total = total; }

    @Override
    public String toString() {
        return "Commande{" +
                "id='" + id + '\'' +
                ", date=" + date +
                ", articles=" + articles +
                ", total=" + total +
                '}';
    }
}
