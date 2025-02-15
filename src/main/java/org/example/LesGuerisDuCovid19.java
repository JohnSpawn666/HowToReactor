package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@SpringBootApplication
public class LesGuerisDuCovid19 implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(LesGuerisDuCovid19.class);
    private static final DateTimeFormatter DATE_PARAM_FORMATER = DateTimeFormatter.ISO_DATE;
    private static final String SOURCE_INTERESSANTE = "ministere-sante";

    // Initialisation du WebClient
    private final WebClient httpClient = WebClient.create("http://coronavirusapi-france.now.sh");

    public static void main(String[] args) {

        var app = new SpringApplication(LesGuerisDuCovid19.class);
        app.setWebApplicationType(WebApplicationType.NONE);     // Ne lance pas le serveur web
        app.run(args);

    }

    @Override
    public void run(String... args) throws Exception {

        var hier = LocalDate.now().minusDays(1);
        var avantHier = hier.minusDays(1);

        Integer nouveauxGueris =
                // Déclenchement des 2 requêtes en parallèle, et jointure des résultats
                Mono.zip(
                                getNombreGueris(hier, SOURCE_INTERESSANTE),
                                getNombreGueris(avantHier, SOURCE_INTERESSANTE))
                        .doFirst(() -> LOG.info("Récupération des données entre hier et avant-hier..."))
                        .map(tuple -> tuple.getT1() - tuple.getT2())
                        .onErrorResume(error -> Mono.fromRunnable(
                                () -> LOG.error("Problème lors  de la récupération des données : {}", error.getMessage())))
                        .block();

    }

    private Mono<Integer> getNombreGueris(LocalDate date, final String source) {
        // Récupération des données
        return httpClient.get()
                .uri(uri -> uri.path("/FranceGlobalDataByDate").queryParam("date", DATE_PARAM_FORMATER.format(date)).build())
                .retrieve()
                .bodyToMono(JsonNode.class)

                // Log lorsque le flux se déclenche
                .doFirst(() -> LOG.info("Récupération des données pour le {} sur la source {}...", date, source))

                // Log lorsqu'un élément est émis dans le flux
                .doOnNext(jsonData -> LOG.info("Données reçues : {}", jsonData.toPrettyString()))

                // Mapping de la données pour récupérer le contenu du champ
                .map(jsonData -> jsonData.get("FranceGlobalDataByDate"))

                //Génération d'un flux d'index mappé à partir d'un Mono
                .flatMapMany(dateByDate -> Flux.range(0, dateByDate.size()).map(dateByDate::get))

                // filtre de la données concernée
                .filter(dataByDate -> source.equals(dataByDate.get("sourceType").textValue())).next()

                // Mapping avec valeur possiblement nulle
                .flatMap(data -> Mono.justOrEmpty(data.get("gueris")))
                .map(JsonNode::intValue)

                // Déclenchement d'une erreur si valeur nulle a n'importe quel moment dans le flux
                .switchIfEmpty(Mono.error(new RuntimeException(String.format("Pas de données pour le %s sur la source %s", date.toString(), source))));
    }
}