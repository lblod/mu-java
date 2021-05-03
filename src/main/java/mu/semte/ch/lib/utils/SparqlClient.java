package mu.semte.ch.lib.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.riot.RDFLanguages;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.*;
import static mu.semte.ch.lib.Constants.*;
import static mu.semte.ch.lib.utils.RequestHelper.getCurrentHttpRequest;

@Service
@Slf4j
public class SparqlClient {

    @Value("${sparql.endpoint}")
    private String sparqlUrl;

    @Value("${sparql.authSudo:true}")
    private boolean isAuthSudo;

    public void insertModel(String graphUri, Model model) {
        var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
        String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
        executeUpdateQuery(updateQuery);
    }

    @SneakyThrows
    public void executeUpdateQuery(String updateQuery) {
        log.debug(updateQuery);
        try (var httpClient = buildHttpClient(); RDFConnection conn = RDFConnectionRemote.create()
                .destination(sparqlUrl)
                .httpClient(httpClient)
                .build()) {
            conn.update(updateQuery);
        }

    }

    @SneakyThrows
    public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
        log.debug(query);
        try (var httpClient = buildHttpClient();
             QueryExecution queryExecution = QueryExecutionFactory.sparqlService(sparqlUrl, query, httpClient)) {
            return resultHandler.apply(queryExecution.execSelect());
        }
    }

    public Model executeSelectQuery(String query) {
        return executeSelectQuery(query, resultSet -> {
            Model model = ModelFactory.createDefaultModel();
            resultSet.forEachRemaining(querySolution -> {
                RDFNode subject = querySolution.get("s");
                RDFNode predicate = querySolution.get("p");
                RDFNode object = querySolution.get("o");
                var triple = Triple.create(subject.asNode(), predicate.asNode(), object.asNode());
                model.getGraph().add(triple);
            });
            return model;
        });
    }

    @SneakyThrows
    public boolean executeAskQuery(String askQuery) {
        log.debug(askQuery);
        try (var httpClient = buildHttpClient();
             QueryExecution queryExecution = QueryExecutionFactory.sparqlService(sparqlUrl, askQuery, httpClient)) {
            return queryExecution.execAsk();
        }
    }

    public void dropGraph(String graphUri) {
        executeUpdateQuery("clear graph <" + graphUri + ">");
    }

    public CloseableHttpClient buildHttpClient() {
        Optional<BasicHeader> musSessionIdHeader = getCurrentHttpRequest().map(r -> r.getHeader(HEADER_MU_SESSION_ID)).map(h -> new BasicHeader(HEADER_MU_SESSION_ID, h));
        Optional<BasicHeader> musCallIdHeader = getCurrentHttpRequest().map(r -> r.getHeader(HEADER_MU_CALL_ID)).map(h -> new BasicHeader(HEADER_MU_CALL_ID, h));
        Optional<BasicHeader> muAuthSudo = isAuthSudo ? of(new BasicHeader(HEADER_MU_AUTH_SUDO, "true")) : empty();
        return HttpClients.custom()
                .setDefaultHeaders(Stream.of(musSessionIdHeader, musCallIdHeader, muAuthSudo)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList()))
                .build();

    }


}
