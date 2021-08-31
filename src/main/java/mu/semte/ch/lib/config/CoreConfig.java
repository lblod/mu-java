package mu.semte.ch.lib.config;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.handler.DefaultExceptionHandler;
import mu.semte.ch.lib.utils.SparqlClient;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.github.jsonldjava.shaded.com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.text.CaseUtils.toCamelCase;

@Configuration
@Import({SparqlClient.class, DefaultExceptionHandler.class})
@EnableAsync
@EnableScheduling
@Slf4j
public class CoreConfig implements AsyncConfigurer {

    @Value("${sparql.queryStore.path:classpath*:sparql/*.sparql}")
    private Resource[] queries;


    @Override
    public Executor getAsyncExecutor() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public SparqlQueryStore sparqlQueryLoader() {
        log.info("Adding {} queries to the store", queries.length);

        var queriesMap = Arrays.stream(queries)
                .map(r -> {
                    try {
                        var key = toCamelCase(removeExtension(r.getFilename()), false, '-');
                        return immutableEntry(key, IOUtils.toString(r.getInputStream(), UTF_8));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .peek(e -> log.info("query {} added to the store", e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return () -> queriesMap;
    }


}
