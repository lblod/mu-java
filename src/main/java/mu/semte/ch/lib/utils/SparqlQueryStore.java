package mu.semte.ch.lib.utils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.SneakyThrows;

import java.io.StringReader;
import java.util.Map;

import static org.springframework.ui.freemarker.FreeMarkerTemplateUtils.processTemplateIntoString;

public interface SparqlQueryStore {
  Map<String, String> asMap();

  default String getQuery(String queryName) {
    return asMap().get(queryName);
  }

  default long size() {
    return asMap().size();
  }

  default boolean isPresent(String queryName) {
    return asMap().containsKey(queryName);
  }

  @SneakyThrows
  default String getQueryWithParameters(String queryName, Map<String, Object> parameters) {
    String query = getQuery(queryName);
    return computeQueryWithParameters(query, parameters);
  }

  static String computeQuery(String query, Object... parameters) {
    return query.formatted(parameters);
  }

  @SneakyThrows
  static String computeQueryWithParameters(String query, Map<String, Object> parameters) {
    Template template = new Template("name", new StringReader(query),
                                     new Configuration(Configuration.VERSION_2_3_30));
    return processTemplateIntoString(template, parameters);
  }
}
