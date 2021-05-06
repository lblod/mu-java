package mu.semte.ch.lib.config;

import mu.semte.ch.lib.config.config.DummySpringContext;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = DummySpringContext.class)
class CoreConfigTest {

  @Autowired
  private SparqlQueryStore queryStore;

  @Test
  void sparqlQueryLoader() {
    assertEquals(1, queryStore.size());
    assertTrue(queryStore.isPresent("testQuery"));
    assertEquals("select ?s ?p ?o where {?s ?p ?o}", queryStore.getQuery("testQuery"));
  }
}
