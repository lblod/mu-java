package mu.semte.ch.lib.shacl;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.engine.TargetType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.config.config.DummySpringContext;

import static mu.semte.ch.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.lib.utils.ModelUtils.toModel;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Collectors;

@SpringBootTest(classes = DummySpringContext.class)
@Slf4j
public class ShaclServiceTest {
  @Value("classpath:shacl/application-profile.ttl")
  private Resource applicationProfile;

  @Test
  public void testProfile() throws Exception {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
        filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    var shapes = Shapes.parse(shapesGraph);
    var targetClasses = ShaclService.getTargetClasses(shapes);
    log.info("{}", targetClasses);

    assertTrue(targetClasses
        .contains("https://data.vlaanderen.be/id/concept/BesluitType/fb21d14b-734b-48f4-bd4e-888163fd08e8"));
  }
}
