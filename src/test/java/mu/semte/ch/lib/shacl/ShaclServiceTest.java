package mu.semte.ch.lib.shacl;

import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.config.config.DummySpringContext;
import mu.semte.ch.lib.utils.ModelUtils;

import static mu.semte.ch.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.lib.utils.ModelUtils.toModel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = DummySpringContext.class)
@Slf4j
public class ShaclServiceTest {
  @Value("classpath:shacl/application-profile.ttl")
  private Resource applicationProfile;

  @Value("classpath:shacl/example.ttl")
  private Resource exampleTtl;

  @Value("classpath:shacl/example2.ttl")
  private Resource example2Ttl;

  @Value("classpath:shacl/example3.ttl")
  private Resource example3Ttl;

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

  @Test
  public void testFilterBesluitType() throws Exception {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
        filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    var shapes = Shapes.parse(shapesGraph);
    var service = new ShaclService(shapes, true);
    var model = toModel(exampleTtl.getInputStream(), Lang.TURTLE);
    var filteredModel = service.filter(exampleTtl.getInputStream(), Lang.TURTLE);
    log.info("model len: {}, filtered model len: {}", model.size(), filteredModel.size());

    log.info("filtered model : \n{}",
        ModelUtils.toString(ModelFactory.createModelForGraph(filteredModel), Lang.TURTLE));
    assertEquals(model.size(), filteredModel.size());
  }

  @Test
  public void testFilterClassDefinedNotDefined() throws Exception {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
        filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    var shapes = Shapes.parse(shapesGraph);
    var service = new ShaclService(shapes, true);
    var model = toModel(example2Ttl.getInputStream(), Lang.TURTLE);
    var filteredModel = service.filter(example2Ttl.getInputStream(), Lang.TURTLE);
    log.info("model len: {}, filtered model len: {}", model.size(), filteredModel.size());

    log.info("filtered model : \n{}",
        ModelUtils.toString(ModelFactory.createModelForGraph(filteredModel), Lang.TURTLE));
    assertEquals(model.size(), filteredModel.size());
  }

  @Test
  public void testFilterClassDefinedNotDefined2() throws Exception {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
        filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    var shapes = Shapes.parse(shapesGraph);
    var service = new ShaclService(shapes, true);
    var model = toModel(example3Ttl.getInputStream(), Lang.TURTLE);
    var filteredModel = service.filter(example3Ttl.getInputStream(), Lang.TURTLE);
    log.info("model len: {}, filtered model len: {}", model.size(), filteredModel.size());

    log.info("filtered model : \n{}",
        ModelUtils.toString(ModelFactory.createModelForGraph(filteredModel), Lang.TURTLE));
    assertNotEquals(model.size(), filteredModel.size());
    model.removeAll(ResourceFactory.createResource("http://bittich.be/xx"), null, null);
    assertEquals(model.size(), filteredModel.size());

  }
}
