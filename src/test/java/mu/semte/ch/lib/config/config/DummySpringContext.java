package mu.semte.ch.lib.config.config;

import mu.semte.ch.lib.config.CoreConfig;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
@Import(CoreConfig.class)
public class DummySpringContext {
}
