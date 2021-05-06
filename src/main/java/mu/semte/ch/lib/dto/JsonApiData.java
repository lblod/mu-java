package mu.semte.ch.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JsonApiData {
  private String type;
  private String id;
  private Map<String, String> attributes;
}
