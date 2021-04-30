package mu.semte.ch.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileDataObject {
  private String graph;
  private String physicalId;
  private String logicalId;
  private String created;
  private String creator;
  private String physicalFile;
  private String logicalFile;
  private String physicalFileName;
  private String fileSize;
  private String logicalFileName;
  private String fileExtension;
  private String contentType;
}
