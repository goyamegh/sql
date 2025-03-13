package org.opensearch.sql.directquery.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response class for direct query resources.
 *
 * @param <T> The type of data contained in the response
 */
@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetDirectQueryResourcesResponse<T> {
  private T data;

  private GetDirectQueryResourcesResponse(T data) {
    this.data = data;
  }

  public static GetDirectQueryResourcesResponse<List<String>> withStringList(List<String> data) {
    return new GetDirectQueryResourcesResponse<>(data);
  }

  public static GetDirectQueryResourcesResponse<List<Map<String, String>>> withStringMapList(
      List<Map<String, String>> data) {
    return new GetDirectQueryResourcesResponse<>(data);
  }

  public static <V> GetDirectQueryResourcesResponse<Map<String, V>> withMap(Map<String, V> data) {
    return new GetDirectQueryResourcesResponse<>(data);
  }
}
