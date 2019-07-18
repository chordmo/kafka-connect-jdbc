package io.confluent.connect.jdbc.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class SourceOffset {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
  protected static final String EXECUTE_TIME_FIELD = "execute_time";
  protected final Long executeTimeOffset;

  public SourceOffset(Long executeTimeOffset) {
    if (executeTimeOffset == null) {
      this.executeTimeOffset = -1L;
    } else {
      this.executeTimeOffset = executeTimeOffset;
    }
  }

  public static SourceOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return new SourceOffset(null);
    }
    Long time = (Long) map.get(EXECUTE_TIME_FIELD);
    return new SourceOffset(time);
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(1);
    if (executeTimeOffset != null) {
      map.put(EXECUTE_TIME_FIELD, executeTimeOffset);
    } else {
      map.put(EXECUTE_TIME_FIELD, -1L);
    }
    return map;
  }
}
