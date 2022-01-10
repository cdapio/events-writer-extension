package context;

import java.util.Map;

/**
 * Context passed to {@link events.writer.EventWriter#initialize(EventWriterContext)}
 */
public interface EventWriterContext {
    /**
     * Properties are derived from the CDAP configuration.
     * @return unmodifiable properties for the events writer
     */
    Map<String, String> getProperties();
}
