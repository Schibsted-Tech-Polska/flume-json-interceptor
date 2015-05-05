package pl.schibsted.flume.interceptor.json;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonInterceptor implements Interceptor {
    static final String SERIALIZERS = "serializers";
    private static final Logger logger =
            LoggerFactory.getLogger(JsonInterceptor.class);

    private String headerName = "testName";
    private String headerJSONPath = "testValue";
    private final JsonInterceptorSerializer serializer;

    public JsonInterceptor(String headerName, String headerJSONPath, JsonInterceptorSerializer serializer) {
        this.headerName = headerName;
        this.headerJSONPath = headerJSONPath;
        this.serializer = serializer;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {

            String body = new String(event.getBody());
            Map<String, String> headers = event.getHeaders();
            String value = JsonPath.read(body, headerJSONPath);
            headers.put(headerName, serializer.serialize(value));

        } catch (com.jayway.jsonpath.PathNotFoundException e) {
            logger.warn("Skipping event due to: PathNotFoundException.", e);
        } catch (com.jayway.jsonpath.InvalidJsonException e) {
            logger.warn("Skipping event due to: InvalidJsonException.", e);
        } catch (java.lang.ClassCastException e) {
            logger.warn("Skipping event due to: ClassCastException.", e);
        } catch (Exception e) {
            logger.warn("Skipping event due to: unknown error.", e);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents = new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        private String headerName;
        private String headerJSONPath;
        private JsonInterceptorSerializer serializer;
        private final JsonInterceptorSerializer defaultSerializer = new JsonInterceptorPassThroughSerializer();

        @Override
        public void configure(Context context) {
            headerName = context.getString("name");
            headerJSONPath = context.getString("jsonpath");

            configureSerializers(context);
        }

        private void configureSerializers(Context context) {
            String serializerListStr = context.getString(SERIALIZERS);
            if (StringUtils.isEmpty(serializerListStr)) {
                serializer = defaultSerializer;
                return;
            }

            String[] serializerNames = serializerListStr.split("\\s+");
            if (serializerNames.length > 1) {
                logger.warn("Only one serializer is supported.");
            }
            String serializerName = serializerNames[0];
            Context serializerContexts = new Context(context.getSubProperties(SERIALIZERS + "."));
            Context serializerContext = new Context(serializerContexts.getSubProperties(serializerName + "."));

            String type = serializerContext.getString("type", "DEFAULT");
            String name = serializerContext.getString("name");

            Preconditions.checkArgument(!StringUtils.isEmpty(name), "Supplied name cannot be empty.");
            if ("DEFAULT".equals(type)) {
                serializer = defaultSerializer;
            } else {
                serializer = getCustomSerializer(type, serializerContext);
            }

        }

        private JsonInterceptorSerializer getCustomSerializer(String clazzName, Context context) {
            try {
                JsonInterceptorSerializer serializer = (JsonInterceptorSerializer) Class
                        .forName(clazzName).newInstance();
                serializer.configure(context);
                return serializer;
            } catch (Exception e) {
                logger.error("Could not instantiate event serializer.", e);
                Throwables.propagate(e);
            }
            return defaultSerializer;
        }

        @Override
        public JsonInterceptor build() {
            Preconditions.checkArgument(headerName != null, "Header name was misconfigured");
            Preconditions.checkArgument(headerJSONPath != null, "Header JSONPath was misconfigured");
            return new JsonInterceptor(headerName, headerJSONPath, serializer);
        }
    }
}
