package pl.schibsted.flume.interceptor.json;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JsonInterceptor.class);

    private String headerName = "testName";
    private String headerJSONPath = "testValue";


    public JsonInterceptor(String headerName, String headerJSONPath) {
        this.headerName = headerName;
        this.headerJSONPath = headerJSONPath;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {

        String body = new String(event.getBody());

        Map<String, String> headers = event.getHeaders();

        String value = JsonPath.read(body, headerJSONPath);

        headers.put(headerName, value);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
    }

    public static class Builder
            implements Interceptor.Builder {

        private String headerName;
        private String headerJSONPath;
        private List<NameAndSerializer> serializerList;
        private final JsonInterceptorSerializer defaultSerializer = new JsonInterceptorPassThroughSerializer();

        @Override
        public void configure(Context context) {
            headerName = context.getString("name");
            headerJSONPath = context.getString("jsonpath");

            configureSerializers(context);
        }

        private void configureSerializers(Context context) {
            String serializerListStr = context.getString(SERIALIZERS);
            Preconditions.checkArgument(!StringUtils.isEmpty(serializerListStr),
                    "Must supply at least one name and serializer");
            String[] serializerNames = serializerListStr.split("\\s+");
            Context serializerContexts =
                    new Context(context.getSubProperties(SERIALIZERS + "."));
            serializerList = Lists.newArrayListWithCapacity(serializerNames.length);
            for(String serializerName : serializerNames) {
                Context serializerContext = new Context(
                        serializerContexts.getSubProperties(serializerName + "."));
                String type = serializerContext.getString("type", "DEFAULT");
                String name = serializerContext.getString("name");
                Preconditions.checkArgument(!StringUtils.isEmpty(name),
                        "Supplied name cannot be empty.");
                if("DEFAULT".equals(type)) {
                    serializerList.add(new NameAndSerializer(name, defaultSerializer));
                } else {
                    serializerList.add(new NameAndSerializer(name, getCustomSerializer(
                            type, serializerContext)));
                }
            }
        }

        private JsonInterceptorSerializer getCustomSerializer(
                String clazzName, Context context) {
            try {
                JsonInterceptorSerializer serializer = (JsonInterceptorSerializer) Class
                        .forName(clazzName).newInstance();
                serializer.configure(context);
                return serializer;
            } catch (Exception e) {
                LOGGER.error("Could not instantiate event serializer.", e);
                Throwables.propagate(e);
            }
            return defaultSerializer;
        }

        @Override
        public Interceptor build() {
            Preconditions.checkArgument(headerName != null,
                    "Header name was misconfigured");
            Preconditions.checkArgument(headerJSONPath != null,
                    "Header JSONPath name was misconfigured");
            return new JsonInterceptor(headerName, headerJSONPath);
        }
    }

    static class NameAndSerializer {
        private final String headerName;
        private final JsonInterceptorSerializer serializer;
        public NameAndSerializer(String headerName,
                                 JsonInterceptorSerializer serializer) {
            this.headerName = headerName;
            this.serializer = serializer;
        }
    }
}
