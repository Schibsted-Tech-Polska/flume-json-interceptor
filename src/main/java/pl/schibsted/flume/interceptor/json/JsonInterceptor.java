/*
 * Copyright 2015 Schibsted Tech Polska Sp. z o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.schibsted.flume.interceptor.json;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.base.Charsets;
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

import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.CONFIG_SERIALIZERS;
import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.CONFIG_HEADER_NAME;
import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.CONFIG_HEADER_JSONPATH;
import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.DEFAULT_SERIALIZER;
import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.CONFIG_SERIALIZER_TYPE;
import static pl.schibsted.flume.interceptor.json.JsonInterceptor.Constants.CONFIG_SERIALIZER_NAME;

public class JsonInterceptor implements Interceptor {
    private static final Logger logger =
            LoggerFactory.getLogger(JsonInterceptor.class);

    private String headerName;
    private String headerJSONPath;
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

            String body = new String(event.getBody(), Charsets.UTF_8);
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
            headerName = context.getString(CONFIG_HEADER_NAME);
            headerJSONPath = context.getString(CONFIG_HEADER_JSONPATH);

            configureSerializers(context);
        }

        @Override
        public JsonInterceptor build() {
            Preconditions.checkArgument(headerName != null, "Header name was misconfigured");
            Preconditions.checkArgument(headerJSONPath != null, "Header JSONPath was misconfigured");
            return new JsonInterceptor(headerName, headerJSONPath, serializer);
        }

        private void configureSerializers(Context context) {
            String serializerListStr = context.getString(CONFIG_SERIALIZERS);
            if (StringUtils.isEmpty(serializerListStr)) {
                serializer = defaultSerializer;
                return;
            }

            String[] serializerNames = serializerListStr.split("\\s+");
            if (serializerNames.length > 1) {
                logger.warn("Only one serializer is supported.");
            }
            String serializerName = serializerNames[0];

            Context serializerContexts = new Context(context.getSubProperties(CONFIG_SERIALIZERS + "."));
            Context serializerContext = new Context(serializerContexts.getSubProperties(serializerName + "."));

            String type = serializerContext.getString(CONFIG_SERIALIZER_TYPE, DEFAULT_SERIALIZER);
            String name = serializerContext.getString(CONFIG_SERIALIZER_NAME);

            Preconditions.checkArgument(!StringUtils.isEmpty(name), "Supplied name cannot be empty.");
            if (DEFAULT_SERIALIZER.equals(type)) {
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
    }


    public static class Constants {

        public static final String CONFIG_SERIALIZERS = "serializers";
        public static final String DEFAULT_SERIALIZER = "DEFAULT";
        public static final String CONFIG_HEADER_NAME = "name";
        public static final String CONFIG_HEADER_JSONPATH = "jsonpath";
        public static final String CONFIG_SERIALIZER_TYPE = "type";
        public static final String CONFIG_SERIALIZER_NAME = "name";
    }
}
