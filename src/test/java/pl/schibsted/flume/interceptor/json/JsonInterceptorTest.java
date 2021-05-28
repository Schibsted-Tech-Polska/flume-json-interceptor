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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(JUnit4.class)
public class JsonInterceptorTest {

    @Before
    public void prepare() {
    }

    private JsonInterceptor getInterceptor(Context context) {
        JsonInterceptor.Builder interceptorBuilder = new JsonInterceptor.Builder();
        interceptorBuilder.configure(context);

        JsonInterceptor interceptor = interceptorBuilder.build();
        interceptor.initialize();
        return interceptor;
    }

    private String getDefaultEventBody() {
        return "{ " +
                "\"pageViewId\":\"4eae0122-052d-41ff-ac5c-120279891184\"," +
                "\"published\":\"2015-04-23T01:37:09+00:00\"," +
                "\"finished\":\"1429753029000\"," +
                "\"params\": {" +
                "\"v1\":\"1\"," +
                "\"v2\":\"2\"," +
                "\"v3\":\"3\"" +
                "}" +
                " }";
    }

    private String getInvalidEventBody() {
        return "{ \"pageViewId\":\"4eae0122-052d-41ff-ac5c-120279891184\",";
    }

    private Event getEvent(Map<String, String> headers, String body) {
        Event event = new JSONEvent();
        event.setBody(body.getBytes());
        event.setHeaders(headers);
        return event;
    }

    private Context getDefaultContext(String headerName, String headerJSONPath) {
        Context context = new Context();
        context.put("serializers", "s1");
        context.put("serializers.s1.name", "s1");
        if (!headerName.isEmpty()) {
            context.put("name", headerName);
        }
        if (!headerJSONPath.isEmpty()) {
            context.put("jsonpath", headerJSONPath);
        }
        return context;
    }

    @Test
    public void testBasicChecks() {

        String headerName = "testName";
        String headerJSONPath = "$.published";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent =
                interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                body,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain " + headerName,
                interceptedEvent.getHeaders().containsKey(headerName));

        String published = "2015-04-23T01:37:09+00:00";

        assertEquals("Header's " + headerName + " should be correct",
                published,
                interceptedEvent.getHeaders().get(headerName));

    }

    @Test
    public void testIncorrectJSONPathShoudNotChangeEvent() {

        String headerName = "testName";
        String headerJSONPath = "$.notExists";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals(interceptedEvent, event);
    }

    @Test
    public void testNonScalarResultShoudNotChangeEvent() {

        String headerName = "testName";
        String headerJSONPath = "$.params";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals(interceptedEvent, event);
    }

    @Test
    public void testInvalidEventBodyShoudNotChangeEvent() {

        String headerName = "testName";
        String headerJSONPath = "$.published";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getInvalidEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals(interceptedEvent, event);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testMissedHeaderName() {

        String headerName = "";
        String headerJSONPath = "$.published";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getInvalidEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        interceptor.intercept(event);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void testMissedJSONPath() {

        String headerName = "testName";
        String headerJSONPath = "";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getInvalidEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);

        JsonInterceptor interceptor = getInterceptor(context);

        interceptor.intercept(event);
    }

    @Test
    public void testMillisSerializer() {

        String headerName = "testName";
        String headerJSONPath = "$.published";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);
        context.put("serializers.s1.type", "pl.schibsted.flume.interceptor.json.JsonInterceptorMillisSerializer");
        context.put("serializers.s1.pattern", "yyyy-MM-dd'T'HH:mm:ssZ");

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                body,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain " + headerName,
                interceptedEvent.getHeaders().containsKey(headerName));

        String published = "1429753029000"; // => 2015-04-23T01:37:09+00:00

        assertEquals("Header's " + headerName + " should be correct",
                published,
                interceptedEvent.getHeaders().get(headerName));
    }

    @Test
    public void testDateTimeFormatSerializer() {

        String headerName = "testName";
        String headerJSONPath = "$.published";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);
        context.put("serializers.s1.type", "pl.schibsted.flume.interceptor.json.JsonInterceptorDateTimeFormatSerializer");
        context.put("serializers.s1.inputpattern", "yyyy-MM-dd'T'HH:mm:ssZ");
        context.put("serializers.s1.outputpattern", "yyyy-MM-dd HH:mm:ss");

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                body,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain " + headerName,
                interceptedEvent.getHeaders().containsKey(headerName));

        String published = "2015-04-22 20:37:09"; // => 2015-04-23T01:37:09+00:00

        assertEquals("Header's " + headerName + " should be correct",
                published,
                interceptedEvent.getHeaders().get(headerName));
    }

    @Test
    public void testMillisecondFormatSerializer(){
        String headerName = "testName";
        String headerJSONPath = "$.finished";

        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        String body = getDefaultEventBody();
        Event event = getEvent(headers, body);

        Context context = getDefaultContext(headerName, headerJSONPath);
        context.put("serializers.s1.type", "pl.schibsted.flume.interceptor.json.JsonInterceptorMillisecondFormatSerializer");
        context.put("serializers.s1.outputpattern", "yyyy-MM-dd HH:mm:ss");

        JsonInterceptor interceptor = getInterceptor(context);

        Event interceptedEvent = interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                body,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain " + headerName,
                interceptedEvent.getHeaders().containsKey(headerName));

        String finished = "2015-04-23 09:37:09";

        assertEquals("Header's " + headerName + " should be correct",
                finished,
                interceptedEvent.getHeaders().get(headerName));
    }

}
