package pl.schibsted.flume.interceptor.json;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(JUnit4.class)
public class JsonInterceptorTest {

    private static String BODY;

    private Event event;
    private Map<String, String> headers;
    private JsonInterceptor interceptor;

    @Before
    public void prepare() throws IOException {

        Charset encoding = StandardCharsets.UTF_8;
        String path = "./src/test/java/pl/schibsted/flume/interceptor/json/event.json";
        List<String> lines = Files.readAllLines(Paths.get(path), encoding);
        BODY = lines.get(0);

        headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        event = new JSONEvent();
        event.setBody(BODY.getBytes());
        event.setHeaders(headers);

    }

    @Test
    public void testInterception() {

        String headerName = "testName";
        String headerJSONPath = "$.published";

        interceptor = new JsonInterceptor(headerName, headerJSONPath);
        interceptor.initialize();

        Event interceptedEvent =
                interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                BODY,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain " + headerName,
                interceptedEvent.getHeaders().containsKey(headerName));

        String published = "2015-04-23T01:37:09+00:00";

        assertEquals("Header's " + headerName + " should be correct",
                published,
                interceptedEvent.getHeaders().get(headerName));

    }

}
