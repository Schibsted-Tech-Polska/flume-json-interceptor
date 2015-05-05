package pl.schibsted.flume.interceptor.json;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import com.google.common.base.Preconditions;

public class JsonInterceptorMillisSerializer implements
        JsonInterceptorSerializer {
    private DateTimeFormatter formatter;
    @Override
    public void configure(Context context) {
        String pattern = context.getString("pattern");
        Preconditions.checkArgument(!StringUtils.isEmpty(pattern),
                "Must configure with a valid pattern");
        formatter = DateTimeFormat.forPattern(pattern);
    }
    @Override
    public String serialize(String value) {
        DateTime dateTime = formatter.parseDateTime(value);
        return Long.toString(dateTime.getMillis());
    }
    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP...
    }
}