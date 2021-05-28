package pl.schibsted.flume.interceptor.json;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class JsonInterceptorMillisecondFormatSerializer implements JsonInterceptorSerializer {

    private DateTimeFormatter outputFormatter;

    @Override
    public void configure(Context context) {
        String outputPattern = context.getString("outputpattern");
        Preconditions.checkArgument(
                !StringUtils.isEmpty(outputPattern), "Must configure with a valid outputpattern");
        outputFormatter = DateTimeFormat.forPattern(outputPattern);
    }

    @Override
    public String serialize(String value) {
        Long millisecond = Long.valueOf(value);
        return outputFormatter.print(millisecond);
    }

    @Override
    public void configure(ComponentConfiguration componentConfiguration) {

    }
}
