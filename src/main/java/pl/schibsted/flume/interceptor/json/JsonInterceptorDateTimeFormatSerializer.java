package pl.schibsted.flume.interceptor.json;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import com.google.common.base.Preconditions;

public class JsonInterceptorDateTimeFormatSerializer implements
        JsonInterceptorSerializer {
    private DateTimeFormatter inputFormatter;
    private DateTimeFormatter outputFormatter;
    @Override
    public void configure(Context context) {
        String inputPattern = context.getString("inputpattern");
        String outputPattern = context.getString("outputpattern");
        Preconditions.checkArgument(!StringUtils.isEmpty(inputPattern),
                "Must configure with a valid inputpattern");
        Preconditions.checkArgument(!StringUtils.isEmpty(outputPattern),
                "Must configure with a valid outputpattern");
        inputFormatter = DateTimeFormat.forPattern(inputPattern);
        outputFormatter = DateTimeFormat.forPattern(outputPattern);
    }
    @Override
    public String serialize(String value) {
        DateTime dateTime = inputFormatter.parseDateTime(value);
        return outputFormatter.print(dateTime.getMillis());
    }
    @Override
    public void configure(ComponentConfiguration conf) {
    }
}
