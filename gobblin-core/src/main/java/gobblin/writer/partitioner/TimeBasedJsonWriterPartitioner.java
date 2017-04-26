package gobblin.writer.partitioner;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of {@link TimeBasedWriterPartitioner} for json records.
 *
 * @author Wallace
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeBasedJsonWriterPartitioner.class);

    private static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
    private final Optional<List<String>> partitionColumns;
    private Gson gson = new Gson();
    private DateFormat ISO8601_DATE_FORMAT;
    Pattern enterTimestampPattern = Pattern.compile("\"enter_timestamp\":\"([^\"]+)\"");

    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        this.partitionColumns = getTimestampToPathFormatter(state, numBranches, branchId);

        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        this.ISO8601_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        this.ISO8601_DATE_FORMAT.setTimeZone(timeZone);
    }

    private Optional<List<String>> getTimestampToPathFormatter(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
        if (state.contains(propName)) {
            List<String> props = state.getPropAsList(propName);
            for (String prop : props) {
                LOG.info("Get prop : " + prop);
            }
            return Optional.of(props);
        }
        return Optional.absent();
    }

    @Override
    public long getRecordTimestamp(byte[] record) {
        return getRecordTimestamp(getWriterPartitionColumnValue(record));
    }

    private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
        return writerPartitionColumnValue.orNull() instanceof Long
                ? (Long)writerPartitionColumnValue.get() * 1000 : System.currentTimeMillis();

    }

    private Optional<Object> getWriterPartitionColumnValue(byte[] record) {
        Optional<Object> fieldValue = Optional.absent();
        if (!this.partitionColumns.isPresent()) {
            return fieldValue;
        }

        for (String partitionColumn : this.partitionColumns.get()) {
            String recordValue;
            try {
                recordValue = new String(record, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOG.error("Unable to load UTF-8 encoding, falling back to system default", e);
                return fieldValue;
            }

            JsonObject jsonObject;
            try {
                jsonObject = gson.fromJson(recordValue, JsonObject.class);
            } catch (JsonSyntaxException e) {
                LOG.error("Exception when parsing JSON string " + recordValue, e);
                return fieldValue;
            }

            try {
                JsonElement element = jsonObject.get(partitionColumn);
                if (element != null) {
                    if (partitionColumn.equals("news_storage")) {
                        String news_storage_string = element.getAsString().replace("\\\"", "\"");
                        Matcher matcher = enterTimestampPattern.matcher(news_storage_string);
                        if (matcher.find()) {
                            String enterTimestamp = matcher.group(1);
                            Date date = ISO8601_DATE_FORMAT.parse(enterTimestamp);
                            LOG.debug(date.toString());
                            fieldValue = Optional.of((Object) Long.valueOf(date.getTime() / 1000L));
                        }
                    } else if (partitionColumn.equals("server_timestamp")) {
                        LOG.debug(String.valueOf(element.getAsLong()));
                        fieldValue = Optional.of((Object) Long.valueOf(element.getAsLong()));
                    }
                }
            } catch (Exception e) {
                LOG.error("Get value from JsonObject", e);
                continue;
            }
            if (fieldValue.isPresent()) {
                return fieldValue;
            }
        }

        return fieldValue;
    }

}
