import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CustomLogFormatter extends Formatter {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(dateFormat.format(new Date(record.getMillis())));
        sb.append(" [").append(record.getLevel()).append("] ");
        sb.append(record.getLoggerName()).append(" - ");
        sb.append(formatMessage(record));
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}
