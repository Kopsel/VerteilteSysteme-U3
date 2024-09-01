public class LogEntry {

    private final int term;
    private final String command;

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return term;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", command='" + command + "'}";
    }
}