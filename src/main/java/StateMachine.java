import java.util.HashMap;
import java.util.Map;

public class StateMachine {
    private final Map<String, String> keyValueStore;

    public StateMachine() {
        this.keyValueStore = new HashMap<>();
    }

    public void apply(String command) {
        String[] parts = command.split("=");
        if (parts.length == 2) {
            String key = parts[0].trim();
            String value = parts[1].trim();
            keyValueStore.put(key, value);
        } else {
            throw new IllegalArgumentException("Invalid command format: " + command);
        }
    }

    public String get(String key) {
        return keyValueStore.get(key);
    }
}