package org.apache.kafka.streams.statestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.model.SaleState;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;

import static spark.Spark.get;
import static spark.Spark.port;

/**
 * @author arthur
 */
public class QueryableStateStoreServer {
    private final KafkaStreams streams;
    private final String stateStoreName;

    public QueryableStateStoreServer(KafkaStreams streams, String stateStoreName) {
        this.streams = streams;
        this.stateStoreName = stateStoreName;
    }

    //默认端口为4567
    public void start() {
        port(Integer.parseInt(System.getProperty("port", "4567")));
        Thread thread = new Thread(() -> {
            StoreQueryParameters<ReadOnlyKeyValueStore<String, SaleState>> parameters =
                    StoreQueryParameters.fromNameAndType(stateStoreName, QueryableStoreTypes.keyValueStore());
            ReadOnlyKeyValueStore<String, SaleState> keyValueStore = streams.store(parameters);
            get("/sale-state", (request, response) -> {
                response.type("application/json");
                ArrayList<SaleState> lst = new ArrayList<>();
                keyValueStore.all().forEachRemaining(e -> lst.add(e.value));
                return new ObjectMapper().writeValueAsString(lst);
            });
        });
        thread.setDaemon(true);
        thread.start();
    }
}
