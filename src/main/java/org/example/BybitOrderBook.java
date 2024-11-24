package org.example;
import javax.websocket.*;
import java.net.URI;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.TreeMap;
import com.google.gson.*;
import java.util.concurrent.CountDownLatch;

public class BybitOrderBook {

    private static final String WS_URL = "wss://stream.bybit.com/v5/public/spot";
    private static final String TOPIC = "orderbook.1.BTCUSDT";

    private final TreeMap<BigDecimal, BigDecimal> bids = new TreeMap<>(Collections.reverseOrder());
    private final TreeMap<BigDecimal, BigDecimal> asks = new TreeMap<>();

    private final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        new BybitOrderBook().start();
    }

    public void start() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            URI uri = new URI(WS_URL);
            container.connectToServer(new BybitEndpoint(), uri);
            latch.await(); // Keep the main thread alive
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @ClientEndpoint
    public class BybitEndpoint {

        @OnOpen
        public void onOpen(Session session) {
            try {
                // Send subscription message upon opening the WebSocket
                JsonObject subscribeMessage = new JsonObject();
                subscribeMessage.addProperty("op", "subscribe");
                JsonArray args = new JsonArray();
                args.add(TOPIC);
                subscribeMessage.add("args", args);

                session.getAsyncRemote().sendText(subscribeMessage.toString());
                System.out.println("Subscribed to " + TOPIC);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @OnMessage
        public void onMessage(String message, Session session) {
            // Parse the received message
            JsonElement jsonElement = JsonParser.parseString(message);

            if (jsonElement.isJsonObject()) {
                JsonObject msgObj = jsonElement.getAsJsonObject();

                if (msgObj.has("success") && msgObj.get("success").getAsBoolean()) {
                    System.out.println("Subscription successful.");
                } else if (msgObj.has("topic") && msgObj.has("type") && msgObj.has("data")) {
                    String messageType = msgObj.get("type").getAsString();
                    JsonObject data = msgObj.getAsJsonObject("data");

                    processOrderBookUpdate(messageType, data);
                }
            }
        }

        @OnError
        public void onError(Session session, Throwable throwable) {
            System.err.println("WebSocket Error: " + throwable.getMessage());
            throwable.printStackTrace();
            latch.countDown(); // Release the latch to stop the program
        }

        @OnClose
        public void onClose(Session session, CloseReason closeReason) {
            System.out.println("WebSocket closed: " + closeReason.getReasonPhrase());
            latch.countDown(); // Release the latch to stop the program
        }
    }

    private void processOrderBookUpdate(String type, JsonObject data) {
        if (type.equals("snapshot")) {
            // Reset local order book
            bids.clear();
            asks.clear();
            System.out.println("Received snapshot.");

            updateOrderBookEntries(bids, data.getAsJsonArray("b"));
            updateOrderBookEntries(asks, data.getAsJsonArray("a"));
        } else if (type.equals("delta")) {
            // Apply delta updates
            updateOrderBookEntries(bids, data.getAsJsonArray("b"));
            updateOrderBookEntries(asks, data.getAsJsonArray("a"));
        }

        displayTopLevels();
    }

    private void updateOrderBookEntries(TreeMap<BigDecimal, BigDecimal> orderBookSide, JsonArray updates) {
        if (updates == null) return; // Check for null updates
        for (JsonElement elem : updates) {
            JsonArray entry = elem.getAsJsonArray();
            BigDecimal price = new BigDecimal(entry.get(0).getAsString());
            BigDecimal size = new BigDecimal(entry.get(1).getAsString());

            if (size.compareTo(BigDecimal.ZERO) == 0) {
                orderBookSide.remove(price);
            } else {
                orderBookSide.put(price, size);
            }
        }
    }

    private void displayTopLevels() {
        if (!bids.isEmpty() && !asks.isEmpty()) {
            System.out.println("Best Bid: " + bids.firstEntry());
            System.out.println("Best Ask: " + asks.firstEntry());
        } else {
            System.out.println("Order book is empty.");
        }
    }
}
