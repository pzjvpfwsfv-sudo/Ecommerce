package com.ecommerce.quality.real;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public final class RealEventCodec {
    public static final int MAX_BYTES = 65536;
    private static final List<String> BUSINESS = List.of("brand", "category_code", "category_id",
            "event_time", "event_type", "price", "product_id", "user_id", "user_session");
    private static final Set<String> OPTIONAL = Set.of("brand", "category_code", "category_id", "user_session");
    private static final Set<String> FIELDS = Set.of("event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session", "schema_version", "event_id",
            "dataset_id", "source_file", "source_row_number", "replay_schema_version", "replay_id", "replayed_at");
    private final ObjectMapper mapper = new ObjectMapper();

    public RealEventCodec() {
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        mapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(8).maxStringLength(MAX_BYTES).maxNumberLength(32).build());
    }

    public RealEvent parse(KafkaEnvelope input) {
        if (input.payload == null) throw invalid("MALFORMED_JSON");
        if (input.payload.length > MAX_BYTES) throw invalid("MESSAGE_TOO_LARGE");
        String json;
        try {
            json = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(input.payload)).toString();
        } catch (CharacterCodingException exception) {
            throw invalid("INVALID_UTF8");
        }
        ObjectNode root = object(json);
        Set<String> names = new HashSet<>();
        root.fieldNames().forEachRemaining(names::add);
        if (!names.equals(FIELDS)) throw invalid("INVALID_FIELDS");
        for (String version : List.of("schema_version", "replay_schema_version")) {
            if (!root.get(version).isIntegralNumber() || !root.get(version).canConvertToInt()
                    || root.get(version).intValue() != 1) throw invalid("INVALID_VERSION");
        }
        for (String field : BUSINESS) text(root, field, OPTIONAL.contains(field));
        String type = root.get("event_type").textValue();
        if (!Set.of("view", "cart", "remove_from_cart", "purchase").contains(type)) throw invalid("INVALID_EVENT_TYPE");
        if (!root.get("price").textValue().matches("(?:0|[1-9][0-9]{0,17})\\.[0-9]{2}")) throw invalid("INVALID_PRICE");
        String eventTime = root.get("event_time").textValue();
        long millis;
        try {
            if (!eventTime.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\+00:00")) {
                throw new IllegalArgumentException();
            }
            millis = OffsetDateTime.parse(eventTime).toInstant().toEpochMilli();
        } catch (RuntimeException exception) {
            throw invalid("INVALID_EVENT_TIME");
        }
        String dataset = text(root, "dataset_id", false);
        String source = text(root, "source_file", false);
        JsonNode ordinal = root.get("source_row_number");
        if (!dataset.equals("rees46-multicategory-2019-2020")
                || !Set.of("2019-Oct.csv.gz", "2019-Nov.csv.gz").contains(source)
                || !ordinal.isIntegralNumber() || !ordinal.canConvertToLong() || ordinal.longValue() < 1
                || !eventTime.startsWith(source.equals("2019-Oct.csv.gz") ? "2019-10-" : "2019-11-")) {
            throw invalid("INVALID_SOURCE");
        }
        validateReplay(root);
        String eventId = text(root, "event_id", false);
        StringBuilder canonical = new StringBuilder("[").append(quote(dataset)).append(',')
                .append(quote(source)).append(',').append(ordinal.longValue()).append(",{");
        for (int i = 0; i < BUSINESS.size(); i++) {
            String name = BUSINESS.get(i);
            if (i > 0) canonical.append(',');
            canonical.append(quote(name)).append(':');
            canonical.append(root.get(name).isNull() ? "null" : quote(root.get(name).textValue()));
        }
        canonical.append("}]");
        if (!eventId.equals("real_" + sha256(canonical.toString().getBytes(StandardCharsets.UTF_8)))) {
            throw invalid("INVALID_EVENT_ID");
        }
        RealEvent event = new RealEvent();
        event.json = json;
        event.datasetId = dataset;
        event.eventId = eventId;
        event.userId = root.get("user_id").textValue();
        event.eventTimeMillis = millis;
        event.topic = input.topic;
        event.partition = input.partition;
        event.offset = input.offset;
        return event;
    }

    private void validateReplay(ObjectNode root) {
        try {
            String id = text(root, "replay_id", false);
            String at = text(root, "replayed_at", false);
            if (!UUID.fromString(id).toString().equals(id)
                    || !OffsetDateTime.parse(at).getOffset().equals(ZoneOffset.UTC)) throw new IllegalArgumentException();
        } catch (RuntimeException exception) {
            throw invalid("INVALID_REPLAY_METADATA");
        }
    }

    private static String text(ObjectNode root, String name, boolean nullable) {
        JsonNode node = root.get(name);
        if (nullable && node.isNull()) return null;
        if (!node.isTextual()) throw invalid("INVALID_FIELD");
        String value = node.textValue();
        if (value.isEmpty() || value.codePointCount(0, value.length()) > 512
                || whitespace(value.codePointAt(0)) || whitespace(value.codePointBefore(value.length()))) {
            throw invalid("INVALID_FIELD");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) throw invalid("INVALID_FIELD");
            } else if (Character.isLowSurrogate(c)) throw invalid("INVALID_FIELD");
        }
        return value;
    }

    private static boolean whitespace(int c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == 0x85;
    }

    // Python json.dumps(ensure_ascii=True) uses lowercase UTF-16 escapes, including DEL.
    private static String quote(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (c < 32 || c >= 127) result.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    else result.append(c);
                }
            }
        }
        return result.append('"').toString();
    }

    public String rejection(KafkaEnvelope input, String reason) {
        byte[] bytes = input.payload == null ? new byte[0] : input.payload;
        String prefix = new String(bytes, 0, Math.min(bytes.length, 8192), StandardCharsets.UTF_8);
        int length = Math.min(2048, prefix.codePointCount(0, prefix.length()));
        String preview = prefix.substring(0, prefix.offsetByCodePoints(0, length));
        ObjectNode result = coordinates(input);
        result.put("reason_code", reason);
        result.put("raw_preview", preview);
        result.put("raw_truncated", bytes.length > 8192 || prefix.codePointCount(0, prefix.length()) > length);
        result.put("raw_bytes", bytes.length);
        result.put("raw_is_null", input.payload == null);
        result.put("raw_sha256", sha256(bytes));
        if (bytes.length <= MAX_BYTES) {
            try {
                ObjectNode inputJson = object(new String(bytes, StandardCharsets.UTF_8));
                for (String name : List.of("event_id", "replay_id")) {
                    JsonNode id = inputJson.get(name);
                    if (id != null && id.isTextual() && id.textValue().length() <= 80) result.set(name, id);
                }
            } catch (InvalidEvent ignored) {
                // Malformed input still has its Kafka coordinates and content digest.
            }
        }
        return serialize(result);
    }

    public String late(RealEvent event, long watermark) {
        ObjectNode result = coordinates(event.envelope());
        result.put("reason_code", "LATE_EVENT");
        result.set("event", object(event.json));
        result.put("watermark", watermark);
        result.put("lateness_ms", watermark - event.eventTimeMillis);
        return serialize(result);
    }

    private ObjectNode coordinates(KafkaEnvelope input) {
        ObjectNode result = mapper.createObjectNode();
        result.put("source_topic", input.topic);
        result.put("source_partition", input.partition);
        result.put("source_offset", input.offset);
        result.put("job_version", "graduation-g2b-v1");
        result.put("observed_at", Instant.now().toString());
        return result;
    }

    private ObjectNode object(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            if (node instanceof ObjectNode object) return object;
        } catch (JsonProcessingException exception) {
            throw invalid("MALFORMED_JSON");
        }
        throw invalid("MALFORMED_JSON");
    }

    private String serialize(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("quality envelope serialization failed", exception);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static InvalidEvent invalid(String code) { return new InvalidEvent(code); }

    public static final class InvalidEvent extends IllegalArgumentException {
        private final String code;
        private InvalidEvent(String code) { super(code); this.code = code; }
        public String code() { return code; }
    }
}
