//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.airlift.http.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class JsonResponseHandler<T> implements ResponseHandler<T> {
    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");
    private final JsonCodec<T> jsonCodec;
    private final Set<Object> successfulResponseCodes;

    public static <T> JsonResponseHandler<T> createJsonResponseHandler(JsonCodec<T> jsonCodec) {
        return new JsonResponseHandler(jsonCodec);
    }

    public static <T> JsonResponseHandler<T> createJsonResponseHandler(JsonCodec<T> jsonCodec, int firstSuccessfulResponseCode, int... otherSuccessfulResponseCodes) {
        return new JsonResponseHandler(jsonCodec, firstSuccessfulResponseCode, otherSuccessfulResponseCodes);
    }

    private JsonResponseHandler(JsonCodec<T> jsonCodec) {
        this(jsonCodec, 200, new int[]{201, 202, 203, 204, 205, 206});
    }

    private JsonResponseHandler(JsonCodec<T> jsonCodec, int firstSuccessfulResponseCode, int... otherSuccessfulResponseCodes) {
        this.jsonCodec = jsonCodec;
        this.successfulResponseCodes = ImmutableSet.builder().add(firstSuccessfulResponseCode).addAll(Ints.asList(otherSuccessfulResponseCodes)).build();
    }


    @Override
    public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
        StatusLine statusLine = response.getStatusLine();
        if (!this.successfulResponseCodes.contains(Integer.valueOf(statusLine.getStatusCode()))) {
            throw new UnexpectedResponseException(String.format("Expected response code to be %s, but was %d: %s", new Object[]{this.successfulResponseCodes, Integer.valueOf(statusLine.getStatusCode()), statusLine.getReasonPhrase()}), response);
        } else {
            Header firstHeader = response.getFirstHeader("Content-Type");
            String contentType = firstHeader != null ? firstHeader.getValue() : null;
            if (contentType == null) {
                throw new UnexpectedResponseException("Content-Type is not set for response", response);
            } else if (!MediaType.parse(contentType).is(MEDIA_TYPE_JSON)) {
                throw new UnexpectedResponseException("Expected application/json response from server but got " + contentType, response);
            } else {
                byte[] bytes;
                try {
                    bytes = ByteStreams.toByteArray(response.getEntity().getContent());
                } catch (IOException var8) {
                    throw new RuntimeException("Error reading response from server");
                }

                try {
                    return this.jsonCodec.fromJson(bytes);
                } catch (IllegalArgumentException var7) {
                    String json = new String(bytes, StandardCharsets.UTF_8);
                    throw new IllegalArgumentException("Unable to create " + this.jsonCodec.getType() + " from JSON response:\n" + json, var7);
                }
            }
        }
    }


}
