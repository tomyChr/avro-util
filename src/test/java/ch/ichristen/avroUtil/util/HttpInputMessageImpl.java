package ch.ichristen.avroUtil.util;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

public class HttpInputMessageImpl implements HttpInputMessage {

    private final HttpHeaders headers;

    @Nullable
    private final InputStream body;

    public HttpInputMessageImpl(HttpHeaders headers, byte[] body) throws IOException {
        this.headers = headers;
        this.body = new ByteArrayInputStream(body);
    }


    @Override
    public InputStream getBody() throws IOException {
        return body;
    }

    @Override
    public HttpHeaders getHeaders() {
        return headers;
    }
}
