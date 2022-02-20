package ch.ichristen.avroUtil.util;

import org.apache.commons.compress.utils.IOUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.lang.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class HttpOutputMessageImpl implements HttpOutputMessage {

    private final HttpHeaders headers;

    @Nullable
    private final OutputStream body;

    public HttpOutputMessageImpl(HttpHeaders headers) {
        this.headers = headers;
        this.body = new ByteArrayOutputStream();
    }

    @Override
    public OutputStream getBody() throws IOException {
        return body;
    }

    public byte[] getByteArray() throws IOException {
        return ((ByteArrayOutputStream)getBody()).toByteArray();
    }

    @Override
    public HttpHeaders getHeaders() {
        return headers;
    }
}
