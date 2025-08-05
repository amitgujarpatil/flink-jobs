package org.core;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RestClient {

    private String baseUri;
    private HttpClient client;
    private ObjectMapper objectMapper = new ObjectMapper();


    public RestClient(String baseUri){
        this.baseUri = baseUri;
        this.client = HttpClient.newHttpClient();
    }

    public <T> T GET(String pathUri, Class<T> responseType) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + pathUri))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            return objectMapper.readValue(response.body(), responseType);
        } else {
            // Handle error responses
            throw new RuntimeException("API call failed with status: " + response.statusCode());
        }
    }
}
