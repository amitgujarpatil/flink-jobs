package org.downloader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ObjectUtil;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    static final String OBD_BASE_URL = "http://internal-apis.intangles.com/vehicle";
    static final String LOCATION_BASE_URL = "http://internal-apis.intangles.com/vehicle";
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10L);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    // HTTP Client - reuse for better performance
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    public static void main(String[] args) throws Exception {

        String accountId = "1273982179231137792";
        String vehicleId = "1308809529009373184";
        long startTime = 1746001921000L;
        long endTime = 1748766721000L;

        String obdUrl = buildObdUrl(vehicleId, startTime, endTime);
        String locationUrl = buildLocationUrl(vehicleId, startTime, endTime);

       String rawStr = makeHttpRequest(obdUrl);

       ObjectMapper mapper = new ObjectMapper();

        JsonNode rootNode = mapper.readTree(rawStr);
        JsonNode result = rootNode.path("result").path("data").get("s3_obddata_results");

        if (!result.isMissingNode() && result.isArray()) {
            List<String> stringList = mapper.convertValue(result, new TypeReference<List<String>>(){});


            System.out.println(stringList.subList(0,10));
        }


//       Map<String,Map<String,Map<String,Map<String,Object>>>> resMap = mapper.readValue(rawStr,HashMap.class);
//       Object  result = resMap.get("result").get("data").get("s3_obddata_results");

       System.out.println();
    }

    public static String buildObdUrl(@Nonnull String vehicleId, @Nonnull long startTime, @Nonnull long endTime ) throws Exception {
        ObjectUtil.checkNotNull(vehicleId, "vehicleId is required");

        if (startTime >= endTime) {
            throw new Exception("startTime should be less than endTime.");
        }

        String accountId = "1273982179231137792";
        return OBD_BASE_URL +
                "/" +
                vehicleId +
                "/obd_data_int/" +
                startTime +
                "/" +
                endTime +
                "?fetch_result_from_multiple_sources=true&lang=en&acc_id="
                + accountId;
    }

    public static String buildLocationUrl(@Nonnull String vehicleId, @Nonnull long startTime, @Nonnull long endTime ) throws Exception {
        ObjectUtil.checkNotNull(vehicleId, "vehicleId is required");

        if (startTime >= endTime) {
            throw new Exception("startTime should be less than endTime.");
        }

        String accountId = "1273982179231137792";
        return LOCATION_BASE_URL +
                "/" +
                vehicleId +
                "/historyInt/" +
                startTime +
                "/" +
                endTime +
                "?fetch_result_from_multiple_sources=true&lang=en&no_dp_filter=true&acc_id="
                + accountId;
    }

    /**
     * Makes a synchronous HTTP GET request.
     */
    private static String makeHttpRequest(String url)
            throws IOException, InterruptedException, URISyntaxException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("User-Agent", "VehicleDataDownloader/1.0")
                .GET()
                .build();

        HttpResponse<String> response = HTTP_CLIENT.send(request,
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("HTTP request failed with status code: " +
                    response.statusCode() + ", body: " + response.body());
        }

        System.out.println("HTTP request successful, response size: " + response.body().length());
        return response.body();
    }
}
