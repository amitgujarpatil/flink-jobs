package org.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.ReferenceType;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ObjectUtil;

import javax.annotation.Nonnull;
import java.sql.Ref;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Main {

    static final String OBD_BASE_URL = "http://internal-apis.intangles.com/vehicle";

    public static void main(String[] args) throws Exception {
        System.out.println("Hello threads");


        String accountId = "1273982179231137792";
        String vehicleId = "1308809529009373184";
        long startTime = 1746001921000L;
        long endTime = 1748766721000L;

        List<String> obdUrls = fetchS3ObdUrls(accountId,vehicleId,startTime,endTime);

        System.out.println(obdUrls.stream().count());


    }

    public static String buildObdUrl(@Nonnull String accountId, @Nonnull String vehicleId, @Nonnull long startTime, @Nonnull long endTime ) throws Exception {
        ObjectUtil.checkNotNull(vehicleId, "vehicleId is required");

        if (startTime >= endTime) {
            throw new Exception("startTime should be less than endTime.");
        }

        return  "/" + vehicleId + "/obd_data_int/" + startTime + "/" + endTime +
                "?fetch_result_from_multiple_sources=true&lang=en&acc_id=" + accountId;
    }

    public static List<String> fetchS3ObdUrls(String accountId, String vehicleId, long startTime, long endTime ) {
        List<String> obdUrls = new ArrayList<>();

        try{
            RestClient restClient = new RestClient(OBD_BASE_URL);
            JsonNode result = restClient.GET(buildObdUrl(accountId,vehicleId,startTime,endTime));
            JsonNode obdNode = result.get("result").get("data").get("s3_obddata_results");

            if(obdNode.isMissingNode()){
                return obdUrls;
            }

            ObjectMapper mapper = new ObjectMapper();

            return mapper.convertValue(obdNode, new TypeReference<List<String>>() {
            });
        } catch (Exception e) {
            return obdUrls;
        }
    }

}
