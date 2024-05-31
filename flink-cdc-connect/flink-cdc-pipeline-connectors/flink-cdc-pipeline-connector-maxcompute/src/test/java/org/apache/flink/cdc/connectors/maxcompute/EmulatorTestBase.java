package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.Odps;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

/** @author dingxin (zhangdingxin.zdx@alibaba-inc.com) */
public class EmulatorTestBase {
    public static final DockerImageName MAXCOMPUTE_IMAGE =
            DockerImageName.parse("maxcompute/maxcompute-emulator:v0.0.4");

    @ClassRule
    public static GenericContainer<?> maxcompute =
            new GenericContainer<>(MAXCOMPUTE_IMAGE)
                    .withExposedPorts(8080)
                    .waitingFor(
                            Wait.forLogMessage(".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                    .withLogConsumer(frame -> System.out.print(frame.getUtf8String()));

    public final MaxComputeOptions testOptions =
            MaxComputeOptions.builder("ak", "sk", getEndpoint(), "mocked_mc").build();

    public final Odps odps = MaxComputeUtils.getOdps(testOptions);

    private String getEndpoint() {
        maxcompute.start();

        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = maxcompute.getHost();
        }
        String endpoint = "http://" + ip + ":" + maxcompute.getFirstMappedPort();
        sendPOST(endpoint + "/init", endpoint);
        return endpoint;
    }

    public static void sendPOST(String postUrl, String postData) {
        try {
            URL url = new URL(postUrl);

            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");
            httpURLConnection.setRequestProperty(
                    "Content-Length", String.valueOf(postData.length()));

            try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
                outputStream.write(postData.getBytes("UTF-8"));
                outputStream.flush();
            }
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException(
                        "POST request failed with response code: " + responseCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
