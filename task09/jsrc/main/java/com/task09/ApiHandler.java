package com.task09;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.openmeteo.OpenMeteoApiClient.getWeatherForecast;

@LambdaHandler(
    lambdaName = "api_handler",
    roleName = "api_handler-role",
    layers = {"sdk-layer"},
    runtime = DeploymentRuntime.JAVA17,
    isPublishVersion = false,
    aliasName = "learn",
    logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
    layerName = "sdk-layer",
    libraries = {"lib/open-meteo-1.0-SNAPSHOT.jar"},
    runtime = DeploymentRuntime.JAVA17,
    artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
    authType = AuthType.NONE,
    invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<Object, String> {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String URL = System.getenv("OPEN_METEO_API_URL");

    @Override
    public String handleRequest(Object input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Received request: " + input);

        if (URL == null) {
            logger.log("Error: OPEN_METEO_API_URL environment variable not set.");
            return generateBadRequestResponse("Internal server error");
        }

        try {
            String response = fetchWeatherDataUsingOpenMeteo();
            logger.log("Weather Data successfully fetched");
            return response;
        } catch (Exception e) {
            logger.log("Error fetching weather data: " + e.getMessage());
            return generateBadRequestResponse("Failed to fetch weather data");
        }
    }

    public static String fetchWeatherDataUsingOpenMeteo() throws Exception {
        try {
            String json = getWeatherForecast(URL);
            return transformWeatherJson(json);
        } catch (IOException e) {
            throw new Exception("Network error while fetching weather data", e);
        } catch (Exception e) {
            throw new Exception("Error processing weather data JSON", e);
        }
    }

    public static String transformWeatherJson(String json) throws Exception {
        JsonNode root = mapper.readTree(json);

        if (root == null || root.get("latitude") == null) {
            throw new Exception("Invalid weather data received");
        }

        ObjectNode finalJson = mapper.createObjectNode();
        finalJson.put("latitude", root.path("latitude").asDouble());
        finalJson.put("longitude", root.path("longitude").asDouble());
        finalJson.put("generationtime_ms", root.path("generationtime_ms").asDouble());
        finalJson.put("utc_offset_seconds", root.path("utc_offset_seconds").asInt());
        finalJson.put("timezone", root.path("timezone").asText());
        finalJson.put("timezone_abbreviation", root.path("timezone_abbreviation").asText());
        finalJson.put("elevation", root.path("elevation").asDouble());
        finalJson.set("hourly_units", root.path("hourly_units"));
        finalJson.set("hourly", root.path("hourly"));
        finalJson.set("current_units", root.path("current_units"));
        finalJson.set("current", root.path("current"));

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalJson);
    }

    private String generateBadRequestResponse(String message) {
        ObjectNode errorResponse = mapper.createObjectNode();
        errorResponse.put("statusCode", 400);
        errorResponse.put("message", message);
        return errorResponse.toString();
    }
}
