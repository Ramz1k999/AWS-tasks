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

import static com.openmeteo.OpenMeteoApiClient.getWeatherForecast;

import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


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
	private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&" +
			"current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

	@Override
	public String handleRequest(Object input, Context context) {
		LambdaLogger logger = context.getLogger();

        JsonNode inputNode = mapper.valueToTree(input);

        String method = inputNode.path("requestContext").path("http").path("method").asText();
        String path = inputNode.path("rawPath").asText();

        // Check if the request is valid
        if (!"/weather".equals(path) || !"GET".equals(method)) {
            return generateBadRequestResponse(path, method);
        }

        try {
            String response = fetchWeatherDataUsingOpenMeteo();
            logger.log("Weather Data: " + response);
            return response;
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            return "Failed to fetch weather data";
        }
    }


	public static String fetchWeatherDataUsingOpenMeteo() throws Exception {
		String json = getWeatherForecast(URL);

		return transformWeatherJson(json);
	}

	public static String transformWeatherJson(String json) throws Exception {
		JsonNode root = mapper.readTree(json);
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

	private String generateBadRequestResponse(String path, String method) {
        try {
            ObjectNode responseJson = mapper.createObjectNode();
            responseJson.put("statusCode", 400);
            responseJson.put("message", String.format("Bad request syntax or unsupported method. Request path: %s. HTTP method: %s", path, method));
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseJson);
        } catch (Exception e) {
            return "Error generating bad request response";
        }
    }
}