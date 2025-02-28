package com.task10;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.*;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;


import static com.openmeteo.OpenMeteoApiClient.getWeatherForecast;

import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


@LambdaHandler(lambdaName = "processor",
	roleName = "processor-role",
	layers = {"sdk-layer"},
	runtime = DeploymentRuntime.JAVA17,
	tracingMode = TracingMode.Active,
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(layerName = "sdk-layer",
		libraries = {"lib/open-meteo-1.0-SNAPSHOT.jar"},
		runtime = DeploymentRuntime.JAVA17,
		artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
@DependsOn(name = "Weather", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})

public class Processor implements RequestHandler<Object, Map<String, Object>> {

    private static final ObjectMapper mapper = new ObjectMapper();
	private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&" +
			"current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
    private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withRegion("eu-central-1")
			.build();
	private final String tableName = System.getenv("table");

	public Map<String, Object> handleRequest(Object request, Context context) {
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
            storeDataInDynamoDB(response);
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

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalJson);
    }

    private void storeDataInDynamoDB(String weatherData) {
        JsonNode weatherJson;
        try {
            weatherJson = mapper.readTree(weatherData);

            Table table = dynamoDBClient.getTable(tableName);
            table.putItem(new PutItemSpec()
                    .withItem(new ValueMap()
                            .with("id", UUID.randomUUID().toString())
                            .with("forecast", weatherJson.toString())
                    ));
        } catch (Exception e) {
            throw new RuntimeException("Failed to store weather data in DynamoDB", e);
        }
    }
}
