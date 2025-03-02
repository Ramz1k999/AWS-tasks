package com.task10;


import com.amazonaws.xray.AWSXRay;
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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
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
    aliasName = "learn",
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
		@EnvironmentVariable(key = "table", value = "${target_table}")
})

public class Processor implements RequestHandler<Object, String> {

    private static final ObjectMapper mapper = new ObjectMapper();
	private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
			.withRegion("eu-central-1")
			.build();
	private final String tableName = System.getenv("table");

	@Override
	public String handleRequest(Object input, Context context) {
		LambdaLogger logger = context.getLogger();

        JsonNode inputNode = mapper.valueToTree(input);

        String method = inputNode.path("requestContext").path("http").path("method").asText();
        String path = inputNode.path("rawPath").asText();

        if (!"/weather".equals(path) || !"GET".equals(method)) {
            return generateBadRequestResponse(path, method);
        }

        try {
            AWSXRay.beginSegment("Processor");


            Map<String, AttributeValue> weatherEntry = transformWeatherJsonToMap(getWeatherForecast(URL));

            dynamoDBClient.putItem(new PutItemRequest().withTableName(tableName).withItem(weatherEntry));

            logger.log("Weather Data: " + weatherEntry);

            AWSXRay.endSegment();

            return weatherEntry.toString();
        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            return "Failed to fetch weather data";
        }
	}


private static Map<String, AttributeValue> transformWeatherJsonToMap(String json) throws Exception {
        JsonNode root = mapper.readTree(json);
        Map<String, AttributeValue> weatherEntry = new HashMap<>();

        // Add the UUID id to the weather entry
        weatherEntry.put("id", new AttributeValue().withS(UUID.randomUUID().toString()));

        // Process the forecast
        Map<String, AttributeValue> forecast = new HashMap<>();
        forecast.put("elevation", new AttributeValue().withN(String.valueOf(root.path("elevation").asInt())));
        forecast.put("generationtime_ms", new AttributeValue().withN(String.valueOf(root.path("generationtime_ms").asInt())));

        // Process hourly data
        JsonNode hourlyNode = root.path("hourly");
        Map<String, AttributeValue> hourly = new HashMap<>();
        hourly.put("temperature_2m", new AttributeValue().withL(
                StreamSupport.stream(hourlyNode.path("temperature_2m").spliterator(), false)
                        .map(node -> new AttributeValue().withN(String.valueOf(node.asDouble())))
                        .collect(Collectors.toList())
        ));
        hourly.put("time", new AttributeValue().withL(
                StreamSupport.stream(hourlyNode.path("time").spliterator(), false)
                        .map(node -> new AttributeValue().withS(node.asText()))
                        .collect(Collectors.toList())
        ));
        forecast.put("hourly", new AttributeValue().withM(hourly));

        // Process hourly units
        JsonNode hourlyUnitsNode = root.path("hourly_units");
        Map<String, AttributeValue> hourlyUnits = new HashMap<>();
        hourlyUnits.put("temperature_2m", new AttributeValue().withS(hourlyUnitsNode.path("temperature_2m").asText()));
        hourlyUnits.put("time", new AttributeValue().withS(hourlyUnitsNode.path("time").asText()));
        forecast.put("hourly_units", new AttributeValue().withM(hourlyUnits));

        // Add latitude, longitude, timezone, etc.
        forecast.put("latitude", new AttributeValue().withN(String.valueOf(root.path("latitude").asDouble())));
        forecast.put("longitude", new AttributeValue().withN(String.valueOf(root.path("longitude").asDouble())));
        forecast.put("timezone", new AttributeValue().withS(root.path("timezone").asText()));
        forecast.put("timezone_abbreviation", new AttributeValue().withS(root.path("timezone_abbreviation").asText()));
        forecast.put("utc_offset_seconds", new AttributeValue().withN(String.valueOf(root.path("utc_offset_seconds").asInt())));

        // Add forecast to the main entry
        weatherEntry.put("forecast", new AttributeValue().withM(forecast));

        return weatherEntry;
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
