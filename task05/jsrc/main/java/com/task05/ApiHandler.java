package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.Architecture;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;


import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
    lambdaName = "api_handler",
    roleName = "api_handler-role",
    runtime = DeploymentRuntime.JAVA17,
	architecture = Architecture.ARM64,
    isPublishVersion = false,
    aliasName = "learn",
    logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)

@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")}
)


public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final String Targettable = System.getenv("table");  // DynamoDB Table Name
    private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
    private final DynamoDB dynamoDB = new DynamoDB(client);
    private final Table table = dynamoDB.getTable(Targettable);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
        context.getLogger().log("Received Request: " + request);

        Map<String, Object> response = new HashMap<>();
        try {
            // Validate Request
            if (!request.containsKey("principalId") || !request.containsKey("content")) {
                return errorResponse(400, "Missing required fields: principalId or content");
            }

            // Extract Request Data
            int principalId = (int) request.get("principalId");
            Map<String, Object> content = (Map<String, Object>) request.get("content");

            // Generate Event Data
            String eventId = UUID.randomUUID().toString();
            String createdAt = Instant.now().toString();

            // Save to DynamoDB
            Item item = new Item()
                    .withPrimaryKey("id", eventId)
                    .withNumber("principalId", principalId)
                    .withString("createdAt", createdAt)
                    .withMap("body", content);

            table.putItem(item);

            // Construct Response
            Map<String, Object> event = new HashMap<>();
            event.put("id", eventId);
            event.put("principalId", principalId);
            event.put("createdAt", createdAt);
            event.put("body", content);

            response.put("statusCode", 201);
            response.put("event", event);

        } catch (Exception e) {
            context.getLogger().log("Error saving event: " + e.getMessage());
            return errorResponse(500, "Internal Server Error");
        }

        return response;
    }

    private Map<String, Object> errorResponse(int statusCode, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        response.put("body", "{\"error\": \"" + message + "\"}");
        return response;
    }
}
