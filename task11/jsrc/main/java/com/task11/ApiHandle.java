package com.task11;

import com.task11.utilities.*;
import com.task11.handlers.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.environment.ValueTransformer;



import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
        lambdaName = "api_handler",
        roleName = "api_handler-role",
        runtime = DeploymentRuntime.JAVA17,
        isPublishVersion = false,
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DependsOn(resourceType = ResourceType.COGNITO_USER_POOL, name = "${booking_userpool}")
@DependsOn(resourceType = ResourceType.DYNAMODB_TABLE, name = "${tables_table}")
@DependsOn(resourceType = ResourceType.DYNAMODB_TABLE, name = "${reservations_table}")
@EnvironmentVariables(value = {
        @EnvironmentVariable(key = "REGION", value = "${region}"),
        @EnvironmentVariable(key = "COGNITO_ID", value = "${booking_userpool}",
                valueTransformer = ValueTransformer.USER_POOL_NAME_TO_USER_POOL_ID),
        @EnvironmentVariable(key = "CLIENT_ID", value = "${booking_userpool}",
                valueTransformer = ValueTransformer.USER_POOL_NAME_TO_CLIENT_ID),
        @EnvironmentVariable(key = "TABLES_TABLE_NAME", value = "${tables_table}"),
        @EnvironmentVariable(key = "RESERVATIONS_TABLE_NAME", value = "${reservations_table}")
})
public class ApiHandle implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final CognitoService cognitoService = new CognitoService();
    private final DynamoDBService dynamoDBService = new DynamoDBService();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String httpMethod = (String) event.get("httpMethod");
        String resource = (String) event.get("resource");

        try {
            switch (resource) {
                case "/signup":
                    return cognitoService.signUp(event);
                case "/signin":
                    return cognitoService.signIn(event);
                case "/tables":
                    return httpMethod.equals("GET") ? dynamoDBService.getAllTables() : dynamoDBService.createTable(event);
                case "/tables/{tableId}":
                    return dynamoDBService.getTableById(event);
                case "/reservations":
                    return httpMethod.equals("GET") ? dynamoDBService.getAllReservations() : dynamoDBService.createReservation(event);
                default:
                    return ResponseUtil.createResponse(400, "Invalid API path");
            }
        } catch (Exception e) {
            return ResponseUtil.createResponse(500, "Internal Server Error: " + e.getMessage());
        }
    }
}
