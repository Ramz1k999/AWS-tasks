package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
    lambdaName = "hello_world",
	roleName = "hello_world-role",
	isPublishVersion = true,
	aliasName = "learn",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)

@LambdaUrlConfig(
    authType = AuthType.NONE,
    invokeMode = InvokeMode.BUFFERED
)

public class HelloWorld implements RequestHandler<Object, Map<String, Object>> {

	public Map<String, Object> handleRequest(Object request, Context context) {
		// Extract HTTP method and path from the request
        String httpMethod = (String) request.getOrDefault("httpMethod", "UNKNOWN");
        String path = (String) request.getOrDefault("path", "UNKNOWN");

        // Create response structure
        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*"); // Enable CORS

        Map<String, Object> response = new HashMap<>();
        response.put("headers", headers);

        // Handle only GET /hello, otherwise return 400 Bad Request
        if ("GET".equalsIgnoreCase(httpMethod) && "/hello".equalsIgnoreCase(path)) {
            response.put("statusCode", 200);
            response.put("body", jsonStringify(Map.of("message", "Hello from Lambda")));
        } else {
            response.put("statusCode", 400);
            response.put("body", jsonStringify(Map.of(
                "error", "Bad Request",
                "message", "Invalid request to " + path + " with method " + httpMethod
            )));
        }

        return response;
    }

     private String jsonStringify(Map<String, Object> map) {
         try {
             return objectMapper.writeValueAsString(map);
         } catch (Exception e) {
             return "{\"error\": \"Internal Server Error\"}";
         }
     }
}
