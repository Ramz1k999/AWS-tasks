package com.task08;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.json.JSONObject;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.events.EventBridgeRuleSource;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
    lambdaName ="uuid_generator",
	roleName = "uuid_generator-role",
	runtime = DeploymentRuntime.JAVA17,
	isPublishVersion = false,
	aliasName = "learn",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)

@EventBridgeRuleSource(targetRule = "uuid_trigger")
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "target_bucket", value = "${target_bucket}}")
})


public class UuidGenerator implements RequestHandler<Object, Map<String, Object>> {

	public Map<String, Object> handleRequest(Object request, Context context) {
	    private static final String BUCKET_NAME = System.getenv("target_bucket");

        private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            uuids.add(UUID.randomUUID().toString());
        }

        JSONObject json = new JSONObject();
        json.put("ids", uuids);
p
        String timestamp = Instant.now().toString();


        byte[] jsonBytes = json.toString(4).getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonBytes);

        // Set metadata
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/json");
        metadata.setContentLength(jsonBytes.length);

        // Upload JSON to S3
        try {
            s3Client.putObject(BUCKET_NAME, timestamp, inputStream, metadata);
            return "File " + timestamp + " created in S3";
        } catch (Exception e) {
            return "Error uploading file: " + e.getMessage();
        }
	}
}
