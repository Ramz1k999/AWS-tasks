package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;



import java.util.Map;
import java.time.Instant;
import java.util.UUID;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


@LambdaHandler(lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	runtime = DeploymentRuntime.JAVA17,
	isPublishVersion = false,
	aliasName = "learn",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")
})
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 10)


public class AuditProducer implements RequestHandler<DynamodbEvent, String> {

    private static final String AUDIT_TABLE_NAME = System.getenv("table");
    private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
    private final DynamoDB dynamoDB = new DynamoDB(client);
    private final Table auditTable = dynamoDB.getTable(AUDIT_TABLE_NAME);

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
            if (record.getEventName().equals("INSERT")) {
                handleInsert(record);
            } else if (record.getEventName().equals("MODIFY")) {
                handleModify(record);
            }
        }
        return "Processed records";
    }

    private void handleInsert(DynamodbEvent.DynamodbStreamRecord record) {
        String key = record.getDynamodb().getNewImage().get("key").getS();
        int value = Integer.parseInt(record.getDynamodb().getNewImage().get("value").getN());

        Item auditItem = new Item()
            .withPrimaryKey("id", UUID.randomUUID().toString())
            .withString("itemKey", key)
            .withString("modificationTime", Instant.now()
        .atOffset(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
            .withMap("newValue", Map.of("key", key, "value", value));

        auditTable.putItem(auditItem);
    }

    private void handleModify(DynamodbEvent.DynamodbStreamRecord record) {
        String key = record.getDynamodb().getNewImage().get("key").getS();
        int oldValue = Integer.parseInt(record.getDynamodb().getOldImage().get("value").getN());
        int newValue = Integer.parseInt(record.getDynamodb().getNewImage().get("value").getN());

        if (oldValue != newValue) {
            Item auditItem = new Item()
                .withPrimaryKey("id", UUID.randomUUID().toString())
                .withString("itemKey", key)
                .withString("modificationTime", Instant.now()
        .atOffset(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
                .withString("updatedAttribute", "value")
                .withInt("oldValue", oldValue)
                .withInt("newValue", newValue);

            auditTable.putItem(auditItem);
        }
    }
}
