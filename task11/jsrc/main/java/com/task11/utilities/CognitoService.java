package com.task11.utilities;

import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.Map;

public class CognitoService {

    private final String userPoolId;
    private final String clientId;
    private final CognitoIdentityProviderClient cognitoClient;

    public CognitoService() {
        // Load environment variables
        this.userPoolId = System.getenv("COGNITO_ID");
        this.clientId = System.getenv("CLIENT_ID");

        if (userPoolId == null || clientId == null) {
            throw new IllegalStateException("Missing environment variables for Cognito.");
        }

        this.cognitoClient = CognitoIdentityProviderClient.create();
    }

    public Map<String, Object> signUp(Map<String, Object> request) {
        AdminCreateUserRequest signUpRequest = AdminCreateUserRequest.builder()
                .userPoolId(userPoolId)
                .username((String) request.get("email"))
                .temporaryPassword((String) request.get("password"))
                .messageAction("SUPPRESS")
                .userAttributes(
                        AttributeType.builder().name("given_name").value((String) request.get("firstName")).build(),
                        AttributeType.builder().name("family_name").value((String) request.get("lastName")).build(),
                        AttributeType.builder().name("email").value((String) request.get("email")).build()
                )
                .build();

        cognitoClient.adminCreateUser(signUpRequest);
        return Map.of("statusCode", 200, "body", "User created");
    }

    public Map<String, Object> signIn(Map<String, Object> request) {
        InitiateAuthRequest authRequest = InitiateAuthRequest.builder()
                .authFlow(AuthFlowType.USER_PASSWORD_AUTH)
                .clientId(clientId)
                .authParameters(Map.of(
                        "USERNAME", (String) request.get("email"),
                        "PASSWORD", (String) request.get("password")
                ))
                .build();

        InitiateAuthResponse response = cognitoClient.initiateAuth(authRequest);
        return Map.of("statusCode", 200, "accessToken", response.authenticationResult().idToken());
    }
}
