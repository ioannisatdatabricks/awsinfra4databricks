#!/usr/bin/env python3

import traceback
import time
import json
import argparse

from awsinfra4databricks import AWSSession, CloudInfraBuilderForWorkspace, NetworkArchitectureDesignOptions, NetworkArchitectureParameters, CustomerManagedKeysOptions

# Define the input
argumentParser = argparse.ArgumentParser()
argumentParser.add_argument('--databricksAccountId', type=str, required=True, help='The Databricks account ID')
argumentParser.add_argument('--awsRegion', type=str, required=True, help='The AWS region where the infrastructure will be deployed')
argumentParser.add_argument('--awsProfileName', type=str, required=True, help='The profile name for authenticating to the AWS account')
args = argumentParser.parse_args()

try:
    # Get the Databricks account Id
    accountId = args.databricksAccountId

    # Starting the AWS session with a Profile
    aws_region = args.awsRegion
    awsSession = AWSSession(profileName=args.awsProfileName)

    availabilityZonesIndexes = awsSession.getAvailabilityZoneIndexes(aws_region)
    availabilityZonesIndexes = availabilityZonesIndexes[0:2] # Keep the first two only

    # Define the networking setup and parameters
    networkDesignOptions = NetworkArchitectureDesignOptions(
        internetAccess=NetworkArchitectureDesignOptions.InternetAccess.HIGH_AVAILABILITY,
        privateLinkEndpoints=NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED,
        dataExfiltrationProtection=NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED,
        vpcArchitecture=NetworkArchitectureDesignOptions.VPCArchitectureMode.HUB_AND_SPOKE
    )
    networkParameters = NetworkArchitectureParameters(
        vpcCidrStartingAddress = '10.10.0.0',
        maxRunningNodesPerSubnet = 1000,
        availabilityZoneIndexes = availabilityZonesIndexes,
        hubVpcStartingAddress='10.11.0.0'
    )

    # Define the CMK options
    customerManagedKeysOptions = CustomerManagedKeysOptions(
        usage=CustomerManagedKeysOptions.Usage.BOTH,
        keyAlias='ha-dep-pl-cmk-hubnspoke'
    )

    # Define the custom tags that will be added in all resources
    resourceTags = {
        "Owner": "ioannis.papadopoulos@databricks.com",
        "RemoveAfter": "2025-12-31"
    }

    # Infra builder
    print("Definining the infrastructure")
    builder = CloudInfraBuilderForWorkspace(
        databricksAccountId=accountId,
        resourceTags=resourceTags,
        networkArchitectureDesignOptions=networkDesignOptions,
        networkArchitectureParameters=networkParameters,
        customerManagedKeysOptions=customerManagedKeysOptions
    )

    cloudFormationScript, inlinePolicyDocument = builder.cloudFormationTemplateBodyParametersAndRequiredPermissions()
 
    print("Template:\n------------------------------------------")
    print(cloudFormationScript)
    print("------------------------------------------\n")
    print("Policy Document:\n------------------------------------------")
    print(json.dumps(inlinePolicyDocument, indent=2))
    print("------------------------------------------")

    # # Create or replace the role with these privileges to be used with CloudFormation
    # roleArn = awsSession.createOrReplaceIamRoleForCloudFormation(
    #     roleName="CF4DatabricksCloudResourcesForWorkspace-2025-04-16",
    #     inlinePolicy= inlinePolicyDocument
    # )
    # print('Role Arn: ' + roleArn)
    # print("Waiting for 10 seconds until the role is properly registered")
    # time.sleep(10)

    # # Retrieving the privileges
    # print("Testing the privileges")
    # allowedActions, disallowedActions = awsSession.checkPermissionsForActions(
    #     inlinePolicyDocument['Statement'][0]['Action'] + inlinePolicyDocument['Statement'][1]['Action'],
    #     roleArn)

    # if len(disallowedActions) > 0:
    #     raise Exception("Role does not allow actions")
    
    # print("Building the infrastructure")
    # awsSession.createCloudFormationStack(
    #     stackName='ha-dep-pl-cmk-hubnspoke',
    #     templateBody=cloudFormationScript,
    #     region=aws_region,
    #     bucketForCFTemplate='temp4cftemplates-eu-west-3-2025-02-23',
    #     roleArn=roleArn
    # )

except Exception as e:
    print("Exception caught:")
    print(str(e))
    print("\nHere is the trace:")
    print(traceback.format_exc())