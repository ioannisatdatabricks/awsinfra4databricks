#!/usr/bin/env python3

import traceback
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

    networkParameters = NetworkArchitectureParameters(
        vpcCidrStartingAddress = '10.10.0.0',
        maxRunningNodesPerSubnet = 1000,
        availabilityZoneIndexes = availabilityZonesIndexes
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
        networkArchitectureParameters=networkParameters
    )

    cloudFormationScript, inlinePolicyDocument = builder.cloudFormationTemplateBodyParametersAndRequiredPermissions()
    print("Saving the output in the files mininum_configuration.yaml and mininum_configuration.json ")
    cfFile = open("mininum_configuration.yaml", 'w')
    cfFile.write(cloudFormationScript)
    cfFile.close()

    pFile = open("mininum_configuration.json", 'w')
    pFile.write(json.dumps(inlinePolicyDocument, indent=2))
    pFile.close()

except Exception as e:
    print("Exception caught:")
    print(str(e))
    print("\nHere is the trace:")
    print(traceback.format_exc())