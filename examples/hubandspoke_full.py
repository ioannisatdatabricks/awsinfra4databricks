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
 
    cloudFormationScript, inlinePolicyDocument = builder.cloudFormationTemplateBodyParametersAndRequiredPermissions()
    print("Saving the output in the files hubandspoke_full.yaml and hubandspoke_full.json ")
    cfFile = open("hubandspoke_full.yaml", 'w')
    cfFile.write(cloudFormationScript)
    cfFile.close()

    pFile = open("hubandspoke_full.json", 'w')
    pFile.write(json.dumps(inlinePolicyDocument, indent=2))
    pFile.close()

except Exception as e:
    print("Exception caught:")
    print(str(e))
    print("\nHere is the trace:")
    print(traceback.format_exc())