from .NetworkArchitecture import NetworkArchitectureDesignOptions, NetworkArchitectureParameters, SubnetConfigurationBuilder, VpcAndSubnetCIDR
from .DatabricksAddresses import DatabricksAddresses
from .CustomerManagedKeys import CustomerManagedKeysOptions, managedServicesPolicyStatement, workspaceStoragePolicyStatement
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from io import StringIO

# A class to constructs the CloudFormation template for the AWS cloud infrastructure required for a Databricks workspace deployment
class CloudInfraBuilderForWorkspace:
    # Initialises the object with the architectural choices and parameters
    def __init__(self,
                 databricksAccountId: str,
                 networkArchitectureDesignOptions: NetworkArchitectureDesignOptions = NetworkArchitectureDesignOptions(),
                 networkArchitectureParameters: NetworkArchitectureParameters = NetworkArchitectureParameters(),
                 customerManagedKeysOptions: CustomerManagedKeysOptions = CustomerManagedKeysOptions(),
                 resourceTags:dict[str:str] = {}):
        self.__databricksAccountId = databricksAccountId
        self.__networkArchitectureDesignOptions = networkArchitectureDesignOptions
        self.__networkArchitectureParameters = networkArchitectureParameters
        self.__customerManagedKeysOptions = customerManagedKeysOptions
        self.__tags = resourceTags
        self.__cloudFormationTemplate = None
        self.__requiredPrivileges = None
        self.__requiredPrivilegesForRollback = None



    # Returns the string of the CloudFormation template in JSON format
    def cloudFormationTemplateBodyParametersAndRequiredPermissions(self) -> tuple[str, dict]:
        # Creates the main structure of the template
        # Initialises the variables self.__cloudFormationTemplate
        self.__initialiseCloudFormationTemplate()

        # Defines the storage
        self.__defineStorageResource()

        # Defines the networking
        self.__defineNetworking()

        # Defines the workspace IAM role
        self.__defineWorkspaceIamRole()

        # Defines the CMK Resources
        self.__defineCustomerManagerKeyResources()

        # Returns the final output
        return (self.__generateCloudFormationTemplateString(), self.__generatePolicyDocument())



    # Generates the policy
    def __generatePolicyDocument(self) -> dict:
        policyDocument = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'RequiredForCreation',
                    'Effect': 'Allow',
                    'Action': sorted(list(self.__requiredPrivileges)),
                    'Resource': '*'
                },
                {
                    'Sid': 'RequiredForRollback',
                    'Effect': 'Allow',
                    'Action': sorted(list(self.__requiredPrivilegesForRollback)),
                    'Resource': '*'
                }
            ]
        }
        return policyDocument



    # Adds tags in a resource
    def __addTagsToResource(self, resource:str, tagsProperty: str = "Tags"):
        if len(self.__tags) > 0:
            tagsArray = [{"Key": t, "Value": self.__tags[t]} for t in self.__tags]
            if tagsProperty in self.__cloudFormationTemplate["Resources"][resource]["Properties"]:
                self.__cloudFormationTemplate["Resources"][resource]["Properties"][tagsProperty] += tagsArray
            else:
                self.__cloudFormationTemplate["Resources"][resource]["Properties"][tagsProperty] = tagsArray



    # Outputs the template as a formatted string
    def __generateCloudFormationTemplateString(self) -> str:
        yaml = YAML(pure=True, typ='rt')
        yaml.width = 4096  # To avoid line wrapping
        yaml.allow_unicode = True
        yaml.preserve_quotes = False
        yaml.indent(mapping=2, sequence=4, offset=2)
        string_stream = StringIO()
        yaml.dump(self.__cloudFormationTemplate, string_stream)
        return string_stream.getvalue()



    # Initialises the CloudFormation template and parameters
    def __initialiseCloudFormationTemplate(self):
        self.__cloudFormationTemplate = CommentedMap({
                "AWSTemplateFormatVersion" : "2010-09-09",
                "Description" : "Cloud resources for the deployment of a Databricks workspace"
            }
        )
        # Insert the Parameters section
        self.__cloudFormationTemplate.insert(
            pos= len(self.__cloudFormationTemplate),
            key= 'Parameters',
            value = CommentedMap({})
        )
        self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
            key='Parameters',
            before= '\n\n-------------------------------------------------------------------------\nThe template parameters\n  provided with default values that can be overriden'
        )
        # Add the Databricks AccountId Parameter
        self.__cloudFormationTemplate['Parameters'].insert(
            pos= len(self.__cloudFormationTemplate['Parameters']),
            key= 'DatabricksAccountId',
            value= CommentedMap({
                    "Description" : "The identifier of the Databricks account to be specified in resources such as cross-account IAM roles and resource-based policies",
                    "Type": "String",
                    "Default": self.__databricksAccountId
                }
            )
        )
        self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key ='DatabricksAccountId', before= 'The Databricks account Id', indent=2)

        # Insert the Rules section
        databricksAddresses = DatabricksAddresses()
        regionMappings = databricksAddresses.mappings()
        self.__cloudFormationTemplate.insert(
            pos= len(self.__cloudFormationTemplate),
            key= 'Rules',
            value= CommentedMap({
                "SupportedRegion": {
                    "Assertions": [
                        {
                            "Assert": {
                                "Fn::Contains": [
                                    [region for region in regionMappings],
                                    {"Ref": "AWS::Region"}
                                ]
                            },
                            "AssertDescription": "The current AWS region is not supported for for this deployment"
                        }
                    ]
                }
            })
        )
        self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
            key='Rules',
            before= '\n\n-------------------------------------------------------------------------\nThe template rules'
        )
        self.__cloudFormationTemplate['Rules'].yaml_set_comment_before_after_key(key ='SupportedRegion', before= 'Checking validity of the region', indent=2)
        # Insert the Mappings section
        if self.__networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED:
            self.__cloudFormationTemplate.insert(
                pos= len(self.__cloudFormationTemplate),
                key= 'Mappings',
                value= CommentedMap({
                    "DatabricksAddresses": regionMappings
                })
            )
            self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
                key='Mappings',
                before= '\n\n-------------------------------------------------------------------------\nThe template mappings'
            )
            self.__cloudFormationTemplate['Mappings'].yaml_set_comment_before_after_key(key ='DatabricksAddresses', before= 'The addresses and endpoints ids for the Databricks VPC endpoints', indent=2)
        # Create the Conditions
        self.__cloudFormationTemplate.insert(
            pos= len(self.__cloudFormationTemplate),
            key= 'Conditions',
            value= CommentedMap({})
        )
        self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
            key='Conditions',
            before= '\n\n-------------------------------------------------------------------------\nThe Conditions defined in this template'
        )
        # Create the Resources and Output sections
        self.__cloudFormationTemplate.insert(
            pos= len(self.__cloudFormationTemplate),
            key= 'Resources',
            value= CommentedMap({})
        )
        self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
            key='Resources',
            before= '\n\n-------------------------------------------------------------------------\nThe Resources created in this template'
        )
        self.__cloudFormationTemplate.insert(
            pos= len(self.__cloudFormationTemplate),
            key= 'Outputs',
            value= CommentedMap({})
        )
        self.__cloudFormationTemplate.yaml_set_comment_before_after_key(
            key='Outputs',
            before= '\n\n-------------------------------------------------------------------------\nThe Outputs of this template'
        )
        # Initialises the privileges
        self.__requiredPrivileges = set()
        self.__requiredPrivilegesForRollback = set()



    # Defines the Storage Resource
    def __defineStorageResource(self):

        # The Bucker name parameter
        self.__cloudFormationTemplate['Parameters'].insert(
            pos= len(self.__cloudFormationTemplate['Parameters']),
            key= 'DBFSRootBucketName',
            value= CommentedMap({
                    "Description": "The name of the S3 bucket for the workspace storage (DBFS Root)",
                    "Type": "String",
                    "Default": ""
                }
            )
        )
        self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(
            key ='DBFSRootBucketName',
            before= 'The name of the S3 bucket for the workspace storage (DBFS Root)\nif left unspecified, a value based on of the name of the stack and the region will be used.', indent=2)

        # The Name condition
        self.__cloudFormationTemplate['Conditions'].insert(
            pos= len(self.__cloudFormationTemplate['Conditions']),
            key= 'IsBucketNameSpecified',
            value= CommentedMap({
                "Fn::Not" : [{
                    "Fn::Equals" : [
                        {"Ref" : "DBFSRootBucketName"},
                        ""
                    ]
                }]
            })
        )
        self.__cloudFormationTemplate['Conditions'].yaml_set_comment_before_after_key(
            key ='IsBucketNameSpecified',
            before= 'Checks if a name for the DBFS root bucket has been specified', indent=2)

        # The S3 bucket for DBFS
        self.__cloudFormationTemplate["Resources"].insert(
            pos= len(self.__cloudFormationTemplate["Resources"]),
            key= 'DBFSRootBucket',
            value= CommentedMap({
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": {"Fn::If":["IsBucketNameSpecified", {"Ref": "DBFSRootBucketName"}, {"Fn::Sub": "${AWS::StackName}-${AWS::Region}-dbfs"}]},
                    "BucketEncryption": {
                        "ServerSideEncryptionConfiguration": [
                            {
                                "BucketKeyEnabled": True,
                                "ServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": "AES256"
                                }
                            }
                        ]
                    },
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "BlockPublicPolicy": True,
                        "IgnorePublicAcls": True,
                        "RestrictPublicBuckets": True
                    },
                }
            })
        )
        self.__addTagsToResource("DBFSRootBucket")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key ='DBFSRootBucket',
            before= '\n----- Workspace Storage\n\nThe S3 bucket for the workspace storage (DBFS Root)', indent=2)
        # The required privileges
        self.__requiredPrivileges.add("s3:CreateBucket")
        self.__requiredPrivileges.add("s3:PutBucketTagging")
        self.__requiredPrivileges.add("s3:PutBucketPublicAccessBlock")
        self.__requiredPrivileges.add("s3:PutEncryptionConfiguration")
        self.__requiredPrivilegesForRollback.add("s3:DeleteBucket")
        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'DBFSBucketName',
            value= {
                "Description": "The S3 bucket name for DBFS",
                "Value": {"Ref": "DBFSRootBucket"}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(key ='DBFSBucketName', before= 'The name of the S3 bucket for the workspace storage (DBFS Root)', indent=2)

        # The bucket resource policy allowing the Databricks control plane to operate on it
        self.__cloudFormationTemplate["Resources"].insert(
            pos= len(self.__cloudFormationTemplate["Resources"]),
            key= 'DBFSRootBucketPolicy',
            value= CommentedMap({
                "Type": "AWS::S3::BucketPolicy",
                "Properties": {
                    "Bucket": {"Ref": "DBFSRootBucket"},
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Sid": "Grant Databricks Access to DBFS root S3 bucket",
                                "Effect": "Allow",
                                "Principal": {"AWS": "414351767826"},
                                "Action": [
                                    "s3:GetObject",
                                    "s3:GetObjectVersion",
                                    "s3:PutObject",
                                    "s3:DeleteObject",
                                    "s3:ListBucket",
                                    "s3:GetBucketLocation"
                                ],
                                "Resource": [
                                    {"Fn::Sub": "${DBFSRootBucket.Arn}"},
                                    {"Fn::Sub": "${DBFSRootBucket.Arn}/*"}
                                ],
                                "Condition": {
                                    "StringEquals": {
                                       "aws:PrincipalTag/DatabricksAccountId": [
                                           {"Ref": "DatabricksAccountId"}
                                       ]
                                    }
                                }
                            },
                            {
                                "Sid": "Prevent DBFS from accessing Unity Catalog metastore",
                                "Effect": "Deny",
                                "Principal": {
                                    "AWS": "arn:aws:iam::414351767826:root"
                                },
                                "Action": ["s3:*"],
                                "Resource": [
                                    {"Fn::Sub": "${DBFSRootBucket.Arn}/unity-catalog/*"}
                                ]
                            }
                        ]
                    }
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='DBFSRootBucketPolicy', before= 'The policy attached to the bucket', indent=2)
        # The required privileges
        self.__requiredPrivileges.add("s3:PutBucketPolicy")
        self.__requiredPrivileges.add("s3:GetBucketPolicy")
        self.__requiredPrivilegesForRollback.add("s3:DeleteBucketPolicy")

        # The storage credential role arn
        self.__cloudFormationTemplate['Parameters'].insert(
            pos= len(self.__cloudFormationTemplate['Parameters']),
            key= 'StorageCredentialIAMRoleArn',
            value= CommentedMap({
                    "Description": "The storage credential to be used for the workspace storage. Use the output value of the first pass",
                    "Type": "String",
                    "Default": ""
                }
            )
        )
        self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(
            key ='StorageCredentialIAMRoleArn',
            before= 'The ARN of the IAM role for the workspace\'s storage. Use the output value after running the script for the first time', indent=2)

        # The ARN condition
        self.__cloudFormationTemplate['Conditions'].insert(
            pos= len(self.__cloudFormationTemplate['Conditions']),
            key= 'IsStorageCredentialArnSpecified',
            value= CommentedMap({
                "Fn::Not" : [{
                    "Fn::Equals" : [
                        {"Ref" : "StorageCredentialIAMRoleArn"},
                        ""
                    ]
                }]
            })
        )
        self.__cloudFormationTemplate['Conditions'].yaml_set_comment_before_after_key(
            key ='IsStorageCredentialArnSpecified',
            before= 'Checks if the ARN for the storage credential has been specified', indent=2)

        # The IAM role for the storage credential
        self.__cloudFormationTemplate['Resources'].insert(
            pos= len(self.__cloudFormationTemplate['Resources']),
            key= 'StorageCredentialIAMRole',
            value= CommentedMap({
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "Description" : "The IAM role to be used as the storage credential for the Databricks workspace",
                    "RoleName" : {"Fn::Sub":"${AWS::StackName}-StorageCredential"},
                    "AssumeRolePolicyDocument" : {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "AWS": [
                                        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
                                        {"Fn::If":["IsStorageCredentialArnSpecified", {"Ref": "StorageCredentialIAMRoleArn"}, {"Ref": "AWS::NoValue"}]}
                                    ]
                                },
                                "Action": "sts:AssumeRole",
                                "Condition": {
                                    "StringEquals": {
                                        "sts:ExternalId": {"Ref": "DatabricksAccountId"}
                                    }
                                }
                            }
                        ]
                    },
                    "Policies" : [
                        {
                            "PolicyName" : {"Fn::Sub":"${AWS::StackName}-StorageCredentialPolicy"},
                            "PolicyDocument" : {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                                        "Resource": {"Fn::Sub": "${DBFSRootBucket.Arn}/unity-catalog/*"}
                                    },
                                    {
                                        "Effect": "Allow",
                                        "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                                        "Resource": {"Fn::Sub": "${DBFSRootBucket.Arn}"},
                                        "Condition": {
                                            "StringLike": {
                                                "s3:prefix": "unity-catalog/*"
                                            }
                                        }
                                    },
                                    {
                                        "Fn::If": [
                                            "IsStorageCredentialArnSpecified",
                                            {
                                                "Effect": "Allow",
                                                "Action": [
                                                    "sts:AssumeRole"
                                                ],
                                                "Resource": [{"Ref": "StorageCredentialIAMRoleArn"}]
                                            },
                                            {"Ref": "AWS::NoValue"}
                                        ]
                                    },
                                    {
                                        "Sid": "ManagedFileEventsSetupStatement",
                                        "Effect": "Allow",
                                        "Action": [
                                            "s3:GetBucketNotification",
                                            "s3:PutBucketNotification",
                                            "sns:ListSubscriptionsByTopic",
                                            "sns:GetTopicAttributes",
                                            "sns:SetTopicAttributes",
                                            "sns:CreateTopic",
                                            "sns:TagResource",
                                            "sns:Publish",
                                            "sns:Subscribe",
                                            "sqs:CreateQueue",
                                            "sqs:DeleteMessage",
                                            "sqs:ReceiveMessage",
                                            "sqs:SendMessage",
                                            "sqs:GetQueueUrl",
                                            "sqs:GetQueueAttributes",
                                            "sqs:SetQueueAttributes",
                                            "sqs:TagQueue",
                                            "sqs:ChangeMessageVisibility",
                                            "sqs:PurgeQueue"
                                        ],
                                        "Resource": [
                                            {"Fn::Sub": "${DBFSRootBucket.Arn}"},
                                            "arn:aws:sqs:*:*:*",
                                            "arn:aws:sns:*:*:*"
                                        ]
                                    },
                                    {
                                        "Sid": "ManagedFileEventsListStatement",
                                        "Effect": "Allow",
                                        "Action": ["sqs:ListQueues", "sqs:ListQueueTags", "sns:ListTopics"],
                                        "Resource": "*"
                                    },
                                    {
                                        "Sid": "ManagedFileEventsTeardownStatement",
                                        "Effect": "Allow",
                                        "Action": ["sns:Unsubscribe", "sns:DeleteTopic", "sqs:DeleteQueue"],
                                        "Resource": ["arn:aws:sqs:*:*:*", "arn:aws:sns:*:*:*"]
                                    }
                                ]
                            },
                        }
                    ],
                }
            })
        )

        # Add a statement related to the encryption key
        if self.__customerManagedKeysOptions.usage() in (CustomerManagedKeysOptions.Usage.BOTH, CustomerManagedKeysOptions.Usage.STORAGE):
            self.__cloudFormationTemplate['Resources']['StorageCredentialIAMRole']['Properties']["Policies"][0]["PolicyDocument"]["Statement"].append(
                {
                    "Effect": "Allow",
                    "Action": ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"],
                    "Resource": [
                        {"Fn::GetAtt": "EncryptionKey.Arn"}
                    ]
                }
            )

        self.__addTagsToResource("StorageCredentialIAMRole")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='StorageCredentialIAMRole', before= '\nThe IAM role corresponding to the storage credential of the workspace', indent=2)

        self.__requiredPrivileges.add("iam:CreateRole")
        self.__requiredPrivileges.add("iam:GetRole")
        self.__requiredPrivileges.add("iam:TagRole")
        self.__requiredPrivileges.add("iam:PutRolePolicy")
        self.__requiredPrivileges.add("iam:GetRolePolicy")
        self.__requiredPrivilegesForRollback.add("iam:DeleteRole")
        self.__requiredPrivilegesForRollback.add("iam:DeleteRolePolicy")

        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'StorageCredentialIAMRole',
            value= {
                "Description": "The ARN of the cross account IAM role for the storage credential of the Databricks workspace",
                "Value": {"Fn::GetAtt": "StorageCredentialIAMRole.Arn"}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
            key ='StorageCredentialIAMRole',
            before= 'The cross-account IAM role for the workspace storage credential', indent=2)


    # Defines the Networking resources
    def __defineNetworking(self):
        ## The network configuration
        networkConfigBuilder = SubnetConfigurationBuilder(networkArchitectureDesignOptions=self.__networkArchitectureDesignOptions,
                                                          networkArchitectureParameters=self.__networkArchitectureParameters)
        networkConfig = networkConfigBuilder.vpcConfig()

        #### The Databricks VPC
        dbsVpcConfig = networkConfig[VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC]
        # The VPC parameter
        self.__cloudFormationTemplate['Parameters'].insert(
            pos= len(self.__cloudFormationTemplate['Parameters']),
            key= 'DBSVPCCidrBlock',
            value= CommentedMap({
                    "Description": "The CIDR block of the Databricks VPC",
                    "Type": "String",
                    "Default": dbsVpcConfig.vpcCIDR()
                }
            )
        )
        self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key ='DBSVPCCidrBlock', before= 'The CIDR block of the Databricks VPC', indent=2)
        # The VPC resource
        self.__cloudFormationTemplate['Resources'].insert(
            pos= len(self.__cloudFormationTemplate['Resources']),
            key= 'DBSVpc',
            value= CommentedMap({
                "Type": "AWS::EC2::VPC",
                "Properties": {
                    "CidrBlock": {"Ref": "DBSVPCCidrBlock"},
                    "EnableDnsHostnames": True,
                    "EnableDnsSupport": True,
                    "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-DatabricksVPC"}}]
                }
            })
        )
        self.__addTagsToResource("DBSVpc")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='DBSVpc', before= '\n\n----- Networking setup\n\nThe VPC for the Databricks compute nodes', indent=2)
        # The permissions
        self.__requiredPrivileges.add("ec2:CreateVpc")
        self.__requiredPrivileges.add("ec2:DescribeVpcs")
        self.__requiredPrivileges.add("ec2:ModifyVpcAttribute")
        self.__requiredPrivileges.add("ec2:CreateTags")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteVpc")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteTags")
        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'DatabricksVPCId',
            value= {
                "Description": "The Id of the VPC where Databricks deployes the compute nodes",
                "Value": {"Ref": "DBSVpc"}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(key ='DatabricksVPCId', before= 'The Id of the Databricks VPC', indent=2)

        # The HUB VPC
        isHubNSpoke = (self.__networkArchitectureDesignOptions.vpcArchitecture() == NetworkArchitectureDesignOptions.VPCArchitectureMode.HUB_AND_SPOKE)
        if isHubNSpoke:
            hubVpcConfig = networkConfig[VpcAndSubnetCIDR.VpcType.HUB_VPC]
            # The parameter
            self.__cloudFormationTemplate['Parameters'].insert(
                pos= len(self.__cloudFormationTemplate['Parameters']),
                key= 'HubVPCCidrBlock',
                value= CommentedMap({
                        "Description": "The CIDR block of the Hub VPC where all VPC Endpoints, NAT and Internet Gateways are installed",
                        "Type": "String",
                        "Default": hubVpcConfig.vpcCIDR()
                    }
                )
            )
            self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key ='HubVPCCidrBlock', before= 'The CIDR block of the Hub VPC', indent=2)
            # The Hub VPC resource
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= 'HubVpc',
                value= CommentedMap({
                    "Type": "AWS::EC2::VPC",
                    "Properties": {
                        "CidrBlock": {"Ref": "HubVPCCidrBlock"},
                        "EnableDnsHostnames": True,
                        "EnableDnsSupport": True,
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-HubVPC"}}]
                    }
                })
            )
            self.__addTagsToResource("HubVpc")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='HubVpc', before= '\nThe Hub VPC', indent=2)

        # The Internet Gateway that is attached either on the Databricks or the HUB VPC
        isInternetEnabled = (self.__networkArchitectureDesignOptions.internetAccess() != NetworkArchitectureDesignOptions.InternetAccess.DISABLED)
        if isInternetEnabled:
            # The internet gateway
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= 'Igw',
                value= CommentedMap({
                    "Type": "AWS::EC2::InternetGateway",
                    "Properties": {
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-Igw"}}]
                    }
                })
            )
            self.__addTagsToResource("Igw")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='Igw', before= '\nThe Internet Gateway', indent=2)
            # The permissions
            self.__requiredPrivileges.add("ec2:CreateInternetGateway")
            self.__requiredPrivileges.add("ec2:DescribeInternetGateways")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteInternetGateway")
            #... attached to the VPC
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= 'VpcIgwAttachment',
                value= CommentedMap({
                    "Type": "AWS::EC2::VPCGatewayAttachment",
                    "Properties": {
                        "InternetGatewayId": {"Ref": "Igw"},
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key ='VpcIgwAttachment', before= '... attached to the VPC', indent=2)
            # The permissions
            self.__requiredPrivileges.add("ec2:AttachInternetGateway")
            self.__requiredPrivilegesForRollback.add("ec2:DetachInternetGateway")

        # The subnets of the Databricks clusters
        availabilityZoneIndexes = self.__networkArchitectureParameters.availabilityZoneIndexes()
        subnetSetsInDbsVPCs = dbsVpcConfig.subnetCIDRs()
        clusterSubnets = subnetSetsInDbsVPCs[VpcAndSubnetCIDR.SubnetType.CLUSTERS]
        subnetOutputStrings = []
        for iAZ in range(len(availabilityZoneIndexes)):
            azIndex = availabilityZoneIndexes[iAZ]
            subnetCIDR = clusterSubnets[iAZ]
            # The parameter
            parameterName = "DBSClusterSubnet" + str(iAZ + 1) + "CidrBlock"
            self.__cloudFormationTemplate['Parameters'].insert(
                pos= len(self.__cloudFormationTemplate['Parameters']),
                key= parameterName,
                value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the Databricks clusters",
                        "Type": "String",
                        "Default": subnetCIDR
                    }
                )
            )
            self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the Databricks clusters", indent=2)
            # The resource
            resourceName = "DBSClusterSubnet" + str(iAZ + 1)
            subnetOutputStrings.append("${" + resourceName + "}")
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= resourceName,
                value= CommentedMap({
                    "Type": "AWS::EC2::Subnet",
                    "Properties": {
                        "VpcId": {"Ref": "DBSVpc"},
                        "CidrBlock": {"Ref": parameterName},
                        "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                        "MapPublicIpOnLaunch": False,
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-DatabricksClusterSubnet" + str(iAZ + 1)}}]
                    }
                })
            )
            self.__addTagsToResource(resourceName)
            commentForSubnet = " Subnet " + str(iAZ + 1)
            if iAZ == 0: commentForSubnet = '\nSubnets for the Databricks compute nodes\n'+ commentForSubnet
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)
        # The required permissions
        self.__requiredPrivileges.add("ec2:CreateSubnet")
        self.__requiredPrivileges.add("ec2:DescribeSubnets")
        self.__requiredPrivileges.add("ec2:DescribeAvailabilityZones")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteSubnet")
        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'DatabricksSubnetIds',
            value= {
                "Description": "The subnet ids in the VPC for the Databricks clusters",
                "Value": {"Fn::Sub": " ".join(subnetOutputStrings)}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(key ='DatabricksSubnetIds', before= 'The Ids of the subnets in the Databricks VPC where the compute nodes are deployed', indent=2)

        # The transit gateway subnets
        if isHubNSpoke:
            # On the Databricks VPC
            dbsVpcTgwSubnets = subnetSetsInDbsVPCs[VpcAndSubnetCIDR.SubnetType.TRANSITGATEWAY]
            for iAZ in range(len(availabilityZoneIndexes)):
                azIndex = availabilityZoneIndexes[iAZ]
                subnetCIDR = dbsVpcTgwSubnets[iAZ]
                # The parameter
                parameterName = "DBSVPCTransitGatewaySubnet" + str(iAZ + 1) + "CidrBlock"
                self.__cloudFormationTemplate['Parameters'].insert(
                    pos= len(self.__cloudFormationTemplate['Parameters']),
                    key= parameterName,
                    value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the transit gateway attachment in the Databricks VPC",
                        "Type": "String",
                        "Default": subnetCIDR
                    })
                )
                self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the transit gateway attachment in the Databricks VPC", indent=2)
                # The resource
                resourceName = "DBSVPCTransitGatewaySubnet" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= resourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::Subnet",
                        "Properties": {
                            "VpcId": {"Ref": "DBSVpc"},
                            "CidrBlock": {"Ref": parameterName},
                            "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                            "MapPublicIpOnLaunch": False,
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-DBSVPCTransitGatewaySubnet" + str(iAZ + 1)}}]
                        }
                    })
                )
                self.__addTagsToResource(resourceName)
                commentForSubnet = " Subnet " + str(iAZ + 1)
                if iAZ == 0: commentForSubnet = '\nSubnets for the Transit Gateway attachments in the Databricks VPC\n'+ commentForSubnet
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)

            # On the Hub VPC
            hubVpcTgwSubnets = networkConfig[VpcAndSubnetCIDR.VpcType.HUB_VPC].subnetCIDRs()[VpcAndSubnetCIDR.SubnetType.TRANSITGATEWAY]
            for iAZ in range(len(availabilityZoneIndexes)):
                azIndex = availabilityZoneIndexes[iAZ]
                subnetCIDR = hubVpcTgwSubnets[iAZ]
                # The parameter
                parameterName = "HubVPCTransitGatewaySubnet" + str(iAZ + 1) + "CidrBlock"
                self.__cloudFormationTemplate['Parameters'].insert(
                    pos= len(self.__cloudFormationTemplate['Parameters']),
                    key= parameterName,
                    value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the transit gateway attachment in the Hub VPC",
                        "Type": "String",
                        "Default": subnetCIDR
                    })
                )
                self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the transit gateway attachment in the Hub VPC", indent=2)
                # The resource
                resourceName = "HubVPCTransitGatewaySubnet" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= resourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::Subnet",
                        "Properties": {
                            "VpcId": {"Ref": "HubVpc"},
                            "CidrBlock": {"Ref": parameterName},
                            "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                            "MapPublicIpOnLaunch": False,
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-HubVPCTransitGatewaySubnet" + str(iAZ + 1)}}]
                        }
                    })
                )
                self.__addTagsToResource(resourceName)
                commentForSubnet = " Subnet " + str(iAZ + 1)
                if iAZ == 0: commentForSubnet = '\nSubnets for the Transit Gateway attachments in the Hub VPC\n'+ commentForSubnet
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)


        # The EP subnets
        isPrivateLinkEnabled = (self.__networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED)
        if isPrivateLinkEnabled:
            vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC
            vpcResourceName = "DBSVpc"
            if self.__networkArchitectureDesignOptions.vpcArchitecture() == NetworkArchitectureDesignOptions.VPCArchitectureMode.HUB_AND_SPOKE:
                vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.HUB_VPC
                vpcResourceName = "HubVpc"
            epSubnets = networkConfig[vpcTypeOfEpSubnets].subnetCIDRs()[VpcAndSubnetCIDR.SubnetType.VPCENDPOINTS]
            for iAZ in range(len(availabilityZoneIndexes)):
                azIndex = availabilityZoneIndexes[iAZ]
                subnetCIDR = epSubnets[iAZ]
                # The parameter
                parameterName = "VPCEndpointSubnet" + str(iAZ + 1) + "CidrBlock"
                self.__cloudFormationTemplate['Parameters'].insert(
                    pos= len(self.__cloudFormationTemplate['Parameters']),
                    key= parameterName,
                    value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the VPC endpoints",
                        "Type": "String",
                        "Default": subnetCIDR
                    })
                )
                self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the VPC endpoints", indent=2)
                # The resource
                resourceName = "VPCEndpointSubnet" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= resourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::Subnet",
                        "Properties": {
                            "VpcId": {"Ref": vpcResourceName},
                            "CidrBlock": {"Ref": parameterName},
                            "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                            "MapPublicIpOnLaunch": False,
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-VPCEndpointSubnet" + str(iAZ + 1)}}]
                        }
                    })
                )
                self.__addTagsToResource(resourceName)
                commentForSubnet = " Subnet " + str(iAZ + 1)
                if iAZ == 0: commentForSubnet = '\nSubnets for the VPC endpoints\n'+ commentForSubnet
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)

        # The Network firewall subnets
        isNetworkFirewall = (self.__networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED)
        isUsingSingleAZ = (self.__networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD)
        if isNetworkFirewall:
            vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC
            vpcResourceName = "DBSVpc"
            if isHubNSpoke:
                vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.HUB_VPC
                vpcResourceName = "HubVpc"
            nfwSubnets = networkConfig[vpcTypeOfEpSubnets].subnetCIDRs()[VpcAndSubnetCIDR.SubnetType.NETWORKFIREWALL]
            for iAZ in range(len(availabilityZoneIndexes)):
                azIndex = availabilityZoneIndexes[iAZ]
                subnetCIDR = nfwSubnets[iAZ]
                # The parameter
                parameterName = "FirewallSubnet" + str(iAZ + 1) + "CidrBlock"
                self.__cloudFormationTemplate['Parameters'].insert(
                    pos= len(self.__cloudFormationTemplate['Parameters']),
                    key= parameterName,
                    value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the network firewall",
                        "Type": "String",
                        "Default": subnetCIDR
                    })
                )
                self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the network firewall", indent=2)
                # The resource
                resourceName = "FirewallSubnet" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= resourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::Subnet",
                        "Properties": {
                            "VpcId": {"Ref": vpcResourceName},
                            "CidrBlock": {"Ref": parameterName},
                            "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                            "MapPublicIpOnLaunch": False,
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-FirewallSubnet" + str(iAZ + 1)}}]
                        }
                    })
                )
                self.__addTagsToResource(resourceName)
                commentForSubnet = " Subnet " + str(iAZ + 1)
                if iAZ == 0: commentForSubnet = '\nSubnet(s) for the Network Firewall\n'+ commentForSubnet
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)
                if isUsingSingleAZ: break

        # The NAT Gateway subnets
        if isInternetEnabled:
            vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC
            vpcResourceName = "DBSVpc"
            if self.__networkArchitectureDesignOptions.vpcArchitecture() == NetworkArchitectureDesignOptions.VPCArchitectureMode.HUB_AND_SPOKE:
                vpcTypeOfEpSubnets = VpcAndSubnetCIDR.VpcType.HUB_VPC
                vpcResourceName = "HubVpc"
            natSubnets = networkConfig[vpcTypeOfEpSubnets].subnetCIDRs()[VpcAndSubnetCIDR.SubnetType.NATGATEWAY]
            for iAZ in range(len(availabilityZoneIndexes)):
                azIndex = availabilityZoneIndexes[iAZ]
                subnetCIDR = natSubnets[iAZ]
                # The parameter
                parameterName = "NatSubnet" + str(iAZ + 1) + "CidrBlock"
                self.__cloudFormationTemplate['Parameters'].insert(
                    pos= len(self.__cloudFormationTemplate['Parameters']),
                    key= parameterName,
                    value= CommentedMap({
                        "Description": "The CIDR block of subnet " + str(iAZ + 1) + " for the NAT Gateway",
                        "Type": "String",
                        "Default": subnetCIDR
                    })
                )
                self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key =parameterName, before= "The CIDR block of subnet " + str(iAZ + 1) + " for the NAT Gateway(s)", indent=2)
                # The resource
                resourceName = "NatSubnet" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= resourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::Subnet",
                        "Properties": {
                            "VpcId": {"Ref": vpcResourceName},
                            "CidrBlock": {"Ref": parameterName},
                            "AvailabilityZone": {"Fn::Select": [azIndex, {"Fn::GetAZs": ""}]},
                            "MapPublicIpOnLaunch": False,
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-NatSubnet" + str(iAZ + 1)}}]
                        }
                    })
                )
                self.__addTagsToResource(resourceName)
                commentForSubnet = " Subnet " + str(iAZ + 1)
                if iAZ == 0: commentForSubnet = '\nSubnet(s) for the NAT Gateway(s)\n'+ commentForSubnet
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=resourceName, before=commentForSubnet, indent=2)
                if isUsingSingleAZ: break

        # The NAT Gateway and Elastic IP address
        if isInternetEnabled:
            for iAZ in range(len(availabilityZoneIndexes)):
                # The Elastic IP
                eipResourceName = "ElasticIPForNat" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= eipResourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::EIP",
                        "Properties": {
                            "Domain": "vpc",
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-" + eipResourceName}}]
                        }
                    })
                )
                self.__addTagsToResource(eipResourceName)
                commentForIPs = " Elastic IP " + str(iAZ + 1)
                if iAZ == 0: commentForIPs = '\nNAT Gateway(s) and their Elastic IP address(es)\n'+ commentForIPs
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=eipResourceName, before=commentForIPs, indent=2)
                # The NAT Gateway
                natResourceName = "NatGateway" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos= len(self.__cloudFormationTemplate['Resources']),
                    key= natResourceName,
                    value= CommentedMap({
                        "Type": "AWS::EC2::NatGateway",
                        "Properties": {
                            "AllocationId": {"Fn::GetAtt": eipResourceName + ".AllocationId"},
                            "ConnectivityType": "public",
                            "SubnetId": {"Ref": "NatSubnet" + str(iAZ + 1)},
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub":"${AWS::StackName}-" + natResourceName}}]
                        }
                    })
                )
                self.__addTagsToResource(natResourceName)
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=natResourceName, before=" NAT Gateway " + str(iAZ + 1), indent=2)
                if isUsingSingleAZ: break
            # Required permissions
            self.__requiredPrivileges.add("ec2:AllocateAddress")
            self.__requiredPrivileges.add("ec2:AssociateAddress")
            self.__requiredPrivileges.add("ec2:DescribeAddresses")
            self.__requiredPrivileges.add("ec2:CreateNatGateway")
            self.__requiredPrivileges.add("ec2:DescribeNatGateways")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteVpc")
            self.__requiredPrivilegesForRollback.add("ec2:ReleaseAddress")
            self.__requiredPrivilegesForRollback.add("ec2:DisassociateAddress")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteNatGateway")

        ### The Network Firewall
        if isNetworkFirewall:
            # The list of domains to be whitelisted for HTTPS access
            whiteListedDomains = ".databricks.com, .amazonaws.com, .pypi.org, .pythonhosted.org, .cran.r-project.org, .maven.org, .storage-download.googleapis.com, .spark-packages.org"
            self.__cloudFormationTemplate['Parameters'].insert(
                pos= len(self.__cloudFormationTemplate['Parameters']),
                key= "WhitelistedDomainsForNetworkFirewall",
                value= CommentedMap({
                    "Description": "The list of domains to be whitelisted for HTTPS access",
                    "Type": "CommaDelimitedList",
                    "Default": whiteListedDomains
                })
            )
            self.__cloudFormationTemplate['Parameters'].yaml_set_comment_before_after_key(key="WhitelistedDomainsForNetworkFirewall", before= "The list of domains to be whitelisted for HTTPS access", indent=2)
            # The resource
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "StatefulNetworkFirewallRulesForWhiteListedDomains",
                value= CommentedMap({
                    "Type": "AWS::NetworkFirewall::RuleGroup",
                    "Properties": {
                        "RuleGroupName": {"Fn::Sub": "${AWS::StackName}-StatefulNetworkFirewallRulesForWhiteListedDomains"},
                        "Description": "Rules allowing https access to a list of domains",
                        "Type": "STATEFUL",
                        "Capacity": 100,
                        "RuleGroup": {
                            "RuleVariables": {"IPSets":{"HOME_NET": {"Definition":["10.0.0.0/8"]}}},
                            "RulesSource": {
                                "RulesSourceList": {
                                    "GeneratedRulesType": "ALLOWLIST",
                                    "Targets": {"Ref": "WhitelistedDomainsForNetworkFirewall"},
                                    "TargetTypes": ["TLS_SNI"]
                                }
                            }
                        }
                    }
                })
            )
            self.__addTagsToResource("StatefulNetworkFirewallRulesForWhiteListedDomains")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="StatefulNetworkFirewallRulesForWhiteListedDomains",
                before= "\nThe Network firewall, rules and policy\n The stateful rule for whitelisted domains",
                indent=2
            )
            # Required permissions
            self.__requiredPrivileges.add("network-firewall:CreateRuleGroup")
            self.__requiredPrivileges.add("network-firewall:DescribeRuleGroup")
            self.__requiredPrivileges.add("network-firewall:ListRuleGroups")
            self.__requiredPrivileges.add("network-firewall:TagResource")
            self.__requiredPrivilegesForRollback.add("network-firewall:DeleteRuleGroup")

            # The network firewall policy stateful rules for legacy metastore
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "StatefulNetworkFirewallRulesForLegacyMetastore",
                value= CommentedMap({
                    "Type": "AWS::NetworkFirewall::RuleGroup",
                    "Properties": {
                        "RuleGroupName": {"Fn::Sub": "${AWS::StackName}-StatefulNetworkFirewallRulesForLegacyMetastore"},
                        "Description": "Rules allowing access to the 3306 port",
                        "Type": "STATEFUL",
                        "Capacity": 10,
                        "RuleGroup": {
                            "RulesSource": {
                                "StatefulRules": [
                                    {
                                        "Action": "PASS",
                                        "Header": {
                                            "Protocol": "TCP",
                                            "Direction": "ANY",
                                            "Source": "10.0.0.0/8",
                                            "SourcePort": "ANY",
                                            "Destination": "ANY",
                                            "DestinationPort": 3306
                                        },
                                        "RuleOptions": [
                                            {
                                                "Keyword": "sid:1000001"
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                    }
                })
            )
            self.__addTagsToResource("StatefulNetworkFirewallRulesForLegacyMetastore")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="StatefulNetworkFirewallRulesForLegacyMetastore",
                before= " The stateful rule for the legacy metastore (access to the MySQL port)",
                indent=2
            )

            # The network firewall policy stateful rules blocking access for specific protocols
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "StatefulNetworkFirewallRulesForBlockedProtocols",
                value= CommentedMap({
                    "Type": "AWS::NetworkFirewall::RuleGroup",
                    "Properties": {
                        "RuleGroupName": {"Fn::Sub": "${AWS::StackName}-StatefulNetworkFirewallRulesForBlockedProtocols"},
                        "Description": "Rules blocking access to specific protocols",
                        "Type": "STATEFUL",
                        "Capacity": 10,
                        "RuleGroup": {
                            "RulesSource": {
                                "StatefulRules": [
                                    {"Action": "DROP",
                                    "Header":{"Protocol": "FTP", "Direction": "ANY", "Source": "ANY", "SourcePort": "ANY", "Destination": "ANY", "DestinationPort": "ANY"},
                                    "RuleOptions":[{"Keyword":"sid:2000001"}]},
                                    {"Action": "DROP",
                                    "Header":{"Protocol": "SSH", "Direction": "ANY", "Source": "ANY", "SourcePort": "ANY", "Destination": "ANY", "DestinationPort": "ANY"},
                                    "RuleOptions":[{"Keyword":"sid:2000002"}]},
                                    {"Action": "DROP",
                                    "Header":{"Protocol": "ICMP", "Direction": "ANY", "Source": "ANY", "SourcePort": "ANY", "Destination": "ANY", "DestinationPort": "ANY"},
                                    "RuleOptions":[{"Keyword":"sid:2000003"}]},
                                ]
                            }
                        },
                    }
                })
            )
            self.__addTagsToResource("StatefulNetworkFirewallRulesForBlockedProtocols")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="StatefulNetworkFirewallRulesForBlockedProtocols",
                before= " The stateful rule for blocking specific protocols",
                indent=2
            )

            # The network firewall policy
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "NetworkFirewallPolicy",
                value= CommentedMap({
                    "Type": "AWS::NetworkFirewall::FirewallPolicy",
                    "Properties": {
                        "FirewallPolicyName": {"Fn::Sub": "${AWS::StackName}-NetworkFirewallPolicy"},
                        "Description": "Network Firewall Policy for Databricks",
                        "FirewallPolicy": {
                            "PolicyVariables": {"RuleVariables": {"HOME_NET": {"Definition": ["10.0.0.0/8"]}}},
                            "StatefulRuleGroupReferences": [
                                {"ResourceArn": {"Ref": "StatefulNetworkFirewallRulesForWhiteListedDomains"}},
                                {"ResourceArn": {"Ref": "StatefulNetworkFirewallRulesForLegacyMetastore"}},
                                {"ResourceArn": {"Ref": "StatefulNetworkFirewallRulesForBlockedProtocols"}}
                            ],
                            "StatelessDefaultActions": ["aws:forward_to_sfe"],
                            "StatelessFragmentDefaultActions": ["aws:forward_to_sfe"]
                        },
                    }
                })
            )
            self.__addTagsToResource("NetworkFirewallPolicy")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="NetworkFirewallPolicy",
                before= " Network Firewall policy",
                indent=2
            )
            # Required permissions
            self.__requiredPrivileges.add("network-firewall:CreateFirewallPolicy")
            self.__requiredPrivileges.add("network-firewall:DescribeFirewallPolicy")
            self.__requiredPrivilegesForRollback.add("network-firewall:DeleteFirewallPolicy")

            # The network firewall
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "NetworkFirewall",
                value= CommentedMap({
                    "Type": "AWS::NetworkFirewall::Firewall",
                    "Properties": {
                        "FirewallName": {"Fn::Sub": "${AWS::StackName}-NetworkFirewall"},
                        "Description": "Primary network firewall for Databricks",
                        "FirewallPolicyArn": {"Ref": "NetworkFirewallPolicy"},
                        "DeleteProtection": False,
                        "FirewallPolicyChangeProtection": False,
                        "SubnetChangeProtection": True,
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "SubnetMappings": [],
                    }
                })
            )
            for iAZ in range(len(availabilityZoneIndexes)):
                subnetMapping = {"SubnetId": {"Ref": "FirewallSubnet" + str(iAZ+1)}}
                self.__cloudFormationTemplate["Resources"]["NetworkFirewall"]["Properties"]["SubnetMappings"].append(subnetMapping)
                if isUsingSingleAZ: break
            self.__addTagsToResource("NetworkFirewall")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="NetworkFirewall",
                before= " The Network Firewall itself",
                indent=2
            )
            # Required permissions
            self.__requiredPrivileges.add("network-firewall:CreateFirewall")
            self.__requiredPrivileges.add("network-firewall:DescribeFirewall")
            self.__requiredPrivileges.add("network-firewall:AssociateFirewallPolicy")
            self.__requiredPrivileges.add("network-firewall:AssociateSubnets")
            self.__requiredPrivilegesForRollback.add("network-firewall:DeleteFirewall")
            self.__requiredPrivilegesForRollback.add("logs:ListLogDeliveries")

        # The Transit gateway
        if isHubNSpoke:
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "TransitGateway",
                value= CommentedMap({
                    "Type": "AWS::EC2::TransitGateway",
                    "Properties": {
                        "Description": "The transit gateway connecting the Databricks VPC with the Hub",
                        "AutoAcceptSharedAttachments": "disable",
                        "DefaultRouteTableAssociation": "disable",
                        "DefaultRouteTablePropagation": "disable",
                        "DnsSupport": "enable",
                        "SecurityGroupReferencingSupport": "enable",
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-TransitGateway"}}]
                    }
                })
            )
            self.__addTagsToResource("TransitGateway")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGateway",
                before= "\nThe Transit Gateway and its VPC attachments",
                indent=2
            )
            # Required permissions
            self.__requiredPrivileges.add("ec2:CreateTransitGateway")
            self.__requiredPrivileges.add("ec2:ModifyTransitGateway")
            self.__requiredPrivileges.add("ec2:DescribeTransitGateways")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteTransitGateway")

            # Hub VPC attachment
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "HubVpcTransitGatewayAttachment",
                value= CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayAttachment",
                    "Properties": {
                        "TransitGatewayId": {"Ref": "TransitGateway"},
                        "VpcId": {"Ref": "HubVpc"},
                        "SubnetIds": [],
                        "Options": {
                            "ApplianceModeSupport": "enable",
                            "DnsSupport": "enable",
                            "SecurityGroupReferencingSupport": "enable"
                        },
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-TGwyAttachmentToHubVPC"}}]
                    }
                })
            )
            self.__addTagsToResource("HubVpcTransitGatewayAttachment")
            for iAZ in range(len(availabilityZoneIndexes)):
                subnetMapping = {"Ref": "HubVPCTransitGatewaySubnet" + str(iAZ+1)}
                self.__cloudFormationTemplate["Resources"]["HubVpcTransitGatewayAttachment"]["Properties"]["SubnetIds"].append(subnetMapping)
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="HubVpcTransitGatewayAttachment",
                before= " The Transit Gateway Attachment on the Hub VPC",
                indent=2
            )
            # Required permissions
            self.__requiredPrivileges.add("ec2:CreateTransitGatewayVpcAttachment")
            self.__requiredPrivileges.add("ec2:DescribeTransitGatewayVpcAttachments")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteTransitGatewayVpcAttachment")

            # Databricks VPC attachment
            self.__cloudFormationTemplate['Resources'].insert(
                pos= len(self.__cloudFormationTemplate['Resources']),
                key= "DBSVpcTransitGatewayAttachment",
                value= CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayAttachment",
                    "Properties": {
                        "TransitGatewayId": {"Ref": "TransitGateway"},
                        "VpcId": {"Ref": "DBSVpc"},
                        "SubnetIds": [],
                        "Options": {
                            "ApplianceModeSupport": "enable",
                            "DnsSupport": "enable",
                            "SecurityGroupReferencingSupport": "enable"
                        },
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-TGwyAttachmentToDBSVPC"}}]
                    }
                })
            )
            self.__addTagsToResource("DBSVpcTransitGatewayAttachment")
            for iAZ in range(len(availabilityZoneIndexes)):
                subnetMapping = {"Ref": "DBSVPCTransitGatewaySubnet" + str(iAZ+1)}
                self.__cloudFormationTemplate["Resources"]["DBSVpcTransitGatewayAttachment"]["Properties"]["SubnetIds"].append(subnetMapping)
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="DBSVpcTransitGatewayAttachment",
                before= " The Transit Gateway Attachment on the Databricks VPC",
                indent=2
            )

        ## The route tables        
        # The route table(s) for the cluster subnets
        for iAZ in range(len(availabilityZoneIndexes)):
            rtResourceName = "DBSClusterSubnetRouteTable" + str(iAZ + 1)
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key=rtResourceName,
                value=CommentedMap({            
                    "Type": "AWS::EC2::RouteTable",
                    "Properties": {
                        "VpcId": {"Ref": "DBSVpc"},
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-" + rtResourceName}}]
                    }
                })
            )
            self.__addTagsToResource(rtResourceName)
            commentForRT = "\n Route table for cluster subnet " + str(iAZ + 1)
            if iAZ == 0: commentForRT = '\nRoute Tables\n'+ commentForRT
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=rtResourceName, before=commentForRT, indent=2)

            # The route to the internet or other VPCs
            routeToInternetResourceName = None
            if isHubNSpoke or isInternetEnabled:
                routeToInternetResourceName = "RouteToInternetInDBSClusterSubnetRouteTable" + str(iAZ + 1)
                # Set up a route to the transit gateway
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=routeToInternetResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::Route",
                        "Properties": {
                            "RouteTableId": {"Ref": rtResourceName},
                            "DestinationCidrBlock": "0.0.0.0/0"
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=routeToInternetResourceName,
                    before="  Route to internet", indent=2)
                # Case of a hub and spoke architecture
                if isHubNSpoke:
                    self.__cloudFormationTemplate["Resources"][routeToInternetResourceName]['DependsOn'] = "DBSVpcTransitGatewayAttachment"
                    self.__cloudFormationTemplate["Resources"][routeToInternetResourceName]["Properties"]["TransitGatewayId"] = {
                        "Ref": "TransitGateway"
                    }
                else:
                    idx = 0 if isUsingSingleAZ else iAZ
                    # In case where there is a network firewall
                    if isNetworkFirewall:
                        self.__cloudFormationTemplate["Resources"][routeToInternetResourceName]["Properties"]["VpcEndpointId"] = {
                            "Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [idx, {"Fn::GetAtt": "NetworkFirewall.EndpointIds"}]}]}]
                        }
                    else: # Otherwise route traffice to NAT Gateway
                        self.__cloudFormationTemplate["Resources"][routeToInternetResourceName]["Properties"]["GatewayId"] = {"Ref": "NatGateway" + str(idx + 1)}

            # Attach to the subnet
            rtAssocResourceName = "DBSClusterSubnet" + str(iAZ + 1) + "RouteTableAssociation"
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key=rtAssocResourceName,
                value=CommentedMap({
                    "Type": "AWS::EC2::SubnetRouteTableAssociation",
                    "Properties": {
                        "RouteTableId": {"Ref": rtResourceName},
                        "SubnetId": {"Ref": "DBSClusterSubnet" + str(iAZ + 1)}
                    }
                })
            )
            if routeToInternetResourceName is not None:
                self.__cloudFormationTemplate["Resources"][rtAssocResourceName]["DependsOn"] = routeToInternetResourceName
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key=rtAssocResourceName,
                before="  ...attached to the subnet", indent=2)
        # Required permissions
        self.__requiredPrivileges.add("ec2:CreateRouteTable")
        self.__requiredPrivileges.add("ec2:DescribeRouteTables")
        self.__requiredPrivileges.add("ec2:CreateRoute")
        self.__requiredPrivileges.add("ec2:AssociateRouteTable")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteRouteTable")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteRoute")
        self.__requiredPrivilegesForRollback.add("ec2:DisassociateRouteTable")

        # Route tables for the endpoint subnets
        if isPrivateLinkEnabled:
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="EndpointSubnetsRouteTable",
                value=CommentedMap({
                    "Type": "AWS::EC2::RouteTable",
                    "Properties": {
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-" + "EndpointSubnetsRouteTable"}}]
                    }
                })
            )
            self.__addTagsToResource("EndpointSubnetsRouteTable")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="EndpointSubnetsRouteTable",
                before="\n Route table for the VPC Endpoint Subnets", indent=2)

            if isHubNSpoke:
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RouteToInternetInHubVpcEndpointSubnetsRouteTable",
                    value=CommentedMap({
                        "DependsOn": "HubVpcTransitGatewayAttachment",
                        "Type": "AWS::EC2::Route",
                        "Properties": {
                            "RouteTableId": {"Ref": "EndpointSubnetsRouteTable"},
                            "DestinationCidrBlock": "10.0.0.0/8",
                            "TransitGatewayId": {"Ref": "TransitGateway"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RouteToInternetInHubVpcEndpointSubnetsRouteTable",
                    before="  Route to the Databricks cluster subnets via the Transit Gateway", indent=2)
            # Associate it to the subnets
            for iAZ in range(len(availabilityZoneIndexes)):
                subnetName = "VPCEndpointSubnet" + str(iAZ + 1)
                resourceName = "EndpointSubnet" + str(iAZ + 1) + "RouteTableAssociation"
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=resourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::SubnetRouteTableAssociation",
                        "Properties": {
                            "RouteTableId": {"Ref": "EndpointSubnetsRouteTable"},
                            "SubnetId": {"Ref": subnetName}
                        }
                    })
                )
                if isHubNSpoke:
                    self.__cloudFormationTemplate["Resources"][resourceName]["DependsOn"] = "RouteToInternetInHubVpcEndpointSubnetsRouteTable"
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=resourceName,
                    before="  ...attached to the endpoint subnet " + str(iAZ + 1), indent=2)

        # Route tables for the firewall subnets
        if isNetworkFirewall:
            for iAZ in range(len(availabilityZoneIndexes)):
                rtResourceName = "FirewallRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=rtResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::RouteTable",
                        "Properties": {
                            "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-" + rtResourceName}}]
                        }
                    })
                )
                self.__addTagsToResource(rtResourceName)
                commentForRT = "\n Route table for the network firewall subnet " + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=rtResourceName, before=commentForRT, indent=2)
                # Route to internet
                rtRouteResourceName = "RouteToInternetInFirewallRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=rtRouteResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::Route",
                        "Properties": {
                            "RouteTableId": {"Ref": rtResourceName},
                            "DestinationCidrBlock": "0.0.0.0/0",
                            "GatewayId": {"Ref": "NatGateway" + str(iAZ + 1)}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=rtRouteResourceName,
                    before="  Route to internet", indent=2)
                rtRouteToClustersResourceName = "RouteToVPCsInFirewallRouteTable" + str(iAZ + 1)
                if isHubNSpoke: # Route to the cluster subnet through the transit gateway
                    self.__cloudFormationTemplate['Resources'].insert(
                        pos=len(self.__cloudFormationTemplate['Resources']),
                        key=rtRouteToClustersResourceName,
                        value=CommentedMap({
                            "DependsOn": "HubVpcTransitGatewayAttachment",
                            "Type": "AWS::EC2::Route",
                            "Properties": {
                                "RouteTableId": {"Ref": rtResourceName},
                                "DestinationCidrBlock": "10.0.0.0/8",
                                "TransitGatewayId": {"Ref": "TransitGateway"}
                            }
                        })
                    )
                # Associate the route table to the subnet
                resourceName = "FirewallSubnetRouteTable" + str(iAZ + 1) + "Association"
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=resourceName,
                    value=CommentedMap({
                        "DependsOn": [rtRouteResourceName, rtRouteToClustersResourceName] if isHubNSpoke else rtRouteResourceName,
                        "Type": "AWS::EC2::SubnetRouteTableAssociation",
                        "Properties": {
                            "RouteTableId": {"Ref": rtResourceName},
                            "SubnetId": {"Ref": "FirewallSubnet" + str(iAZ + 1)}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=resourceName,
                    before="  ...attached to the network firewall subnet " + str(iAZ + 1), indent=2)
                # Use only the first AZ in case of no high availability
                if isUsingSingleAZ: break

        # Route tables for the NAT subnets
        if isInternetEnabled:
            for iAZ in range(len(availabilityZoneIndexes)):
                rtResourceName = "NatRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=rtResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::RouteTable",
                        "Properties": {
                            "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-" + rtResourceName}}]
                        }
                    })
                )
                self.__addTagsToResource(rtResourceName)
                commentForRT = "\n Route table for the NAT Gateway subnet " + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=rtResourceName, before=commentForRT, indent=2)
                # Route to internet goes to the Internet Gateway
                routeToInternetResourceName = "RouteToInternetInNatSubnetRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=routeToInternetResourceName,
                    value=CommentedMap({
                        "DependsOn": "VpcIgwAttachment",
                        "Type": "AWS::EC2::Route",
                        "Properties": {
                            "RouteTableId": {"Ref": rtResourceName},
                            "DestinationCidrBlock": "0.0.0.0/0",
                            "GatewayId": {"Ref": "Igw"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=routeToInternetResourceName,
                    before="  Route to internet", indent=2)
                returnTrafficRouteResourceName = None
                if isNetworkFirewall or isHubNSpoke:
                    returnTrafficRouteResourceName = "ReturnRouteInNatRouteTable" + str(iAZ + 1)
                    self.__cloudFormationTemplate['Resources'].insert(
                        pos=len(self.__cloudFormationTemplate['Resources']),
                        key=returnTrafficRouteResourceName,
                        value=CommentedMap({
                            "Type": "AWS::EC2::Route",
                            "Properties": {
                                "RouteTableId": {"Ref": rtResourceName},
                                "DestinationCidrBlock": "10.0.0.0/8",
                            }
                        })
                    )
                    if isNetworkFirewall: # route traffic to the network firewall
                        self.__cloudFormationTemplate["Resources"][returnTrafficRouteResourceName]["Properties"]["VpcEndpointId"] = {
                            "Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [iAZ, {"Fn::GetAtt": "NetworkFirewall.EndpointIds"}]}]}]
                        }
                    else: # route traffic to the transit gateway
                        self.__cloudFormationTemplate["Resources"][returnTrafficRouteResourceName]["Properties"]["TransitGatewayId"] = {
                            "Ref": "TransitGateway"
                        }
                        self.__cloudFormationTemplate["Resources"][returnTrafficRouteResourceName]["DependsOn"] = "HubVpcTransitGatewayAttachment"
                    self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                        key=returnTrafficRouteResourceName,
                        before="  Route to the Databricks clusters", indent=2)
                # Attach to the subnet
                resourceName = "NatSubnetRouteTable" + str(iAZ + 1) + "Association"
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=resourceName,
                    value=CommentedMap({
                        "DependsOn": routeToInternetResourceName if returnTrafficRouteResourceName is None else [routeToInternetResourceName, returnTrafficRouteResourceName],
                        "Type": "AWS::EC2::SubnetRouteTableAssociation",
                        "Properties": {
                            "RouteTableId": {"Ref": rtResourceName},
                            "SubnetId": {"Ref": "NatSubnet" + str(iAZ + 1)}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=resourceName,
                    before="  ...attached to the NAT Gatway subnet " + str(iAZ + 1), indent=2)
                # Use only the first AZ in case of no high availability
                if isUsingSingleAZ: break

        # Route tables for the Transit Gateway subnets and the attachments
        if isHubNSpoke:
            for iAZ in range(len(availabilityZoneIndexes)):
                # The route table for the subnet on the Hub VPC
                rtHubResourceName = "HubVpcTransitGatewaySubnetsRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=rtHubResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::RouteTable",
                        "Properties": {
                            "VpcId": {"Ref": "HubVpc"},
                            "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-" + rtHubResourceName}}]
                        }
                    })
                )
                self.__addTagsToResource(rtHubResourceName)
                commentForRT = "\n Route table for the Transit Gateway subnet " + str(iAZ + 1) + " in the Hub VPC"
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(key=rtHubResourceName, before=commentForRT, indent=2)
                # Route to the spoke VPCs
                rtHubRouteToSpokeVpcsResourceName = "RouteToSpokeVpcsInHubVpcTransitGatewaySubnetsRouteTable" + str(iAZ + 1)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=rtHubRouteToSpokeVpcsResourceName,
                    value=CommentedMap({
                        "DependsOn": "HubVpcTransitGatewayAttachment",
                        "Type": "AWS::EC2::Route",
                        "Properties": {
                            "RouteTableId": {"Ref": rtHubResourceName},
                            "DestinationCidrBlock": "10.0.0.0/8",
                            "TransitGatewayId": {"Ref": "TransitGateway"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=rtHubRouteToSpokeVpcsResourceName,
                    before="  Route to the Databricks VPC", indent=2)
                # Route to the Internet
                idx = 0 if isUsingSingleAZ else iAZ
                rtHubRouteToInternetResourceName = None
                if isInternetEnabled:
                    rtHubRouteToInternetResourceName = "RouteToInternetInHubVpcTransitGatewaySubnetsRouteTable" + str(iAZ + 1)
                    self.__cloudFormationTemplate['Resources'].insert(
                        pos=len(self.__cloudFormationTemplate['Resources']),
                        key=rtHubRouteToInternetResourceName,
                        value=CommentedMap({
                            "Type": "AWS::EC2::Route",
                            "Properties": {
                                "RouteTableId": {"Ref": rtHubResourceName},
                                "DestinationCidrBlock": "0.0.0.0/0"
                            }
                        })
                    )
                    self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                        key=rtHubRouteToInternetResourceName,
                        before="  Route to the Internet", indent=2)
                    if isNetworkFirewall: # Send traffic to the firewall
                        self.__cloudFormationTemplate["Resources"][rtHubRouteToInternetResourceName]["Properties"]["VpcEndpointId"] = {
                            "Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [idx, {"Fn::GetAtt": "NetworkFirewall.EndpointIds"}]}]}]
                        }
                    else: # Send traffic to the NAT Gateway
                        self.__cloudFormationTemplate["Resources"][rtHubRouteToInternetResourceName]["Properties"]["GatewayId"] = {"Ref": "NatGateway" + str(idx + 1)}
                # Associate to the subnet
                resourceName = "HubVpcTransitGatewaySubnet1RouteTable" + str(iAZ + 1) + "Association"
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=resourceName,
                    value=CommentedMap({
                        "DependsOn": rtHubRouteToSpokeVpcsResourceName if rtHubRouteToInternetResourceName is None else [rtHubRouteToSpokeVpcsResourceName, rtHubRouteToInternetResourceName],
                        "Type": "AWS::EC2::SubnetRouteTableAssociation",
                        "Properties": {
                            "RouteTableId": {"Ref": rtHubResourceName},
                            "SubnetId": {"Ref": "HubVPCTransitGatewaySubnet" + str(iAZ + 1)}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=resourceName,
                    before="  ...attached to the Transit Gateway subnet " + str(iAZ + 1) + " in the Hub VPC", indent=2)

            # The route table for the Databricks VPC attachment
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayRouteTableDbs",
                value=CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayRouteTable",
                    "Properties": {
                        "TransitGatewayId": {"Ref": "TransitGateway"},
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-TransitGatewayRouteTableDbs"}}]
                    }
                })
            )
            self.__addTagsToResource("TransitGatewayRouteTableDbs")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGatewayRouteTableDbs",
                before="\n Route table for the Transit Gateway attachment on the Databricks VPC", indent=2)
            self.__requiredPrivileges.add("ec2:CreateTransitGatewayRouteTable")
            self.__requiredPrivileges.add("ec2:DescribeTransitGatewayRouteTables")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteTransitGatewayRouteTable")
            routeDependencies = []
            # Routes to the Hub VPC endpoint subnets
            for iAZ in range(len(availabilityZoneIndexes)):
                tgrtTableHubResourceName = "RouteToEndpointSubnet" + str(iAZ + 1) + "InTransitGatewayRouteTableDbs"
                routeDependencies.append(tgrtTableHubResourceName)
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key=tgrtTableHubResourceName,
                    value=CommentedMap({
                        "Type": "AWS::EC2::TransitGatewayRoute",
                        "Properties": {
                            "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                            "DestinationCidrBlock": {"Ref": "VPCEndpointSubnet" + str(iAZ + 1) + "CidrBlock"},
                            "TransitGatewayAttachmentId": {"Ref": "HubVpcTransitGatewayAttachment"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key=tgrtTableHubResourceName,
                    before="  Route to the VPC Endpoints", indent=2)
            self.__requiredPrivileges.add("ec2:CreateTransitGatewayRoute")
            self.__requiredPrivileges.add("ec2:DescribeTransitGatewayRouteTables")
            self.__requiredPrivileges.add("ec2:SearchTransitGatewayRoutes")
            self.__requiredPrivilegesForRollback.add("ec2:DeleteTransitGatewayRoute")
            if isInternetEnabled:
                # The static route to internet through the hub VPC
                routeDependencies.append("RouteToInternetInTransitGatewayRouteTable")
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RouteToInternetInTransitGatewayRouteTable",
                    value=CommentedMap({
                        "Type": "AWS::EC2::TransitGatewayRoute",
                        "Properties": {
                            "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                            "DestinationCidrBlock": "0.0.0.0/0",
                            "TransitGatewayAttachmentId": {"Ref": "HubVpcTransitGatewayAttachment"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RouteToInternetInTransitGatewayRouteTable",
                    before="  Route to the Internet", indent=2)
            # Block other inter-vpc communication
            routeDependencies.append("BlockRouteToVPCsInTransitGatewayRouteTable")
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="BlockRouteToVPCsInTransitGatewayRouteTable",
                value=CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayRoute",
                    "Properties": {
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                        "DestinationCidrBlock": "10.0.0.0/8",
                        "Blackhole": True
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="BlockRouteToVPCsInTransitGatewayRouteTable",
                before="  blocks traffic to other hub VPCs", indent=2)
            # The route table associations
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayAttachmentForDBSVpcRouteTableAssociation",
                value=CommentedMap({
                    "DependsOn": routeDependencies,
                    "Type": "AWS::EC2::TransitGatewayRouteTableAssociation",
                    "Properties": {
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                        "TransitGatewayAttachmentId": {"Ref": "DBSVpcTransitGatewayAttachment"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGatewayAttachmentForDBSVpcRouteTableAssociation",
                before="  attaching the route table to the Transit Gateway attachment of the Databricks VPC", indent=2)
            self.__requiredPrivileges.add("ec2:AssociateTransitGatewayRouteTable")
            self.__requiredPrivileges.add("ec2:GetTransitGatewayRouteTableAssociations")
            self.__requiredPrivilegesForRollback.add("ec2:DisassociateTransitGatewayRouteTable")

            # The route table for the Hub VPC attachment
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayRouteTableHub",
                value=CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayRouteTable",
                    "Properties": {
                        "TransitGatewayId": {"Ref": "TransitGateway"},
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-TransitGatewayRouteTableHub"}}]
                    }
                })
            )
            self.__addTagsToResource("TransitGatewayRouteTableHub")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGatewayRouteTableHub",
                before="\n Route table for the Transit Gateway attachment on the Hub VPC", indent=2)
            # The static route to the Databricks VPC
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="RouteToDBSVpcInTransitGatewayRouteTable",
                value=CommentedMap({
                    "Type": "AWS::EC2::TransitGatewayRoute",
                    "Properties": {
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                        "DestinationCidrBlock": {"Ref": "DBSVPCCidrBlock"},
                        "TransitGatewayAttachmentId": {"Ref": "DBSVpcTransitGatewayAttachment"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="RouteToDBSVpcInTransitGatewayRouteTable",
                before="  the static route to the Databricks VPC", indent=2)
            # The route table associations
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayAttachmentForHubVpcRouteTableAssociation",
                value=CommentedMap({
                    "DependsOn": "RouteToDBSVpcInTransitGatewayRouteTable",
                    "Type": "AWS::EC2::TransitGatewayRouteTableAssociation",
                    "Properties": {
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"},
                        "TransitGatewayAttachmentId": {"Ref": "HubVpcTransitGatewayAttachment"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGatewayAttachmentForHubVpcRouteTableAssociation",
                before="  ...attached to the Transit Gateway attachment of the Hub VPC", indent=2)

            # Propagate the attachments to the routes tables
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayAttachmentForHubVpcRouteTablePropagation",
                value=CommentedMap({
                    "DependsOn": "TransitGatewayAttachmentForDBSVpcRouteTableAssociation",
                    "Type": "AWS::EC2::TransitGatewayRouteTablePropagation",
                    "Properties": {
                        "TransitGatewayAttachmentId": {"Ref": "HubVpcTransitGatewayAttachment"},
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableDbs"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="TransitGatewayAttachmentForHubVpcRouteTablePropagation",
                before="\n Propagating the route tables to the attachments", indent=2)
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="TransitGatewayAttachmentForDBSVpcRouteTablePropagation",
                value=CommentedMap({
                    "DependsOn": "TransitGatewayAttachmentForHubVpcRouteTableAssociation",
                    "Type": "AWS::EC2::TransitGatewayRouteTablePropagation",
                    "Properties": {
                        "TransitGatewayAttachmentId": {"Ref": "DBSVpcTransitGatewayAttachment"},
                        "TransitGatewayRouteTableId": {"Ref": "TransitGatewayRouteTableHub"}
                    }
                })
            )
            self.__requiredPrivileges.add("ec2:EnableTransitGatewayRouteTablePropagation")
            self.__requiredPrivileges.add("ec2:GetTransitGatewayRouteTablePropagations")
            self.__requiredPrivilegesForRollback.add("ec2:DisableTransitGatewayRouteTablePropagation")

        # The S3 VPC endpoint. It is associated to the cluster route tables. Note that tags do not yet work in cloudformation for these resources!
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="S3GatewayEndpoint",
            value=CommentedMap({
                "Type": "AWS::EC2::VPCEndpoint",
                "Properties": {
                    "ServiceName": {"Fn::Sub": "com.amazonaws.${AWS::Region}.s3"},
                    "VpcEndpointType": "Gateway",
                    "VpcId": {"Ref": "DBSVpc"},
                    "RouteTableIds": [{"Ref": "DBSClusterSubnetRouteTable" + str(iAZ + 1)} for iAZ in range(len(availabilityZoneIndexes))],
                    "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-S3GatewayEndpoint"}}]
                }
            })
        )
        self.__addTagsToResource("S3GatewayEndpoint")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="S3GatewayEndpoint",
            before="\nGateway VPC Endpoints\n\n S3 VPC Endpoint", indent=2)
        self.__requiredPrivileges.add("ec2:CreateVpcEndpoint")
        self.__requiredPrivileges.add("ec2:DescribeVpcEndpoints")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteVpcEndpoints")

        # The security group for the Databricks clusters
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClusters",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroup",
                "Properties": {
                    "GroupName": {"Fn::Sub": "${AWS::StackName}-SecurityGroupForDatabricksClusters"},
                    "VpcId": {"Ref": "DBSVpc"},
                    "GroupDescription": "Security group for the Databricks clusters",
                }
            })
        )
        self.__addTagsToResource("SecurityGroupForDatabricksClusters")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClusters",
            before="\nSecurity groups\n\n The security group for the Databricks clusters", indent=2)
        self.__requiredPrivileges.add("ec2:CreateSecurityGroup")
        self.__requiredPrivileges.add("ec2:DescribeSecurityGroups")
        self.__requiredPrivileges.add("ec2:ModifySecurityGroupRules")
        self.__requiredPrivilegesForRollback.add("ec2:DeleteSecurityGroup")
        # Allow all access from the same security group
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersDefaultTcpIngress",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupIngress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow all tcp inbound access from the same security group",
                    "SourceSecurityGroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "IpProtocol": "tcp",
                    "FromPort": 0,
                    "ToPort": 65535
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersDefaultTcpIngress",
            before="  allowing all tcp ingress from the same security group", indent=2)
        self.__requiredPrivileges.add("ec2:AuthorizeSecurityGroupIngress")
        self.__requiredPrivilegesForRollback.add("ec2:RevokeSecurityGroupIngress")
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersDefaultUdpIngress",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupIngress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow all udp inbound access from the same security group",
                    "SourceSecurityGroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "IpProtocol": "udp",
                    "FromPort": 0,
                    "ToPort": 65535
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersDefaultUdpIngress",
            before="  allowing all udp ingress from the same security group", indent=2)
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersDefaultTcpEgress",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupEgress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow all tcp outbound access to the same security group",
                    "DestinationSecurityGroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "IpProtocol": "tcp",
                    "FromPort": 0,
                    "ToPort": 65535
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersDefaultTcpEgress",
            before="  allowing all tcp egress to the same security group", indent=2)
        self.__requiredPrivileges.add("ec2:AuthorizeSecurityGroupEgress")
        self.__requiredPrivilegesForRollback.add("ec2:RevokeSecurityGroupEgress")
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersDefaultUdpEgress",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupEgress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow all udp outbound access to the same security group",
                    "DestinationSecurityGroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "IpProtocol": "udp",
                    "FromPort": 0,
                    "ToPort": 65535
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersDefaultUdpEgress",
            before="  allowing all udp egress to the same security group", indent=2)
        # Allow egress to HTTPS
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersEgressForHttps",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupEgress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow accessing Databricks infrastructure, cloud data sources, and library repositories",
                    "CidrIp": "0.0.0.0/0",
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersEgressForHttps",
            before="  allowing all https egress", indent=2)
        # Allow egress to the MySQL port for the legacy hive metastore
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersEgressForMetastore",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupEgress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow accessing the legacy Databricks hive metastore",
                    "CidrIp": "0.0.0.0/0",
                    "IpProtocol": "tcp",
                    "FromPort": 3306,
                    "ToPort": 3306
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersEgressForMetastore",
            before="  allowing all egress to the MySQL port 3306 for accessing the legacy Databricks Hive metastore", indent=2)
        # Databricks private link
        if isPrivateLinkEnabled:
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="SecurityGroupForDatabricksClustersEgressForPrivateLink",
                value=CommentedMap({
                    "Type": "AWS::EC2::SecurityGroupEgress",
                    "Properties": {
                        "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                        "Description": "Allow egress to Databricks PrivateLink endpoints",
                        "CidrIp": "0.0.0.0/0",
                        "IpProtocol": "tcp",
                        "FromPort": 6666,
                        "ToPort": 6666
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="SecurityGroupForDatabricksClustersEgressForPrivateLink",
                before="  allowing all egress to the Databricks VPC endpoints", indent=2)
        # Data plane to control plane
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="SecurityGroupForDatabricksClustersEgressForInternalCalls",
            value=CommentedMap({
                "Type": "AWS::EC2::SecurityGroupEgress",
                "Properties": {
                    "GroupId": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"},
                    "Description": "Allow egress for internal calls from the Databricks compute plane to the Databricks control plane API and for Unity Catalog logging and lineage data streaming into Databricks",
                    "CidrIp": "0.0.0.0/0",
                    "IpProtocol": "tcp",
                    "FromPort": 8443,
                    "ToPort": 8451
                }
            })
        )
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="SecurityGroupForDatabricksClustersEgressForInternalCalls",
            before="  allowing all egress to the Databricks control plane", indent=2)
        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'DatabricksSecurityGroupId',
            value= {
                "Description": "The id of the security group that is attached to the Databricks compute nodes",
                "Value": {"Fn::GetAtt": "SecurityGroupForDatabricksClusters.GroupId"}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
            key ='DatabricksSecurityGroupId',
            before= 'The id of the security group that is attached to the Databricks compute nodes', indent=2)

        # VPC endpoints and security group
        if isPrivateLinkEnabled:
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="SecurityGroupForEndpoints",
                value=CommentedMap({
                    "Type": "AWS::EC2::SecurityGroup",
                    "Properties": {
                        "GroupName": {"Fn::Sub": "${AWS::StackName}-SecurityGroupForEndpoints"},
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "GroupDescription": "Allow ingress traffic from the Databricks clusters on specific ports",
                    }
                })
            )
            self.__addTagsToResource("SecurityGroupForEndpoints")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="SecurityGroupForEndpoints",
                before="\n The security group for the VPC interface endpoints", indent=2)
            # Allow ingress and egress access from the private networks
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="SecurityGroupForEndpointsDefaultTcpIngress",
                value=CommentedMap({
                    "Type": "AWS::EC2::SecurityGroupIngress",
                    "Properties": {
                        "GroupId": {"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"},
                        "Description": "Allow all tcp inbound access from the private networks",
                        "CidrIp": "10.0.0.0/8",
                        "IpProtocol": "tcp",
                        "FromPort": 0,
                        "ToPort": 65535
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="SecurityGroupForEndpointsDefaultTcpIngress",
                before="  allowing all tcp inbound access from the private networks", indent=2)
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="SecurityGroupForEndpointsDefaultUdpIngress",
                value=CommentedMap({
                    "Type": "AWS::EC2::SecurityGroupIngress",
                    "Properties": {
                        "GroupId": {"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"},
                        "Description": "Allow all udp inbound access from the private networks",
                        "CidrIp": "10.0.0.0/8",
                        "IpProtocol": "udp",
                        "FromPort": 0,
                        "ToPort": 65535
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="SecurityGroupForEndpointsDefaultUdpIngress",
                before="  allowing all udp inbound access from the private networks", indent=2)

            # The interface VPC entpoints

            # For STS
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="STSInterfaceEndpoint",
                value=CommentedMap({
                    "Type": "AWS::EC2::VPCEndpoint",
                    "Properties": {
                        "ServiceName": {"Fn::Sub": "com.amazonaws.${AWS::Region}.sts"},
                        "VpcEndpointType": "Interface",
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "PrivateDnsEnabled": False if isHubNSpoke else True,
                        "SecurityGroupIds": [{"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"}],
                        "SubnetIds": [{"Ref": "VPCEndpointSubnet" + str(iAZ + 1)} for iAZ in range(len(availabilityZoneIndexes))],
                        "PolicyDocument": {
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {"AWS": {"Ref": "AWS::AccountId"}},
                                    "Action": [
                                        "sts:AssumeRole",
                                        "sts:GetAccessKeyInfo",
                                        "sts:GetSessionToken",
                                        "sts:DecodeAuthorizationMessage",
                                        "sts:TagSession"
                                    ],
                                    "Resource": "*"
                                },
                                {
                                    "Effect": "Allow",
                                    "Principal": {"AWS": "414351767826"},
                                    "Action": [
                                        "sts:AssumeRole",
                                        "sts:GetSessionToken",
                                        "sts:TagSession"
                                    ],
                                    "Resource": "*"
                                }
                            ]
                        },
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-STSInterfaceEndpoint"}}]
                    }
                })
            )
            self.__addTagsToResource("STSInterfaceEndpoint")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="STSInterfaceEndpoint",
                before="\nVPC Endpoints of interface type\n The STS VPC endpoint", indent=2)
            if isHubNSpoke:
                # Set up private DNS in the Databricks VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="PrivateHostedZoneForSTSEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::HostedZone",
                        "Properties": {
                            "Name": {"Fn::Sub": "sts.${AWS::Region}.amazonaws.com"},
                            "HostedZoneConfig": {"Comment": {"Fn::Sub":"Private hosted zone for sts.${AWS::Region}.amazonaws.com"}},
                            "VPCs": [{"VPCId": {"Ref": "DBSVpc"}, "VPCRegion": {"Ref": "AWS::Region"}}]
                        }
                    })
                )
                self.__addTagsToResource("PrivateHostedZoneForSTSEndoint", "HostedZoneTags")
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="PrivateHostedZoneForSTSEndoint",
                    before="  setting private DNS on the Databricks VPC for STS", indent=2)
                self.__requiredPrivileges.add("route53:CreateHostedZone")
                self.__requiredPrivileges.add("route53:AssociateVPCWithHostedZone")
                self.__requiredPrivileges.add("route53:GetChange")
                self.__requiredPrivileges.add("route53:ChangeTagsForResource")
                self.__requiredPrivilegesForRollback.add("route53:DeleteHostedZone")
                self.__requiredPrivilegesForRollback.add("route53:DisassociateVPCFromHostedZone")
                self.__requiredPrivilegesForRollback.add("route53:ListQueryLoggingConfigs")
                # Create a record set pointing to the VPC endpoint at the Hub VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RecordSetForPrivateHostedZoneForSTSEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::RecordSet",
                        "Properties": {
                            "Name": {"Fn::Sub": "sts.${AWS::Region}.amazonaws.com"},
                            "Type": "A",
                            "AliasTarget": {
                                "DNSName": {"Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "STSInterfaceEndpoint.DnsEntries"}]}]}]},
                                "HostedZoneId": {"Fn::Select": [0,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "STSInterfaceEndpoint.DnsEntries"}]}]}]}
                            },
                            "Comment": "Points to the STS VPC endpoint",
                            "HostedZoneId": {"Ref": "PrivateHostedZoneForSTSEndoint"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RecordSetForPrivateHostedZoneForSTSEndoint",
                    before="  the record set for STS in the private DNS zone", indent=2)
                self.__requiredPrivileges.add("route53:GetHostedZone")
                self.__requiredPrivileges.add("route53:ChangeResourceRecordSets")
                self.__requiredPrivileges.add("route53:ListHostedZones")

            # For Kinesis streams
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="KinesisInterfaceEndpoint",
                value=CommentedMap({
                    "Type": "AWS::EC2::VPCEndpoint",
                    "Properties": {
                        "ServiceName": {"Fn::Sub": "com.amazonaws.${AWS::Region}.kinesis-streams"},
                        "VpcEndpointType": "Interface",
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "PrivateDnsEnabled": False if isHubNSpoke else True,
                        "SecurityGroupIds": [{"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"}],
                        "SubnetIds": [{"Ref": "VPCEndpointSubnet" + str(iAZ + 1)} for iAZ in range(len(availabilityZoneIndexes))],
                        "PolicyDocument": {
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {"AWS": "414351767826"},
                                    "Action": [
                                        "kinesis:PutRecord",
                                        "kinesis:PutRecords",
                                        "kinesis:DescribeStream"
                                    ],
                                    "Resource": {"Fn::Sub": "arn:${AWS::Partition}:kinesis:${AWS::Region}:414351767826:stream/*"}
                                }
                            ]
                        },
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-KinesisInterfaceEndpoint"}}]
                    }
                })
            )
            self.__addTagsToResource("KinesisInterfaceEndpoint")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="KinesisInterfaceEndpoint",
                before="\n The STS VPC endpoint", indent=2)
            if isHubNSpoke:
                # Set up private DNS in the Databricks VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="PrivateHostedZoneForKinesisEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::HostedZone",
                        "Properties": {
                            "Name": {"Fn::Sub": "kinesis-streams.${AWS::Region}.amazonaws.com"},
                            "HostedZoneConfig": {"Comment": {"Fn::Sub":"Private hosted zone for kinesis-streams.${AWS::Region}.amazonaws.com"}},
                            "VPCs": [{"VPCId": {"Ref": "DBSVpc"}, "VPCRegion": {"Ref": "AWS::Region"}}]
                        }
                    })
                )
                self.__addTagsToResource("PrivateHostedZoneForKinesisEndoint", "HostedZoneTags")
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="PrivateHostedZoneForKinesisEndoint",
                    before="  setting private DNS on the Databricks VPC for Kinesis streams", indent=2)
                # Create a record set pointing to the VPC endpoint at the Hub VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RecordSetForPrivateHostedZoneForKinesisStreamEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::RecordSet",
                        "Properties": {
                            "Name": {"Fn::Sub": "kinesis-streams.${AWS::Region}.amazonaws.com"},
                            "Type": "A",
                            "AliasTarget": {
                                "DNSName": {"Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "KinesisInterfaceEndpoint.DnsEntries"}]}]}]},
                                "HostedZoneId": {"Fn::Select": [0,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "KinesisInterfaceEndpoint.DnsEntries"}]}]}]}
                            },
                            "Comment": "Points to the STS VPC endpoint",
                            "HostedZoneId": {"Ref": "PrivateHostedZoneForKinesisEndoint"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RecordSetForPrivateHostedZoneForKinesisStreamEndoint",
                    before="  the record set for Kinesis streams in the private DNS zone", indent=2)
    
            # For the Databricks Workspace (REST API)
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="DBSRestApiInterfaceEndpoint",
                value=CommentedMap({
                    "Type": "AWS::EC2::VPCEndpoint",
                    "Properties": {
                        "ServiceName": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "workspaceEP"]},
                        "VpcEndpointType": "Interface",
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "PrivateDnsEnabled": False if isHubNSpoke else True,
                        "SecurityGroupIds": [{"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"}],
                        "SubnetIds": [{"Ref": "VPCEndpointSubnet" + str(iAZ + 1)} for iAZ in range(len(availabilityZoneIndexes))],
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-DBSRestApiInterfaceEndpoint"}}]
                    }
                })
            )
            self.__addTagsToResource("DBSRestApiInterfaceEndpoint")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="DBSRestApiInterfaceEndpoint",
                before="\n The Databricks Workspace VPC endpoint", indent=2)
            self.__requiredPrivileges.add("route53:AssociateVPCWithHostedZone")
            # Register the output
            self.__cloudFormationTemplate["Outputs"].insert(
                pos= len(self.__cloudFormationTemplate["Outputs"]),
                key= 'DatabricksWorkspaceVpcEndpoint',
                value= {
                    "Description": "The workspace (REST API) VPC endpoint for Databricks",
                    "Value": {"Ref": "DBSRestApiInterfaceEndpoint"}
                }
            )
            self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
                key ='DatabricksWorkspaceVpcEndpoint',
                before= 'The id of VPC Endpoint for the Databricks REST API', indent=2)
            if isHubNSpoke:
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="PrivateHostedZoneForDatabricksWorkspaceEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::HostedZone",
                        "Properties": {
                            "Name": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "workspace"]},
                            "HostedZoneConfig": {"Comment": "Private hosted zone for the Dabricks control plane"},
                            "VPCs": [{"VPCId": {"Ref": "DBSVpc"}, "VPCRegion": {"Ref": "AWS::Region"}}]
                        }
                    })
                )
                self.__addTagsToResource("PrivateHostedZoneForDatabricksWorkspaceEndoint", "HostedZoneTags")
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="PrivateHostedZoneForDatabricksWorkspaceEndoint",
                    before="  setting private DNS on the Databricks VPC for the Databricks REST API service", indent=2)
                # Create a record set pointing to the VPC endpoint at the Hub VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RecordSetForPrivateHostedZoneForDatabricksWorkspaceEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::RecordSet",
                        "Properties": {
                            "Name": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "workspace"]},
                            "Type": "A",
                            "AliasTarget": {
                                "DNSName": {"Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "DBSRestApiInterfaceEndpoint.DnsEntries"}]}]}]},
                                "HostedZoneId": {"Fn::Select": [0,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "DBSRestApiInterfaceEndpoint.DnsEntries"}]}]}]}
                            },
                            "Comment": "Points to the Databricks workspace VPC endpoint",
                            "HostedZoneId": {"Ref": "PrivateHostedZoneForDatabricksWorkspaceEndoint"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RecordSetForPrivateHostedZoneForDatabricksWorkspaceEndoint",
                    before="  the record set for the Databricks REST API service in the private DNS zone", indent=2)




    
            # For the Databricks SCC relay
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="DBSRelayApiInterfaceEndpoint",
                value=CommentedMap({
                    "Type": "AWS::EC2::VPCEndpoint",
                    "Properties": {
                        "ServiceName": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "backendEP"]},
                        "VpcEndpointType": "Interface",
                        "VpcId": {"Ref": "HubVpc" if isHubNSpoke else "DBSVpc"},
                        "PrivateDnsEnabled": False if isHubNSpoke else True,
                        "SecurityGroupIds": [{"Fn::GetAtt": "SecurityGroupForEndpoints.GroupId"}],
                        "SubnetIds": [{"Ref": "VPCEndpointSubnet" + str(iAZ + 1)} for iAZ in range(len(availabilityZoneIndexes))],
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-DBSRelayApiInterfaceEndpoint"}}]
                    }
                })
            )
            self.__addTagsToResource("DBSRelayApiInterfaceEndpoint")
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="DBSRelayApiInterfaceEndpoint",
                before="\n The Databricks SCCR VPC endpoint", indent=2)
            # Register the output
            self.__cloudFormationTemplate["Outputs"].insert(
                pos= len(self.__cloudFormationTemplate["Outputs"]),
                key= 'DatabricksBackendVpcEndpoint',
                value= {
                    "Description": "The backend (SCCR) VPC endpoint for Databricks",
                    "Value": {"Ref": "DBSRelayApiInterfaceEndpoint"}
                }
            )
            self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
                key ='DatabricksBackendVpcEndpoint',
                before= 'The id of VPC Endpoint for the Databricks backend', indent=2)
            if isHubNSpoke:
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="PrivateHostedZoneForDatabricksBackendEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::HostedZone",
                        "Properties": {
                            "Name": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "backend"]},
                            "HostedZoneConfig": {"Comment": "Private hosted zone for the Dabricks backend"},
                            "VPCs": [{"VPCId": {"Ref": "DBSVpc"}, "VPCRegion": {"Ref": "AWS::Region"}}]
                        }
                    })
                )
                self.__addTagsToResource("PrivateHostedZoneForDatabricksBackendEndoint", "HostedZoneTags")
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="PrivateHostedZoneForDatabricksBackendEndoint",
                    before="  setting private DNS on the Databricks VPC for the Databricks SCCR service", indent=2)
                # Create a record set pointing to the VPC endpoint at the Hub VPC
                self.__cloudFormationTemplate['Resources'].insert(
                    pos=len(self.__cloudFormationTemplate['Resources']),
                    key="RecordSetForPrivateHostedZoneForDatabricksBackendEndoint",
                    value=CommentedMap({
                        "Type": "AWS::Route53::RecordSet",
                        "Properties": {
                            "Name": {"Fn::FindInMap": ["DatabricksAddresses", {"Ref": "AWS::Region"}, "backend"]},
                            "Type": "A",
                            "AliasTarget": {
                                "DNSName": {"Fn::Select": [1,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "DBSRelayApiInterfaceEndpoint.DnsEntries"}]}]}]},
                                "HostedZoneId": {"Fn::Select": [0,{"Fn::Split": [":",{"Fn::Select": [0, {"Fn::GetAtt": "DBSRelayApiInterfaceEndpoint.DnsEntries"}]}]}]}
                            },
                            "Comment": "Points to the Databricks workspace VPC endpoint",
                            "HostedZoneId": {"Ref": "PrivateHostedZoneForDatabricksBackendEndoint"}
                        }
                    })
                )
                self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                    key="RecordSetForPrivateHostedZoneForDatabricksBackendEndoint",
                    before="  the record set for the Databricks SCCR service in the private DNS zone", indent=2)


    # Defines the IAM role Resource
    def __defineWorkspaceIamRole(self):
        # The Cross Account IAM role
        self.__cloudFormationTemplate['Resources'].insert(
            pos=len(self.__cloudFormationTemplate['Resources']),
            key="WorkspaceIamRole",
            value=CommentedMap({
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": {"Fn::Sub": "${AWS::StackName}-WorkspaceIamRole"},
                    "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-WorkspaceIamRole"}}],
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Sid": "",
                                "Principal": {"AWS": "arn:aws:iam::414351767826:root"},
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Condition": {"StringEquals": {"sts:ExternalId": {"Ref": "DatabricksAccountId"}}}
                            }
                        ],
                        "Version": "2012-10-17"
                    },
                    "Path": "/",
                    "Policies": [
                        {
                            "PolicyName": {"Fn::Sub": "${AWS::StackName}-WorkspaceIAMRolePolicy"},
                            "PolicyDocument": {
                                "Statement": [
                                    {
                                        "Sid": "GeneralPermissions",
                                        "Effect": "Allow",
                                        "Action": [
                                            "ec2:AssociateIamInstanceProfile",
                                            "ec2:AttachVolume",
                                            "ec2:AuthorizeSecurityGroupEgress",
                                            "ec2:AuthorizeSecurityGroupIngress",
                                            "ec2:CancelSpotInstanceRequests",
                                            "ec2:CreateTags",
                                            "ec2:CreateVolume",
                                            "ec2:DeleteTags",
                                            "ec2:DeleteVolume",
                                            "ec2:DescribeAvailabilityZones",
                                            "ec2:DescribeIamInstanceProfileAssociations",
                                            "ec2:DescribeInstanceStatus",
                                            "ec2:DescribeInstances",
                                            "ec2:DescribeInternetGateways",
                                            "ec2:DescribeNatGateways",
                                            "ec2:DescribeNetworkAcls",
                                            "ec2:DescribePrefixLists",
                                            "ec2:DescribeReservedInstancesOfferings",
                                            "ec2:DescribeRouteTables",
                                            "ec2:DescribeSecurityGroups",
                                            "ec2:DescribeSpotInstanceRequests",
                                            "ec2:DescribeSpotPriceHistory",
                                            "ec2:DescribeSubnets",
                                            "ec2:DescribeVolumes",
                                            "ec2:DescribeVpcAttribute",
                                            "ec2:DescribeVpcs",
                                            "ec2:DetachVolume",
                                            "ec2:DisassociateIamInstanceProfile",
                                            "ec2:ReplaceIamInstanceProfileAssociation",
                                            "ec2:RequestSpotInstances",
                                            "ec2:RevokeSecurityGroupEgress",
                                            "ec2:RevokeSecurityGroupIngress",
                                            "ec2:RunInstances",
                                            "ec2:TerminateInstances",
                                            "ec2:DescribeFleetHistory",
                                            "ec2:ModifyFleet",
                                            "ec2:DeleteFleets",
                                            "ec2:DescribeFleetInstances",
                                            "ec2:DescribeFleets",
                                            "ec2:CreateFleet",
                                            "ec2:DeleteLaunchTemplate",
                                            "ec2:GetLaunchTemplateData",
                                            "ec2:CreateLaunchTemplate",
                                            "ec2:DescribeLaunchTemplates",
                                            "ec2:DescribeLaunchTemplateVersions",
                                            "ec2:ModifyLaunchTemplate",
                                            "ec2:DeleteLaunchTemplateVersions",
                                            "ec2:CreateLaunchTemplateVersion",
                                            "ec2:AssignPrivateIpAddresses",
                                            "ec2:GetSpotPlacementScores"
                                        ],
                                        "Resource": "*"
                                    },
                                    {
                                        "Sid": "CreateServiceLinkedRole",
                                        "Effect": "Allow",
                                        "Action": [
                                            "iam:CreateServiceLinkedRole",
                                            "iam:PutRolePolicy"
                                        ],
                                        "Resource": "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot",
                                        "Condition": {"StringLike": {"iam:AWSServiceName": "spot.amazonaws.com"}}
                                    },
                                    {
                                        "Sid": "AllowPassRoleForInstanceProfile",
                                        "Effect": "Allow",
                                        "Action": "iam:PassRole",
                                        "Resource": "*"
                                    }
                                ]
                            }
                        }
                    ]
                }
            })
        )
        self.__addTagsToResource("WorkspaceIamRole")
        self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
            key="WorkspaceIamRole",
            before='\n----- Credentials for Databricks\n\nThe workspace cross-account IAM role', indent=2)
        self.__requiredPrivileges.add("iam:CreateRole")
        self.__requiredPrivileges.add("iam:GetRole")
        self.__requiredPrivileges.add("iam:TagRole")
        self.__requiredPrivileges.add("iam:PutRolePolicy")
        self.__requiredPrivileges.add("iam:GetRolePolicy")
        self.__requiredPrivilegesForRollback.add("iam:DeleteRole")
        self.__requiredPrivilegesForRollback.add("iam:DeleteRolePolicy")

        # The output
        self.__cloudFormationTemplate["Outputs"].insert(
            pos= len(self.__cloudFormationTemplate["Outputs"]),
            key= 'WorkspaceIAMRole',
            value= {
                "Description": "The ARN of the cross account IAM role for the Databricks workspace",
                "Value": {"Fn::GetAtt": "WorkspaceIamRole.Arn"}
            }
        )
        self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
            key ='WorkspaceIamRole',
            before= 'The cross-account IAM role for the workspace', indent=2)


    # Defines the CMK Resources
    def __defineCustomerManagerKeyResources(self):
        cmkUsage = self.__customerManagedKeysOptions.usage()
        if cmkUsage != CustomerManagedKeysOptions.Usage.NONE:
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="EncryptionKey",
                value=CommentedMap({
                    "Type": "AWS::KMS::Key",
                    "Properties": {
                        "BypassPolicyLockoutSafetyCheck": True,
                        "Enabled": True,
                        "KeyPolicy": {
                            "Statement": [
                                {
                                    "Sid": "Enable Owner Account Permissions",
                                    "Effect": "Allow",
                                    "Principal": {
                                        "AWS": {"Fn::Sub": "arn:aws:iam::${AWS::AccountId}:root"}
                                    },
                                    "Action": "kms:*",
                                    "Resource": "*"
                                },
                            ]
                        },
                        "Tags": [{"Key": "Name", "Value": {"Fn::Sub": "${AWS::StackName}-EncryptionKey"}}],
                    }
                })
            )
            self.__addTagsToResource("EncryptionKey")
            description = "KMS Customer Manager Key used by Databricks to encrypt/decrypt the data on "
            if cmkUsage == CustomerManagedKeysOptions.Usage.MANAGED_SERVICES:
                description += "the control plane"
                self.__cloudFormationTemplate["Resources"]["EncryptionKey"]["Properties"]["KeyPolicy"]["Statement"] += managedServicesPolicyStatement("DatabricksAccountId")
            elif cmkUsage == CustomerManagedKeysOptions.Usage.STORAGE:
                description += "the data plane"
                self.__cloudFormationTemplate["Resources"]["EncryptionKey"]["Properties"]["KeyPolicy"]["Statement"] += workspaceStoragePolicyStatement("DatabricksAccountId", "WorkspaceIamRole")
            elif cmkUsage == CustomerManagedKeysOptions.Usage.BOTH:
                description += "both the control and data plane"
                self.__cloudFormationTemplate["Resources"]["EncryptionKey"]["Properties"]["KeyPolicy"]["Statement"] += managedServicesPolicyStatement("DatabricksAccountId") + workspaceStoragePolicyStatement("DatabricksAccountId", "WorkspaceIamRole")
            else:
                raise Exception("Invalid CMK usage: " + str(cmkUsage))
            self.__cloudFormationTemplate["Resources"]["EncryptionKey"]["Properties"]["Description"] = description
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="EncryptionKey",
                before='\n----- Customer Managed keys for Databricks\n\n The KMS key', indent=2)
            self.__requiredPrivileges.add("kms:CreateKey")
            self.__requiredPrivileges.add("kms:DescribeKey")
            self.__requiredPrivileges.add("kms:EnableKey")
            self.__requiredPrivileges.add("kms:PutKeyPolicy")
            self.__requiredPrivileges.add("kms:TagResource")
            self.__requiredPrivileges.add("kms:ListResourceTags")
            self.__requiredPrivilegesForRollback.add("kms:DisableKey")
            self.__requiredPrivilegesForRollback.add("kms:ScheduleKeyDeletion")
            self.__requiredPrivilegesForRollback.add("kms:UntagResource")
            # The output
            self.__cloudFormationTemplate["Outputs"].insert(
                pos= len(self.__cloudFormationTemplate["Outputs"]),
                key= 'EncryptionKeyArn',
                value= {
                    "Description": "The ARN of the " + description,
                    "Value": {"Fn::GetAtt": "EncryptionKey.Arn"}
                }
            )
            self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
                key ='EncryptionKeyArn',
                before= 'The ARN of the Encryption key', indent=2)

            # Also create the alias
            aliasName = {"Fn::Sub": "alias/${AWS::StackName}"} if self.__customerManagedKeysOptions.keyAlias() is None else "alias/" + self.__customerManagedKeysOptions.keyAlias()
            self.__cloudFormationTemplate['Resources'].insert(
                pos=len(self.__cloudFormationTemplate['Resources']),
                key="EncryptionKeyAlias",
                value=CommentedMap({
                    "Type": "AWS::KMS::Alias",
                    "Properties": {
                        "AliasName": aliasName,
                        "TargetKeyId": {"Ref": "EncryptionKey"}
                    }
                })
            )
            self.__cloudFormationTemplate['Resources'].yaml_set_comment_before_after_key(
                key="EncryptionKeyAlias",
                before=' The key alias', indent=2)
            self.__requiredPrivileges.add("kms:CreateAlias")
            self.__requiredPrivilegesForRollback.add("kms:DeleteAlias")
            # The output
            self.__cloudFormationTemplate["Outputs"].insert(
                pos= len(self.__cloudFormationTemplate["Outputs"]),
                key= 'EncryptionKeyAlias',
                value= {
                    "Description": "The alias of the " + description,
                    "Value": {"Ref": "EncryptionKeyAlias"}  # Need to update that so that the "alias/" prefix is removed
                }
            )
            self.__cloudFormationTemplate['Outputs'].yaml_set_comment_before_after_key(
                key ='EncryptionKeyAlias',
                before= 'The Alias of the Encryption key', indent=2)
