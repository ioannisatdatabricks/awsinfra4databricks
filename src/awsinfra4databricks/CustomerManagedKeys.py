from enum import Enum

# Generates the fragment for CloudFormation for the managed services
def managedServicesPolicyStatement(databricksIdRefName: str) -> list[dict]:
    return [
        {
            "Sid": "Allow Databricks to use KMS key for managed services in the control plane",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::414351767826:root"
            },
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalTag/DatabricksAccountId": [{"Ref": databricksIdRefName}]
                }
            }
        }
    ]


def workspaceStoragePolicyStatement(databricksIdRefName: str, iamRoleResourceName: str) -> list[dict]:
    return [
        {
            "Sid": "Allow Databricks to use KMS key for DBFS",
            "Effect": "Allow",
            "Principal":{
                "AWS":"arn:aws:iam::414351767826:root"
            },
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:PrincipalTag/DatabricksAccountId": [{"Ref": databricksIdRefName}]
                }
            }
        },
        {
            "Sid": "Allow Databricks to use KMS key for DBFS (Grants)",
            "Effect": "Allow",
            "Principal":{
                "AWS":"arn:aws:iam::414351767826:root"
            },
            "Action": [
                "kms:CreateGrant",
                "kms:ListGrants",
                "kms:RevokeGrant"
            ],
            "Resource": "*",
            "Condition": {
                "Bool": {
                    "kms:GrantIsForAWSResource": "true"
                },
                "StringEquals": {
                    "aws:PrincipalTag/DatabricksAccountId": [{"Ref": databricksIdRefName}]
                }
            }
        },
        {
            "Sid": "Allow Databricks to use KMS key for EBS",
            "Effect": "Allow",
            "Principal": {
                "AWS": {"Fn::GetAtt": iamRoleResourceName + ".Arn"}
            },
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey*",
                "kms:CreateGrant",
                "kms:DescribeKey"
            ],
            "Resource": "*",
            "Condition": {
                "ForAnyValue:StringLike": {
                    "kms:ViaService": "ec2.*.amazonaws.com"
                }
            }
        }
    ]

class CustomerManagedKeysOptions:
    class Usage(Enum):
        NONE = 0
        MANAGED_SERVICES = 1
        STORAGE = 2
        BOTH = 3

    def __init__(self, usage: Usage = Usage.NONE, keyAlias:str = None):
        self.__usage = usage
        self.__keyAlias = keyAlias
    
    def usage(self) -> Usage:
        return self.__usage
    
    def keyAlias(self) -> str:
        return self.__keyAlias
