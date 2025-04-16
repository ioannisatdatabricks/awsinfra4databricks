import boto3
import json
import time
from botocore.exceptions import WaiterError, ClientError

class AWSSession:

    # Initialises the session with the credentials
    def __init__(self,
                 profileName:str = None,
                 accessKeyId:str = None,
                 secretAccessKey:str = None,
                 sessionToken:str = None):
        self.__session = None
        self.__callerArn = None
        latestError = None
        try:
            if profileName is not None:
                self.__session = boto3.Session(profile_name = profileName)
            elif (accessKeyId is not None) and (secretAccessKey is not None):
                if sessionToken is not None:
                    self.__session = boto3.Session(aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey, aws_session_token=sessionToken)
                else:
                    self.__session = boto3.Session(aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey)
            else:
                self.__session = boto3.Session()

            # Check for the validity of the credentials by trying to retrieve the caller ARN
            callerIdentity = self.__session.client('sts').get_caller_identity()
            self.__callerArn = callerIdentity["Arn"]
            if self.__callerArn.startswith('arn:aws:sts::'):
                accountId = callerIdentity['Account']
                roleName = self.__callerArn.split('/')[1]
                self.__callerArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
        except Exception as e:
            latestError = str(e)

        # Rethrow the exception
        if latestError is not None:
            raise Exception(latestError)
        
    # returns the actions that are not permitted by the current caller or the provided role ARN
    def checkPermissionsForActions(self, actionList: list, roleArn:str = None) -> tuple[list[str]]:
        disallowedActions = []
        allowedActions = []
        latestError = None
        policySourceArn = self.__callerArn if roleArn is None else roleArn
        try:
            response = self.__session.client('iam').simulate_principal_policy(
                PolicySourceArn = policySourceArn,
                ActionNames=actionList
            )
            if 'EvaluationResults' in response:
                for action in response['EvaluationResults']:
                    actionName = action['EvalActionName']
                    if action['EvalDecision'] != 'allowed':
                        disallowedActions.append(actionName)
                    else:
                        allowedActions.append(actionName)
        except Exception as e:
            latestError = str(e)
        # Rethrow the exception
        if latestError is not None:
            raise Exception(latestError)
        return (allowedActions, disallowedActions)


    # Retrieves the information of the availability zones in a region
    def getAvailabilityZoneIndexes(self, region: str):
        ec2 = self.__session.client('ec2', region_name=region)
        response = ec2.describe_availability_zones()
        regions = sorted([
            z['ZoneName'] for z in response['AvailabilityZones']
            if z['ZoneType'] == 'availability-zone' and z['ZoneName'].startswith(region) and z['State'] == 'available'
        ])
        return [ord((r[len(region):]).lower()) - ord('a') for r in regions]


    def __waitUntilIamRoleHasBeenDeleted(self, roleName:str) -> bool:
        iam = self.__session.client('iam')
        max_attempts=30
        delay=2
        for attempt in range(max_attempts):
            try:
                iam.get_role(RoleName=roleName)
            except iam.exceptions.NoSuchEntityException:
                return True
            time.sleep(delay)
        return False

    def __deleteIamRoleIfExists(self, roleName:str):
        iam = self.__session.client('iam')
        errorMessage = None
        try:
            iam.get_role(RoleName = roleName)
            # Detach all policies from the role
            attached_policies = iam.list_attached_role_policies(RoleName=roleName)
            for policy in attached_policies.get('AttachedPolicies', []):
                iam.detach_role_policy(RoleName=roleName, PolicyArn=policy['PolicyArn'])            
            # Delete inline policies
            inline_policies = iam.list_role_policies(RoleName=roleName)
            for policy_name in inline_policies.get('PolicyNames', []):
                iam.delete_role_policy(RoleName=roleName, PolicyName=policy_name)
            # Delete the role
            iam.delete_role(RoleName=roleName)
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                errorMessage = str(e)
        if errorMessage is not None: raise Exception(errorMessage)
        self.__waitUntilIamRoleHasBeenDeleted(roleName)

    def __waitForRoleCreation(self, roleName:str) -> bool:
        iam = self.__session.client('iam')
        max_attempts=30
        delay=2
        for attempt in range(max_attempts):
            try:
                iam.get_role(RoleName=roleName)
                return True
            except iam.exceptions.NoSuchEntityException:
                time.sleep(delay)
        return False

    # Creates or replaces an IAM role to be assumed by CloudFormation
    def createOrReplaceIamRoleForCloudFormation(self, roleName:str, inlinePolicy: dict) -> str:
        self.__deleteIamRoleIfExists(roleName)
        iam = self.__session.client('iam')
        # Define the trust policy
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "cloudformation.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        roleArn = None
        errorMessage = None
        try:
            # Create the IAM role
            roleArn = iam.create_role(
                RoleName=roleName,
                AssumeRolePolicyDocument=json.dumps(trust_policy)
            )['Role']['Arn']
            # Attach the inline policy to the role
            iam.put_role_policy(
                RoleName=roleName,
                PolicyName='PrivilegesToCloudFormation',
                PolicyDocument=json.dumps(inlinePolicy)
            )
        except ClientError as e:
            errorMessage = str(e)

        if errorMessage is not None: raise Exception(errorMessage)
        self.__waitForRoleCreation(roleName)
        return roleArn


    # Creates a cloudformation stack
    def createCloudFormationStack(
            self,
            stackName: str,
            templateBody: str,
            region: str,
            bucketForCFTemplate: str = None,
            roleArn: str = None) -> bool:
        cf_client = self.__session.client('cloudformation', region_name=region)
        try:
            kwargs = {
                "StackName": stackName,
                "Capabilities":['CAPABILITY_NAMED_IAM']
            }
            if roleArn is not None: kwargs["RoleARN"] = roleArn
            if bucketForCFTemplate is not None:
                s3 = self.__session.client('s3', region_name=region)
                s3.put_object(Body=templateBody.encode(), Bucket=bucketForCFTemplate, Key=stackName+'.yaml')
                time.sleep(1)
                templateURL = f"https://{bucketForCFTemplate}.s3.amazonaws.com/{stackName}.yaml"
                kwargs['TemplateURL'] = templateURL
            else:
                kwargs["TemplateBody"] = templateBody
            
            cf_client.create_stack(**kwargs)
            
            # Get the waiter for stack creation complete
            print(f"Waiting for stack {stackName} to be created...")
            createWaiter = cf_client.get_waiter('stack_create_complete')            
            createWaiter.wait(StackName=stackName, WaiterConfig={'Delay': 10, 'MaxAttempts': 100})
            print(f"Stack {stackName} created successfully!")
            return True
        except WaiterError as e:
            print("Error: Stack creation failed or rolled back.")
            try:
                rollbackWaiter = cf_client.get_waiter('stack_rollback_complete')            
                rollbackWaiter.wait(StackName=stackName, WaiterConfig={'Delay': 10, 'MaxAttempts': 100})
            except WaiterError as er: print("Rollback error: " + str(er))
            try:
                print("deleting the stack")
                cf_client.delete_stack(StackName=stackName, DeletionMode="FORCE_DELETE_STACK")
                deleteWaiter = cf_client.get_waiter('stack_delete_complete')            
                deleteWaiter.wait(StackName=stackName, WaiterConfig={'Delay': 10, 'MaxAttempts': 100})
            except ClientError as ed:
                print(f"Error: {ed}")
            return False
        except ClientError as e:
            print(f"Error: {e}")
            return False
