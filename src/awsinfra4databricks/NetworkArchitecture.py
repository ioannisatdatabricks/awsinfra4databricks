from enum import Enum
import math
import ipaddress

# Design options for the network architecture
class NetworkArchitectureDesignOptions:
    class InternetAccess(Enum):
        STANDARD = 1
        HIGH_AVAILABILITY = 2
        DISABLED = 3

    class PrivateLinkEndpoints(Enum):
        ENABLED = 1
        DISABLED = 2

    class VPCArchitectureMode(Enum):
        SINGLE_VPC = 1
        HUB_AND_SPOKE = 2

    class DataExfiltrationProtection(Enum):
        ACTIVATED = 1
        DEACTIVATED = 2

    def __init__(self,
                 internetAccess: InternetAccess = InternetAccess.STANDARD,
                 privateLinkEndpoints: PrivateLinkEndpoints = PrivateLinkEndpoints.DISABLED,
                 vpcArchitecture: VPCArchitectureMode = VPCArchitectureMode.SINGLE_VPC,
                 dataExfiltrationProtection: DataExfiltrationProtection = DataExfiltrationProtection.DEACTIVATED):
        self.__internetAccess = internetAccess
        self.__privateLinkEndpoints = privateLinkEndpoints
        self.__vpcArchitecture = vpcArchitecture
        self.__dataExfiltrationProtection = dataExfiltrationProtection
    
    def internetAccess(self) -> InternetAccess:
        return self.__internetAccess
    
    def privateLinkEndpoints(self) -> PrivateLinkEndpoints:
        return self.__privateLinkEndpoints
    
    def vpcArchitecture(self) -> VPCArchitectureMode:
        return self.__vpcArchitecture
    
    def dataExfiltrationProtection(self) -> DataExfiltrationProtection:
        return self.__dataExfiltrationProtection


# Parameters for the architecture of the network
class NetworkArchitectureParameters:

    def __init__(self,
                 vpcCidrStartingAddress: str = '10.0.0.0',
                 maxRunningNodesPerSubnet: int = 512,
                 availabilityZoneIndexes: tuple[int] = (0,1),
                 maxVpcEndpointsPerSubnet: int = 10,
                 hubVpcStartingAddress: str = None):
        self.__vpcCidrStartingAddress = vpcCidrStartingAddress
        self.__maxRunningNodesPerSubnet = maxRunningNodesPerSubnet
        self.__availabilityZoneIndexes = availabilityZoneIndexes
        self.__maxVpcEndpointsPerSubnet = maxVpcEndpointsPerSubnet
        self.__hubVpcStartingAddress = hubVpcStartingAddress
        if len(availabilityZoneIndexes) < 2:
            raise Exception("There should be at least 2 availability zones specified")
        if len(availabilityZoneIndexes) != len(set(availabilityZoneIndexes)):
            raise Exception("There should be no duplicates in the availability zones specified")

    def vpcCidrStartingAddress(self) -> str:
        return self.__vpcCidrStartingAddress
    
    def maxRunningNodesPerSubnet(self) -> int:
        return self.__maxRunningNodesPerSubnet
    
    def availabilityZoneIndexes(self) -> tuple[int]:
        return self.__availabilityZoneIndexes
    
    def maxVpcEndpointsPerSubnet(self) -> int:
        return self.__maxVpcEndpointsPerSubnet
    
    def hubVpcStartingAddress(self) -> str:
        return self.__hubVpcStartingAddress


# The configuration CIDR ranges of the various subnets
class VpcAndSubnetCIDR:
    class SubnetType(Enum):
        CLUSTERS = 0
        VPCENDPOINTS = 1
        NETWORKFIREWALL = 2
        NATGATEWAY = 3
        TRANSITGATEWAY = 4

    class VpcType(Enum):
        DATABRICKS_VPC = 0
        HUB_VPC = 1
    
    def __init__(self,
                 vpcCIDR: str,
                 vpcType: VpcType = VpcType.DATABRICKS_VPC,
                 subnetsForClusters: list[str] = None,
                 subnetsForVpcEndpoints: list[str] = None,
                 subnetsForNetworkFirewall: list[str] = None,
                 subnetsForNatGateway: list[str] = None,
                 subnetsForTransitGateway: list[str] = None):
        self.__vpcCIDR = vpcCIDR
        self.__vpcType = vpcType
        self.__config = {}
        if subnetsForClusters is not None:
            self.__config[VpcAndSubnetCIDR.SubnetType.CLUSTERS] = subnetsForClusters
        if subnetsForVpcEndpoints is not None:
            self.__config[VpcAndSubnetCIDR.SubnetType.VPCENDPOINTS] = subnetsForVpcEndpoints
        if subnetsForNetworkFirewall is not None:
            self.__config[VpcAndSubnetCIDR.SubnetType.NETWORKFIREWALL] = subnetsForNetworkFirewall
        if subnetsForNatGateway is not None:
            self.__config[VpcAndSubnetCIDR.SubnetType.NATGATEWAY] = subnetsForNatGateway
        if subnetsForTransitGateway is not None:
            self.__config[VpcAndSubnetCIDR.SubnetType.TRANSITGATEWAY] = subnetsForTransitGateway

    def vpcCIDR(self) -> str:
        return self.__vpcCIDR
    
    def vpcType(self) -> VpcType:
        return self.__vpcType

    def subnetCIDRs(self) -> dict[SubnetType, list[str]]:
        return self.__config


# It calculates the subnets
class SubnetConfigurationBuilder:

    def __subnetBitLength(num_ips:int) -> int:
        return int(math.log2(1 << (num_ips + 5).bit_length()))


    def __init__(self,
                 networkArchitectureDesignOptions: NetworkArchitectureDesignOptions,
                 networkArchitectureParameters: NetworkArchitectureParameters):
        
        numberOfNodesPerSubnet = networkArchitectureParameters.maxRunningNodesPerSubnet()
        n_availability_zones = len(networkArchitectureParameters.availabilityZoneIndexes())
        startingVpcIpAddress = networkArchitectureParameters.vpcCidrStartingAddress()

        if numberOfNodesPerSubnet < 10: raise Exception("Very small number of nodes per subnet specified")
        vm_ips = 2*numberOfNodesPerSubnet
        endpoint_ips = networkArchitectureParameters.maxVpcEndpointsPerSubnet()
        if endpoint_ips < 10: endpoint_ips = 10
        firewall_ips = 10
        nat_gateway_ips = 10
        transit_gateway_ips = 10

        vm_subnet_size = SubnetConfigurationBuilder.__subnetBitLength(vm_ips)
        endpoint_subnet_size = SubnetConfigurationBuilder.__subnetBitLength(endpoint_ips)
        firewall_subnet_size =SubnetConfigurationBuilder. __subnetBitLength(firewall_ips)
        nat_gateway_subnet_size = SubnetConfigurationBuilder.__subnetBitLength(nat_gateway_ips)
        transit_gateway_subnet_size = SubnetConfigurationBuilder.__subnetBitLength(transit_gateway_ips)

        # The case of a single vpc
        if networkArchitectureDesignOptions.vpcArchitecture() == NetworkArchitectureDesignOptions.VPCArchitectureMode.SINGLE_VPC:
 
            # The clusters (VM) subnet
            totalAddresses = n_availability_zones * math.pow(2,vm_subnet_size)

            # VPC endpoint subnet
            if networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED:
                totalAddresses += n_availability_zones * math.pow(2,endpoint_subnet_size)

            # Standard internet access
            if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD:
                # NAT Gateway subnet
                totalAddresses += math.pow(2,nat_gateway_subnet_size)            
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Network firewall subnet
                    totalAddresses += math.pow(2,firewall_subnet_size)
            # High availability internet access
            elif networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.HIGH_AVAILABILITY:
                # NAT Gateway subnets
                totalAddresses += n_availability_zones * math.pow(2,nat_gateway_subnet_size)
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Network firewall subnets
                    totalAddresses += n_availability_zones * math.pow(2,firewall_subnet_size)
            else: # No Internet
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Raise an exception in case a Network Firewall is requested
                    raise Exception("Data Exfiltration Protection requested without Internet Access")

            # Define the VPC size and network
            vpc_size = (int(totalAddresses)).bit_length()        
            vpc_network = ipaddress.ip_network(startingVpcIpAddress + "/" + str(32-vpc_size))
            startingIP = vpc_network[0]

            # VMs
            subnetsForClusters = []
            for _ in range(n_availability_zones):
                subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-vm_subnet_size))
                subnetsForClusters.append(str(subnet))
                startingIP = subnet[-1] + 1

            # EPs
            subnetsForVpcEndpoints = None
            if networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED:
                subnetsForVpcEndpoints = []
                for _ in range(n_availability_zones):
                    subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-endpoint_subnet_size))
                    subnetsForVpcEndpoints.append(str(subnet))
                    startingIP = subnet[-1] + 1

            subnetsForNetworkFirewall = None
            subnetsForNatGateway = None
            if networkArchitectureDesignOptions.internetAccess() != NetworkArchitectureDesignOptions.InternetAccess.DISABLED:

                # Network Firecall subnets
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    subnetsForNetworkFirewall = []
                    for _ in range(n_availability_zones):
                        subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-firewall_subnet_size))
                        subnetsForNetworkFirewall.append(str(subnet))
                        startingIP = subnet[-1] + 1
                        if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD: break

                # NAT Gateway Firecall subnets
                subnetsForNatGateway = []
                for _ in range(n_availability_zones):
                    subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-nat_gateway_subnet_size))
                    subnetsForNatGateway.append(str(subnet))
                    startingIP = subnet[-1] + 1
                    if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD: break

            # Assemble everything together
            self.__vpcConfig = {
                VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC: VpcAndSubnetCIDR(
                    str(vpc_network),
                    VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC,
                    subnetsForClusters=subnetsForClusters,
                    subnetsForVpcEndpoints=subnetsForVpcEndpoints,
                    subnetsForNetworkFirewall=subnetsForNetworkFirewall,
                    subnetsForNatGateway=subnetsForNatGateway
                )
            }

        # The case of hub and spoke architecture
        else:

            # First we build the Databricks VPC
            # The clusters (VM) subnet
            totalAddresses = n_availability_zones * (math.pow(2,vm_subnet_size) + math.pow(2, transit_gateway_subnet_size))

            vpc_size = (int(totalAddresses)).bit_length()        
            vpc_network = ipaddress.ip_network(startingVpcIpAddress + "/" + str(32-vpc_size))
            startingIP = vpc_network[0]

            # VMs
            subnetsForClusters = []
            for _ in range(n_availability_zones):
                subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-vm_subnet_size))
                subnetsForClusters.append(str(subnet))
                startingIP = subnet[-1] + 1

            # Transit Gateway segment subnets
            subnetsForTransitGateway = []
            for _ in range(n_availability_zones):
                subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-transit_gateway_subnet_size))
                subnetsForTransitGateway.append(str(subnet))
                startingIP = subnet[-1] + 1

            self.__vpcConfig = {
                VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC: VpcAndSubnetCIDR(
                    str(vpc_network),
                    VpcAndSubnetCIDR.VpcType.DATABRICKS_VPC,
                    subnetsForClusters=subnetsForClusters,
                    subnetsForTransitGateway=subnetsForTransitGateway
                )
            }

            # Next we build the HubVPC
            hubVpcStartingAddress = networkArchitectureParameters.hubVpcStartingAddress()
            if hubVpcStartingAddress is None:
                raise Exception("Hub and Spoke architecture requested without specifying the Hub VPC starting address")
            
            # Transit Gateway segment subnets
            totalAddresses = n_availability_zones * math.pow(2, transit_gateway_subnet_size)

            # VPC endpoint subnet
            if networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED:
                totalAddresses += n_availability_zones * math.pow(2,endpoint_subnet_size)
            else: # Check if no internet access has been enabled
                if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.DISABLED:
                    raise Exception("Hub and spoke architecture defined with no internet access and no VPC endpoints. No route to the control plane can be defined!")

            # Standard internet access
            if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD:
                # NAT Gateway subnet
                totalAddresses += math.pow(2,nat_gateway_subnet_size)            
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Network firewall subnet
                    totalAddresses += math.pow(2,firewall_subnet_size)
            # High availability internet access
            elif networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.HIGH_AVAILABILITY:
                # NAT Gateway subnets
                totalAddresses += n_availability_zones * math.pow(2,nat_gateway_subnet_size)
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Network firewall subnets
                    totalAddresses += n_availability_zones * math.pow(2,firewall_subnet_size)
            else: # No Internet
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    # Raise an exception in case a Network Firewall is requested
                    raise Exception("Data Exfiltration Protection requested without Internet Access")

            # Define the VPC size and network
            vpc_size = (int(totalAddresses)).bit_length()        
            vpc_network = ipaddress.ip_network(hubVpcStartingAddress + "/" + str(32-vpc_size))
            startingIP = vpc_network[0]

            # EPs
            subnetsForVpcEndpoints = None
            if networkArchitectureDesignOptions.privateLinkEndpoints() == NetworkArchitectureDesignOptions.PrivateLinkEndpoints.ENABLED:
                subnetsForVpcEndpoints = []
                for _ in range(n_availability_zones):
                    subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-endpoint_subnet_size))
                    subnetsForVpcEndpoints.append(str(subnet))
                    startingIP = subnet[-1] + 1

            # Transit Gateway segment subnets
            subnetsForTransitGateway = []
            for _ in range(n_availability_zones):
                subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-transit_gateway_subnet_size))
                subnetsForTransitGateway.append(str(subnet))
                startingIP = subnet[-1] + 1

            subnetsForNetworkFirewall = None
            subnetsForNatGateway = None
            if networkArchitectureDesignOptions.internetAccess() != NetworkArchitectureDesignOptions.InternetAccess.DISABLED:

                # Network Firecall subnets
                if networkArchitectureDesignOptions.dataExfiltrationProtection() == NetworkArchitectureDesignOptions.DataExfiltrationProtection.ACTIVATED:
                    subnetsForNetworkFirewall = []
                    for _ in range(n_availability_zones):
                        subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-firewall_subnet_size))
                        subnetsForNetworkFirewall.append(str(subnet))
                        startingIP = subnet[-1] + 1
                        if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD: break

                # NAT Gateway Firecall subnets
                subnetsForNatGateway = []
                for _ in range(n_availability_zones):
                    subnet = ipaddress.ip_network(str(startingIP) + "/" + str(32-nat_gateway_subnet_size))
                    subnetsForNatGateway.append(str(subnet))
                    startingIP = subnet[-1] + 1
                    if networkArchitectureDesignOptions.internetAccess() == NetworkArchitectureDesignOptions.InternetAccess.STANDARD: break

            # Assemble everything together
            self.__vpcConfig[VpcAndSubnetCIDR.VpcType.HUB_VPC] = VpcAndSubnetCIDR(
                str(vpc_network),
                VpcAndSubnetCIDR.VpcType.HUB_VPC,
                subnetsForVpcEndpoints=subnetsForVpcEndpoints,
                subnetsForNetworkFirewall=subnetsForNetworkFirewall,
                subnetsForNatGateway=subnetsForNatGateway,
                subnetsForTransitGateway=subnetsForTransitGateway
            )

    def vpcConfig(self) -> dict[VpcAndSubnetCIDR.VpcType:VpcAndSubnetCIDR]:
        return self.__vpcConfig
