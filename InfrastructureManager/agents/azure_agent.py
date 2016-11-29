#!/usr/bin/env python

# General-purpose Python library imports
import adal
import threading
import time

# Azure specific imports
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import CachingTypes
from azure.mgmt.compute.models import DiskCreateOptionTypes
from azure.mgmt.compute.models import HardwareProfile
from azure.mgmt.compute.models import LinuxConfiguration
from azure.mgmt.compute.models import NetworkProfile
from azure.mgmt.compute.models import NetworkInterfaceReference
from azure.mgmt.compute.models import OperatingSystemTypes
from azure.mgmt.compute.models import OSDisk
from azure.mgmt.compute.models import OSProfile
from azure.mgmt.compute.models import SshConfiguration
from azure.mgmt.compute.models import SshPublicKey
from azure.mgmt.compute.models import StorageProfile
from azure.mgmt.compute.models import VirtualHardDisk
from azure.mgmt.compute.models import VirtualMachine

from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import IPAllocationMethod
from azure.mgmt.network.models import NetworkInterfaceIPConfiguration
from azure.mgmt.network.models import NetworkInterface
from azure.mgmt.network.models import PublicIPAddress

from haikunator import Haikunator

# AppScale-specific imports
from agents.base_agent import AgentConfigurationException
from agents.base_agent import BaseAgent
from utils import utils

class AzureAgent(BaseAgent):
  """ AzureAgent defines a specialized BaseAgent that allows for interaction
  with Microsoft Azure. It authenticates using the ADAL (Active Directory
  Authentication Library).
  """
  # The Azure URL endpoint that receives all the authentication requests.
  AZURE_AUTH_ENDPOINT = 'https://login.microsoftonline.com/'

  # The Azure Resource URL to get the authentication token using client credentials.
  AZURE_RESOURCE_URL = 'https://management.core.windows.net/'

  # The default Storage Account name to use for Azure.
  DEFAULT_STORAGE_ACCT = 'appscalestorage'

  # The default resource group name to use for Azure.
  DEFAULT_RESOURCE_GROUP = 'appscalegroup'

  # The following constants are string literals that can be used by callers to
  # index into the parameters that the user passes in, as opposed to having to
  # type out the strings each time we need them.
  PARAM_APP_ID = 'azure_app_id'
  PARAM_APP_SECRET = 'azure_app_secret_key'
  PARAM_CREDENTIALS = 'credentials'
  PARAM_EXISTING_RG = 'does_exist'
  PARAM_GROUP = 'group'
  PARAM_INSTANCE_IDS = 'instance_ids'
  PARAM_INSTANCE_TYPE = 'instance_type'
  PARAM_KEYNAME = 'keyname'
  PARAM_IMAGE_ID = 'image_id'
  PARAM_REGION = 'region'
  PARAM_RESOURCE_GROUP = 'azure_resource_group'
  PARAM_STORAGE_ACCOUNT = 'azure_storage_account'
  PARAM_SUBSCRIBER_ID = 'azure_subscription_id'
  PARAM_TENANT_ID = 'azure_tenant_id'
  PARAM_TEST = 'test'
  PARAM_TAG = 'azure_group_tag'
  PARAM_VERBOSE = 'is_verbose'
  PARAM_ZONE = 'zone'

  # A set that contains all of the items necessary to run AppScale in Azure.
  REQUIRED_CREDENTIALS = (
    PARAM_APP_SECRET,
    PARAM_APP_ID,
    PARAM_IMAGE_ID,
    PARAM_INSTANCE_TYPE,
    PARAM_KEYNAME,
    PARAM_SUBSCRIBER_ID,
    PARAM_TENANT_ID,
    PARAM_ZONE
  )

  # The admin username needed to create an Azure VM instance.
  ADMIN_USERNAME = 'azureuser'

  # The file path for the authorized keys on the head node
  # for an Azure VM.
  AUTHORIZED_KEYS_FILE = "/home/{}/.ssh/authorized_keys"

  # The number of seconds to sleep while polling for
  # Azure resources to get created/updated.
  SLEEP_TIME = 10

  # The maximum number of seconds to wait for Azure resources
  # to get created/updated.
  MAX_SLEEP_TIME = 60

  # The maximum number of seconds to wait for an Azure VM to be created.
  # (Takes longer than the creation time for other resources.)
  MAX_VM_CREATION_TIME = 240

  # The Virtual Network and Subnet name to use while creating an Azure
  # Virtual machine.
  VIRTUAL_NETWORK = 'appscaleazure'

  def configure_instance_security(self, parameters):
    """ Configure and setup groups and storage accounts for the VMs spawned.
    This method is called before starting virtual machines.
    Args:
      parameters: A dict containing values necessary to authenticate with the
        underlying cloud.
    Returns:
      True, if the group and account were created successfully.
      False, otherwise.
    """
    return True

  def describe_instances(self, parameters, pending=False):
    """ Queries Microsoft Azure to see which instances are currently
    running, and retrieves information about their public and private IPs.
    Args:
      parameters: A dict containing values necessary to authenticate with the
        underlying cloud.
      pending: If we should show pending instances.
    Returns:
      public_ips: A list of public IP addresses.
      private_ips: A list of private IP addresses.
      instance_ids: A list of unique Azure VM names.
    """
    credentials = self.open_connection(parameters)
    subscription_id = str(parameters[self.PARAM_SUBSCRIBER_ID])
    resource_group = parameters[self.PARAM_RESOURCE_GROUP]
    network_client = NetworkManagementClient(credentials, subscription_id)
    compute_client = ComputeManagementClient(credentials, subscription_id)
    public_ips = []
    private_ips = []
    instance_ids = []

    public_ip_addresses = network_client.public_ip_addresses.list(resource_group)
    for public_ip in public_ip_addresses:
      public_ips.append(public_ip.ip_address)

    network_interfaces = network_client.network_interfaces.list(resource_group)
    for network_interface in network_interfaces:
      for ip_config in network_interface.ip_configurations:
        private_ips.append(ip_config.private_ip_address)

    virtual_machines = compute_client.virtual_machines.list(resource_group)
    for vm in virtual_machines:
      instance_ids.append(vm.name)
    return public_ips, private_ips, instance_ids

  def run_instances(self, count, parameters, security_configured):
    """ Starts 'count' instances in Microsoft Azure, and returns once they
    have been started. Callers should create a network and attach a firewall
    to it before using this method, or the newly created instances will not
    have a network and firewall to attach to (and thus this method will fail).
    Args:
      count: An int, that specifies how many virtual machines should be started.
      parameters: A dict, containing all the parameters necessary to
        authenticate this user with Azure.
      security_configured: Unused, as we assume that the network and firewall
        has already been set up.
    Returns:
      instance_ids: A list of unique Azure VM names.
      public_ips: A list of public IP addresses.
      private_ips: A list of private IP addresses.
    """
    credentials = self.open_connection(parameters)
    subscription_id = str(parameters[self.PARAM_SUBSCRIBER_ID])
    group_name = parameters[self.PARAM_RESOURCE_GROUP]
    network_client = NetworkManagementClient(credentials, subscription_id)
    active_public_ips, active_private_ips, active_instances = \
      self.describe_instances(parameters)
    virtual_network = parameters[self.PARAM_GROUP]
    subnet = network_client.subnets.get(group_name, virtual_network,
                                        virtual_network)
    threads = []
    for _ in range(count):
      vm_network_name = Haikunator().haikunate()
      thread = threading.Thread(target=self.setup_network_and_create_vm,
                                args=(network_client, parameters, subnet,
                                      vm_network_name))
      thread.start()
      threads.append(thread)

    for x in threads:
      x.join()

    public_ips, private_ips, instance_ids = self.describe_instances(parameters)
    public_ips = utils.diff(public_ips, active_public_ips)
    private_ips = utils.diff(private_ips, active_private_ips)
    instance_ids = utils.diff(instance_ids, active_instances)
    return instance_ids, public_ips, private_ips

  def setup_network_and_create_vm(self, network_client, parameters, subnet,
                                  vm_network_name):
    """ Sets up the network interface and creates a virtual machine using that
      interface.
      Args:
        network_client: A NetworkManagementClient instance.
        parameters: A dict, containing all the parameters necessary to
          authenticate this user with Azure.
        subnet: The Subnet resource from the Virtual Network created.
        vm_network_name: The name of the Network to use for the Virtual machine.
    """
    credentials = self.open_connection(parameters)
    resource_group = parameters[self.PARAM_RESOURCE_GROUP]
    self.create_network_interface(network_client, vm_network_name,
                                  vm_network_name, subnet, parameters)
    network_interface = network_client.network_interfaces.get(resource_group,
                                                              vm_network_name)
    self.create_virtual_machine(credentials, network_client, network_interface.id,
                                parameters, vm_network_name)

  def create_virtual_machine(self, credentials, network_client, network_id,
                             parameters, vm_network_name):
    """ Creates an Azure virtual machine using the network interface created.
    Args:
      credentials: A ServicePrincipalCredentials instance, that can be used to
        access or create any resources.
      network_client: A NetworkManagementClient instance.
      network_id: The network id of the network interface created.
      parameters: A dict, containing all the parameters necessary to
        authenticate this user with Azure.
      vm_network_name: The name of the virtual machine to use.
    """
    resource_group = parameters[self.PARAM_RESOURCE_GROUP]
    storage_account = parameters[self.PARAM_STORAGE_ACCOUNT]
    zone = parameters[self.PARAM_ZONE]
    utils.log("Creating a Virtual Machine '{}'".format(vm_network_name))
    subscription_id = str(parameters[self.PARAM_SUBSCRIBER_ID])
    azure_instance_type = parameters[self.PARAM_INSTANCE_TYPE]
    compute_client = ComputeManagementClient(credentials, subscription_id)
    auth_keys_path = self.AUTHORIZED_KEYS_FILE.format(self.ADMIN_USERNAME)

    with open(auth_keys_path, 'r') as pub_ssh_key_fd:
      pub_ssh_key = pub_ssh_key_fd.read()

    public_keys = [SshPublicKey(path=auth_keys_path, key_data=pub_ssh_key)]
    ssh_config = SshConfiguration(public_keys=public_keys)
    linux_config = LinuxConfiguration(disable_password_authentication=True,
                                      ssh=ssh_config)
    os_profile = OSProfile(admin_username=self.ADMIN_USERNAME,
                           computer_name=vm_network_name,
                           linux_configuration=linux_config)

    hardware_profile = HardwareProfile(vm_size=azure_instance_type)

    network_profile = NetworkProfile(
      network_interfaces=[NetworkInterfaceReference(id=network_id)])

    virtual_hd = VirtualHardDisk(
      uri='https://{0}.blob.core.windows.net/vhds/{1}.vhd'.
        format(storage_account, vm_network_name))

    image_hd = VirtualHardDisk(uri=parameters[self.PARAM_IMAGE_ID])
    os_type = OperatingSystemTypes.linux
    os_disk = OSDisk(os_type=os_type, caching=CachingTypes.read_write,
                     create_option=DiskCreateOptionTypes.from_image,
                     name=vm_network_name, vhd=virtual_hd, image=image_hd)

    compute_client.virtual_machines.create_or_update(
      resource_group, vm_network_name, VirtualMachine(
        location=zone, os_profile=os_profile,
        hardware_profile=hardware_profile,
        network_profile=network_profile,
        storage_profile=StorageProfile(os_disk=os_disk)))

    # Sleep until an IP address gets associated with the VM.
    while True:
      public_ip_address = network_client.public_ip_addresses.get(resource_group,
                                                                 vm_network_name)
      if public_ip_address.ip_address:
        utils.log('Azure VM is available at {}'.
                  format(public_ip_address.ip_address))
        break
      utils.log("Waiting {} second(s) for IP address to be available".
                format(self.SLEEP_TIME))
      time.sleep(self.SLEEP_TIME)

  def associate_static_ip(self, instance_id, static_ip):
    """ Associates the given static IP address with the given instance ID.
    Args:
      instance_id: A str that names the instance that the static IP should be
        bound to.
      static_ip: A str naming the static IP to bind to the given instance.
    """

  def terminate_instances(self, parameters):
    """ Deletes the instances specified in 'parameters' running in Azure.
    Args:
      parameters: A dict, containing all the parameters necessary to
        authenticate this user with Azure.
    """
    credentials = self.open_connection(parameters)
    resource_group = parameters[self.PARAM_RESOURCE_GROUP]
    subscription_id = str(parameters[self.PARAM_SUBSCRIBER_ID])
    _, _, instance_ids = self.describe_instances(parameters)

    utils.log("Terminating the vm instance/s '{}'".format(instance_ids))
    compute_client = ComputeManagementClient(credentials, subscription_id)
    for vm_name in instance_ids:
      result = compute_client.virtual_machines.delete(resource_group, vm_name)
      resource_name = 'Virtual Machine' + ':' + vm_name
      self.sleep_until_delete_operation_done(result, resource_name,
                                             self.MAX_VM_CREATION_TIME)

  def sleep_until_delete_operation_done(self, result, resource_name, max_sleep):
    """ Sleeps until the delete operation for the resource is completed
    successfully.
    Args:
      result: An instance, of the AzureOperationPoller to poll for the status
        of the operation being performed.
      resource_name: The name of the resource being deleted.
      max_sleep: The maximum number of seconds to sleep for the resources to
        be deleted.
    """
    time_start = time.time()
    while not result.done():
      utils.log("Waiting {0} second(s) for '{1}' to be "
                "deleted.".format(self.SLEEP_TIME, resource_name))
      time.sleep(self.SLEEP_TIME)
      total_sleep_time = time.time() - time_start
      if total_sleep_time > max_sleep:
        utils.log("Waited {0} second(s) for '{1}' to be deleted. "
          "Operation has timed out.".format(total_sleep_time, resource_name))
        break

  def cleanup_state(self, parameters):
    """ Removes any remote state that was created to run AppScale instances
    during this deployment.
    Args:
      parameters: A dict that includes keys indicating the remote state
        that should be deleted.
    """
    subscription_id = str(parameters[self.PARAM_SUBSCRIBER_ID])
    resource_group = parameters[self.PARAM_RESOURCE_GROUP]
    credentials = self.open_connection(parameters)
    network_client = NetworkManagementClient(credentials, subscription_id)

    utils.log("Deleting the Virtual Network, Public IP Address "
              "and Network Interface created for this deployment.")
    network_interfaces = network_client.network_interfaces.list(resource_group)
    for interface in network_interfaces:
      result = network_client.network_interfaces.delete(resource_group,
                                                        interface.name)
      resource_name = 'Network Interface' + ':' + interface.name
      self.sleep_until_delete_operation_done(result, resource_name,
                                             self.MAX_SLEEP_TIME)

    public_ip_addresses = network_client.public_ip_addresses.list(resource_group)
    for public_ip in public_ip_addresses:
      result = network_client.public_ip_addresses.delete(resource_group,
                                                         public_ip.name)
      resource_name = 'Public IP Address' + ':' + public_ip.name
      self.sleep_until_delete_operation_done(result, resource_name,
                                             self.MAX_SLEEP_TIME)

    virtual_networks = network_client.virtual_networks.list(resource_group)
    for network in virtual_networks:
      result = network_client.virtual_networks.delete(resource_group,
                                                      network.name)
      resource_name = 'Virtual Network' + ':' + network.name
      self.sleep_until_delete_operation_done(result, resource_name,
                                             self.MAX_SLEEP_TIME)

  def assert_required_parameters(self, parameters, operation):
    """ Check whether all the parameters required to interact with Azure are
    present in the provided dict.
    Args:
      parameters: A dict containing values necessary to authenticate with the
        Azure.
      operation: A str representing the operation for which the parameters
        should be checked.
    Raises:
      AgentConfigurationException: If a required parameter is absent.
    """
    # Make sure that the user has set each parameter.
    for param in self.REQUIRED_CREDENTIALS:
      if param not in parameters:
        raise AgentConfigurationException('The required parameter, {0}, was not'
                                          ' specified.'.format(param))

  def open_connection(self, parameters):
    """ Connects to Microsoft Azure with the given credentials, creates an
    authentication token and uses that to get the ServicePrincipalCredentials
    which is needed to access any resources.
    Args:
      parameters: A dict, containing all the parameters necessary to authenticate
        this user with Azure. We assume that the user has already authorized this
        account by creating a Service Principal with the appropriate (Contributor)
        role.
    Returns:
      A ServicePrincipalCredentials instance, that can be used to access or
        create any resources.
    """
    app_id = parameters[self.PARAM_APP_ID]
    app_secret_key = parameters[self.PARAM_APP_SECRET]
    tenant_id = parameters[self.PARAM_TENANT_ID]

    # Get an Authentication token using ADAL.
    context = adal.AuthenticationContext(self.AZURE_AUTH_ENDPOINT + tenant_id)
    token_response = context.acquire_token_with_client_credentials(
      self.AZURE_RESOURCE_URL, app_id, app_secret_key)
    token_response.get('accessToken')

    # To access Azure resources for an application, we need a Service Principal
    # with the accurate role assignment. It can be created using the Azure CLI.
    credentials = ServicePrincipalCredentials(client_id=app_id,
                                              secret=app_secret_key,
                                              tenant=tenant_id)
    return credentials

  def create_network_interface(self, network_client, interface_name, ip_name,
                               subnet, parameters):
    """ Creates the Public IP Address resource and uses that to create the
    Network Interface.
    Args:
      network_client: A NetworkManagementClient instance.
      interface_name: The name to use for the Network Interface resource.
      ip_name: The name to use for the Public IP Address resource.
      subnet: The Subnet resource from the Virtual Network created.
      parameters:  A dict, containing all the parameters necessary to
        authenticate this user with Azure.
    """
    group_name = parameters[self.PARAM_RESOURCE_GROUP]
    region = parameters[self.PARAM_ZONE]
    utils.log("Creating/Updating the Public IP Address '{}'".format(ip_name))
    ip_address = PublicIPAddress(
      location=region, public_ip_allocation_method=IPAllocationMethod.dynamic,
      idle_timeout_in_minutes=4)
    result = network_client.public_ip_addresses.create_or_update(
      group_name, ip_name, ip_address)
    self.sleep_until_update_operation_done(result, ip_name)
    public_ip_address = network_client.public_ip_addresses.get(group_name, ip_name)

    utils.log("Creating/Updating the Network Interface '{}'".format(interface_name))
    network_interface_ip_conf = NetworkInterfaceIPConfiguration(
      name=interface_name, private_ip_allocation_method=IPAllocationMethod.dynamic,
      subnet=subnet, public_ip_address=PublicIPAddress(id=(public_ip_address.id)))

    result = network_client.network_interfaces.create_or_update(
      group_name, interface_name, NetworkInterface(location=region,
        ip_configurations=[network_interface_ip_conf]))
    self.sleep_until_update_operation_done(result, interface_name)

  def sleep_until_update_operation_done(self, result, resource_name):
    """ Sleeps until the create/update operation for the resource is completed
      successfully.
    Args:
      result: An instance, of the AzureOperationPoller to poll for the status
        of the operation being performed.
      resource_name: The name of the resource being updated.
    """
    time_start = time.time()
    while not result.done():
      utils.log("Waiting {0} second(s) for {1} to be created/updated.".
                format(self.SLEEP_TIME, resource_name))
      time.sleep(self.SLEEP_TIME)
      total_sleep_time = time.time() - time_start
      if total_sleep_time > self.MAX_SLEEP_TIME:
        utils.log("Waited {0} second(s) for {1} to be created/updated. "
          "Operation has timed out.".format(total_sleep_time, resource_name))
        break
