'''
The template for the PostgreSQL-based Database as a Service.
'''

import random
import string
import re
from urllib.parse import urlparse

import boto3
import requests
from requests.exceptions import RequestException
import dns.resolver
from clickclick import Action, fatal_error
from senza.aws import encrypt, list_kms_keys
from senza.utils import pystache_render

from senza.templates._helper import check_s3_bucket, get_account_alias

POSTGRES_PORT = 5432
HEALTHCHECK_PORT = 8008
SPILO_IMAGE_ADDRESS = "registry.opensource.zalan.do/acid/spilo-9.5"
ODD_SG_GROUP_NAME_REGEX = 'Odd.*'
ZMON_SG_GROUP_NAME_REGEX = 'app-zmon-db'
PRICE_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"

# This template goes through 2 formatting phases. Once during the init phase and once during
# the create phase of senza. Some placeholders should be evaluated during create.
# This creates some ugly placeholder formatting, therefore some placeholders are placeholders for placeholders
# - version
# - ImageVersion
TEMPLATE = '''
# basic information for generating and executing this definition
SenzaInfo:
  StackName: spilo
  Tags:
    - SpiloCluster: "{{version}}"

# a list of senza components to apply to the definition
SenzaComponents:

  # this basic configuration is required for the other components
  - Configuration:
      Type: Senza::StupsAutoConfiguration # auto-detect network setup

  # will create a launch configuration and auto scaling group with scaling triggers
  - AppServer:
      Type: Senza::TaupageAutoScalingGroup
      AutoScaling:
        Minimum: {{number_of_instances}}
        Maximum: {{number_of_instances}}
        MetricType: CPU
      InstanceType: {{instance_type}}
      {{#use_spot_instances}}
      SpotPrice: {{spot_price}}
      {{/use_spot_instances}}
      {{#ebs_optimized}}
      EbsOptimized: True
      {{/ebs_optimized}}
      BlockDeviceMappings:
        - DeviceName: /dev/xvdk
          {{#use_ebs}}
          Ebs:
            VolumeSize: {{volume_size}}
            VolumeType: {{volume_type}}
            {{#snapshot_id}}
            SnapshotId: {{snapshot_id}}
            {{/snapshot_id}}
            {{#volume_iops}}
            Iops: {{volume_iops}}
            {{/volume_iops}}
          {{/use_ebs}}
      ElasticLoadBalancer:
        - PostgresLoadBalancer
        {{#add_replica_loadbalancer}}
        - PostgresReplicaLoadBalancer
        {{/add_replica_loadbalancer}}
      HealthCheckType: EC2
      SecurityGroups:
        - Fn::GetAtt:
          - SpiloMemberSG
          - GroupId
      IamRoles:
        - Ref: PostgresAccessRole
      AssociatePublicIpAddress: false # change for standalone deployment in default VPC
      TaupageConfig:
        runtime: Docker
        source: {{docker_image}}
        ports:
          {{postgres_port}}: {{postgres_port}}
          {{healthcheck_port}}: {{healthcheck_port}}
        etcd_discovery_domain: "{{discovery_domain}}"
        environment:
          SCOPE: "{{version}}"
          ETCD_DISCOVERY_DOMAIN: "{{discovery_domain}}"
          WAL_S3_BUCKET: "{{wal_s3_bucket}}"
          PGPASSWORD_SUPERUSER: "{{pgpassword_superuser}}"
          PGPASSWORD_ADMIN: "{{pgpassword_admin}}"
          PGPASSWORD_STANDBY: "{{pgpassword_standby}}"
          BACKUP_SCHEDULE: "00 01 * * *"
          {{#ldap_url}}
          LDAP_URL: {{ldap_url}}
          {{/ldap_url}}
          PATRONI_CONFIGURATION: | ## https://github.com/zalando/patroni#yaml-configuration
            bootstrap:
              dcs:
                postgresql:
                  parameters:
                    logging_collector: on
                    log_destination: csvlog
                    log_directory: ../pg_log
                    log_filename: postgresql-%u.log
                    log_file_mode: 0644
                    log_rotation_age: 1d
                    log_truncate_on_rotation: on
                    shared_preload_libraries: pg_stat_statements
                    track_functions: all
                {{#postgresqlconf}}
                    {{postgresqlconf}}
                {{/postgresqlconf}}
              initdb:
                - auth-host: md5
                - auth-local: trust
              pg_hba:
                - hostnossl all all all reject
                {{#ldap_suffix}}
                - hostssl   all +zalandos all ldap ldapserver="localhost" ldapprefix="uid=" ldapsuffix=",{{ldap_suffix}}"
                {{/ldap_suffix}}
                - hostssl   all all all md5
        root: True
        sysctl:
          vm.overcommit_memory: 2
          vm.overcommit_ratio: 80
          vm.dirty_ratio: 8
          vm.dirty_background_ratio: 1
          vm.swappiness: 1
        appdynamics_application: 'spilo-{{version}}'
        mounts:
          /home/postgres/pgdata:
            partition: /dev/xvdk
            filesystem: {{fstype}}
            {{#snapshot_id}}
            erase_on_boot: false
            {{/snapshot_id}}
            {{^snapshot_id}}
            erase_on_boot: true
            {{/snapshot_id}}
            options: {{fsoptions}}
Resources:
  {{#add_replica_loadbalancer}}
  PostgresReplicaRoute53Record:
    Type: AWS::Route53::RecordSet
    Properties:
      Type: CNAME
      TTL: 20
      HostedZoneName: {{hosted_zone}}
      {{#replica_dns_name}}
      Name: {{replica_dns_name}}
      {{/replica_dns_name}}
      {{^replica_dns_name}}
      Name: "{{version}}-replica.{{team_name}}.{{hosted_zone}}"
      {{/replica_dns_name}}
      ResourceRecords:
        - Fn::GetAtt:
           - PostgresReplicaLoadBalancer
           - DNSName
  PostgresReplicaLoadBalancer:
    Type: AWS::ElasticLoadBalancing::LoadBalancer
    Properties:
      CrossZone: true
      HealthCheck:
        HealthyThreshold: 2
        Interval: 5
        Target: HTTP:{{healthcheck_port}}/replica
        Timeout: 3
        UnhealthyThreshold: 2
      Listeners:
        - InstancePort: {{postgres_port}}
          LoadBalancerPort: {{postgres_port}}
          Protocol: TCP
      LoadBalancerName: "spilo-{{version}}-repl"
      ConnectionSettings:
        IdleTimeout: 3600
      SecurityGroups:
        - Fn::GetAtt:
          - SpiloReplicaSG
          - GroupId
      Scheme: internet-facing
      Subnets:
        Fn::FindInMap:
          - LoadBalancerSubnets
          - Ref: AWS::Region
          - Subnets
  {{/add_replica_loadbalancer}}
  PostgresRoute53Record:
    Type: AWS::Route53::RecordSet
    Properties:
      Type: CNAME
      TTL: 20
      HostedZoneName: {{hosted_zone}}
      {{#master_dns_name}}
      Name: {{master_dns_name}}
      {{/master_dns_name}}
      {{^master_dns_name}}
      Name: "{{version}}.{{team_name}}.{{hosted_zone}}"
      {{/master_dns_name}}
      ResourceRecords:
        - Fn::GetAtt:
           - PostgresLoadBalancer
           - DNSName
  PostgresLoadBalancer:
    Type: AWS::ElasticLoadBalancing::LoadBalancer
    Properties:
      CrossZone: true
      HealthCheck:
        HealthyThreshold: 2
        Interval: 5
        Target: HTTP:{{healthcheck_port}}/master
        Timeout: 3
        UnhealthyThreshold: 2
      Listeners:
        - InstancePort: {{postgres_port}}
          LoadBalancerPort: {{postgres_port}}
          Protocol: TCP
      LoadBalancerName: "spilo-{{version}}"
      ConnectionSettings:
        IdleTimeout: 3600
      SecurityGroups:
        - Fn::GetAtt:
          - SpiloMasterSG
          - GroupId
      Scheme: internet-facing
      Subnets:
        Fn::FindInMap:
          - LoadBalancerSubnets
          - Ref: AWS::Region
          - Subnets
  PostgresAccessRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
        - Effect: Allow
          Principal:
            Service: ec2.amazonaws.com
          Action: sts:AssumeRole
      Path: /
      Policies:
      - PolicyName: SpiloEC2S3KMSAccess
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
          - Effect: Allow
            Action:
              - s3:ListBucket
            Resource:
              - "arn:aws:s3:::{{wal_s3_bucket}}"
              - "arn:aws:s3:::{{wal_s3_bucket}}/*"
          - Effect: Allow
            Action:
              - s3:*
            Resource:
              - "arn:aws:s3:::{{wal_s3_bucket}}/spilo/{{version}}/*"
          - Effect: Allow
            Action: ec2:CreateTags
            Resource: "*"
          - Effect: Allow
            Action: ec2:Describe*
            Resource: "*"
          {{#kms_arn}}
          - Effect: Allow
            Action:
              - "kms:Decrypt"
              - "kms:Encrypt"
            Resource:
              - {{kms_arn}}
          {{/kms_arn}}
  SpiloMasterSG:
    Type: "AWS::EC2::SecurityGroup"
    Properties:
      GroupDescription: "Security Group for the master ELB of Spilo: {{version}}"
      SecurityGroupIngress:
        {{spilo_security_group_ingress_rules_block}}
  {{#add_replica_loadbalancer}}
  SpiloReplicaSG:
    Type: "AWS::EC2::SecurityGroup"
    Properties:
      GroupDescription: "Security Group for the replica ELB of Spilo: {{version}}"
      SecurityGroupIngress:
        {{spilo_security_group_ingress_rules_block}}
  {{/add_replica_loadbalancer}}
  SpiloMemberSG:
    Type: "AWS::EC2::SecurityGroup"
    Properties:
      GroupDescription: "Security Group for members of Spilo: {{version}}"
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: {{postgres_port}}
          ToPort: {{postgres_port}}
          SourceSecurityGroupId:
            Fn::GetAtt:
              - SpiloMasterSG
              - GroupId
        - IpProtocol: tcp
          FromPort: {{healthcheck_port}}
          ToPort: {{healthcheck_port}}
          SourceSecurityGroupId:
            Fn::GetAtt:
              - SpiloMasterSG
              - GroupId
        {{#add_replica_loadbalancer}}
        - IpProtocol: tcp
          FromPort: {{postgres_port}}
          ToPort: {{postgres_port}}
          SourceSecurityGroupId:
            Fn::GetAtt:
              - SpiloReplicaSG
              - GroupId
        - IpProtocol: tcp
          FromPort: {{healthcheck_port}}
          ToPort: {{healthcheck_port}}
          SourceSecurityGroupId:
            Fn::GetAtt:
              - SpiloReplicaSG
              - GroupId
        {{/add_replica_loadbalancer}}
        {{#zmon_sg_id}}
        - IpProtocol: tcp
          FromPort: {{promotheus_port}}
          ToPort: {{promotheus_port}}
          SourceSecurityGroupId: "{{zmon_sg_id}}"
        - IpProtocol: tcp
          FromPort: {{postgres_port}}
          ToPort: {{postgres_port}}
          SourceSecurityGroupId: "{{zmon_sg_id}}"
        - IpProtocol: tcp
          FromPort: {{healthcheck_port}}
          ToPort: {{healthcheck_port}}
          SourceSecurityGroupId: "{{zmon_sg_id}}"
        {{/zmon_sg_id}}
        {{#odd_sg_id}}
        - IpProtocol: tcp
          FromPort: 0
          ToPort: 65535
          SourceSecurityGroupId: "{{odd_sg_id}}"
        {{/odd_sg_id}}
  SpiloMemberIngressMembers:
    Type: "AWS::EC2::SecurityGroupIngress"
    Properties:
      GroupId:
        Fn::GetAtt:
          - SpiloMemberSG
          - GroupId
      IpProtocol: tcp
      FromPort: 0
      ToPort: 65535
      SourceSecurityGroupId:
        Fn::GetAtt:
          - SpiloMemberSG
          - GroupId
'''


def ebs_optimized_supported(instance_type):
    # per http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSOptimized.html
    """
    >>> ebs_optimized_supported('c3.xlarge')
    True
    >>> ebs_optimized_supported('t2.micro')
    False
    """
    return instance_type in ('c1.large', 'c3.xlarge', 'c3.2xlarge', 'c3.4xlarge',
                             'c4.large', 'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge', 'c4.8xlarge',
                             'd2.xlarge', 'd2.2xlarge', 'd2.4xlarge', 'd2.8xlarge',
                             'g2.2xlarge', 'i2.xlarge', 'i2.2xlarge', 'i2.4xlarge',
                             'm1.large', 'm1.xlarge', 'm2.2xlarge', 'm2.4xlarge',
                             'm3.xlarge', 'm3.2xlarge', 'r3.xlarge', 'r3.2xlarge',
                             'r3.4xlarge')


def set_default_variables(variables):
    variables.setdefault('version', '{{Arguments.version}}')
    variables.setdefault('team_name', None)
    variables.setdefault('team_region', None)
    variables.setdefault('team_gateway_zone', None)
    # End of required variables #
    variables.setdefault('add_replica_loadbalancer', False)
    variables.setdefault('discovery_domain', None)
    variables.setdefault('master_dns_name', None)
    variables.setdefault('docker_image', get_latest_image())
    variables.setdefault('ebs_optimized', None)
    variables.setdefault('fsoptions', 'noatime,nodiratime,nobarrier')
    variables.setdefault('fstype', 'ext4')
    variables.setdefault('healthcheck_port', HEALTHCHECK_PORT)
    variables.setdefault('hosted_zone', None)
    variables.setdefault('instance_type', 'm4.large')
    variables.setdefault('number_of_instances', 3)
    variables.setdefault('ldap_url', None)
    variables.setdefault('ldap_suffix', None)
    variables.setdefault('kms_arn', None)
    variables.setdefault('odd_sg_id', None)
    variables.setdefault('pgpassword_admin', generate_random_password())
    variables.setdefault('pgpassword_standby', generate_random_password())
    variables.setdefault('pgpassword_superuser', generate_random_password())
    variables.setdefault('postgresqlconf', None)
    variables.setdefault('postgres_port', POSTGRES_PORT)
    variables.setdefault('promotheus_port', '9100')
    variables.setdefault('replica_dns_name', None)
    variables.setdefault('snapshot_id', None)
    variables.setdefault('use_ebs', True)
    variables.setdefault('volume_iops', None)
    variables.setdefault('volume_size', 50)
    variables.setdefault('volume_type', 'gp2')
    variables.setdefault('wal_s3_bucket', None)
    variables.setdefault('zmon_sg_id', None)
    variables.setdefault('use_spot_instances', False)
    variables.setdefault('spot_price', 0)

    return variables


def gather_user_variables(variables, account_info, region):

    set_default_variables(variables)

    missing = []
    for required in ('team_name', 'team_region', 'team_gateway_zone', 'hosted_zone'):
        if not variables.get(required):
            missing.append(required)
    if len(missing) > 0:
        fatal_error("Missing values for the following variables: {0}".format(', '.join(missing)))

    # redefine the region per the user input
    if variables['team_region'] != region.Region:
        fatal_error("Current region {0} do not match the requested region {1}\n"
                    "Change the currect region with --region option or set AWS_DEFAULT_REGION variable.".
                    format(region.Region, variables['team_region']))

    variables['wal_s3_bucket'] = '{}-{}-spilo-dbaas'.format(get_account_alias(), region.Region)

    for name in ('team_gateway_zone', 'hosted_zone'):
        if variables[name][-1] != '.':
            variables[name] += '.'

    # split the ldap url into the URL and suffix (path component)
    if variables['ldap_url']:
        url = urlparse(variables['ldap_url'])
        if url.path and url.path[0] == '/':
            variables['ldap_suffix'] = url.path[1:]

    # if master DNS name is specified but not the replica one - derive the replica name from the master
    if variables['master_dns_name'] and not variables['replica_dns_name']:
        replica_dns_components = variables['master_dns_name'].split('.')
        replica_dns_components[0] += '-repl'
        variables['replica_dns_name'] = '.'.join(replica_dns_components)

    # make sure all DNS names belong to the hosted zone
    for v in ('master_dns_name', 'replica_dns_name'):
        if variables[v] and not check_dns_name(variables[v], variables['hosted_zone'][:-1]):
            fatal_error("{0} should end with {1}".
                        format(v.replace('_', ' '), variables['hosted_zone'][:-1]))

    if variables['ldap_url'] and not variables['ldap_suffix']:
        fatal_error("LDAP URL is missing the suffix: shoud be in a format: "
                    "ldap[s]://example.com[:port]/ou=people,dc=example,dc=com")

    # pick up the proper etcd address depending on the region
    variables['discovery_domain'] = detect_etcd_discovery_domain_for_region(variables['hosted_zone'],
                                                                            region.Region)

    # get the IP addresses of the NAT gateways to acess a given ELB.
    variables['nat_gateway_addresses'] = detect_eu_team_nat_gateways(variables['team_gateway_zone'])
    variables['odd_instance_addresses'] = detect_eu_team_odd_instances(variables['team_gateway_zone'])
    variables['spilo_security_group_ingress_rules_block'] = \
        generate_spilo_master_security_group_ingress(variables['nat_gateway_addresses'] +
                                                     variables['odd_instance_addresses'])

    if variables['postgresqlconf']:
        variables['postgresqlconf'] = generate_postgresql_configuration(variables['postgresqlconf'])

    variables['odd_sg_id'] = detect_security_group(region.Region, ODD_SG_GROUP_NAME_REGEX)
    variables['zmon_sg_id'] = detect_security_group(region.Region, ZMON_SG_GROUP_NAME_REGEX)

    if variables['volume_type'] == 'io1' and not variables['volume_iops']:
        pio_max = variables['volume_size'] * 30
        variables['volume_iops'] = str(pio_max)
    variables['ebs_optimized'] = ebs_optimized_supported(variables['instance_type'])

    # pick up the first key with a description containing spilo
    kms_keys = [k for k in list_kms_keys(region.Region)
                if 'alias/aws/ebs' not in k['aliases'] and 'spilo' in ((k['Description']).lower())]

    if len(kms_keys) == 0:
        raise fatal_error('No KMS key is available for encrypting and decrypting. '
                          'Ensure you have at least 1 key available.')

    kms_key = kms_keys[0]
    kms_keyid = kms_key['KeyId']
    variables['kms_arn'] = kms_key['Arn']

    for key in [k for k in variables if k.startswith('pgpassword_')]:
        encrypted = encrypt(region=region.Region, KeyId=kms_keyid, Plaintext=variables[key], b64encode=True)
        variables[key] = 'aws:kms:{}'.format(encrypted)

    check_s3_bucket(variables['wal_s3_bucket'], region.Region)

    if variables['use_spot_instances'] and variables['spot_price'] == 0:
        with Action("Calculating the maximum spot price for {0}..".format(variables['instance_type'])) as act:
            on_demand_price = get_on_demand_price(act, variables['team_region'], variables['instance_type'])
            if on_demand_price == 0:
                act.fatal_error("Could not get the correct on-demand price, try running without use_spot_instances")
            else:
                variables['spot_price'] = on_demand_price * 1.2

    return variables


def check_dns_name(name, hosted_zone):
    """
    >>> check_dns_name('foo.bar.example.com')
    False
    >>> check_dns_name('foo.bar.' + hosted_zone )
    True
    """
    return name.endswith(hosted_zone)


def generate_random_password(length=64):
    """
    >>> len(generate_random_password(61))
    61
    """
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))


def generate_definition(variables):
    """
    >>> variables = set_default_variables(dict())
    >>> len(generate_definition(variables)) > 300
    True
    """
    definition_yaml = pystache_render(TEMPLATE, variables)
    return definition_yaml


def generate_spilo_master_security_group_ingress(addresses_to_allow):
    result = ""
    for addr in addresses_to_allow:
        if addr != addresses_to_allow[0]:
            result += '\n'+' ' * 8
        result += "- IpProtocol: tcp{delim}FromPort: {port}{delim}ToPort: {port}{delim}CidrIp: {ip}/32".\
                  format(port=POSTGRES_PORT, ip=addr, delim='\n' + ' ' * 10)
    return result


# we cannot use the JSON form {'name': value}, since pystache manges the quotes.
def generate_postgresql_configuration(postgresqlconf):
    result = ""
    options = [t.strip() for t in postgresqlconf.strip('{}').split(',')]
    for opt in options:
        if opt != options[0]:
            result += '\n' + ' ' * 20
        key, value = opt.split(':', 2)
        result += "{0}:  {1}".format(key.strip(), value.strip())
    return result


def get_latest_image(registry_domain='registry.opensource.zalan.do', team='acid', artifact='spilo-9.5'):
    """
    >>> 'registry.opensource.zalan.do' in get_latest_image()
    True
    >>> get_latest_image('dont.exist.url')
    ''
    """
    try:
        r = requests.get('https://{0}/teams/{1}/artifacts/{2}/tags'.format(registry_domain, team, artifact))
        if r.ok:
            # sort the tags by creation date
            latest = None
            for entry in sorted(r.json(), key=lambda t: t['created'], reverse=True):
                tag = entry['name']
                # try to avoid snapshots if possible
                if 'SNAPSHOT' not in tag:
                    latest = tag
                    break
                latest = latest or tag
            return "{0}/{1}/{2}:{3}".format(registry_domain, team, artifact, latest)
    except:
        pass
    return ""


def get_records_for_hosted_zone(zone_name):
    route53 = boto3.client('route53')
    zones = route53.list_hosted_zones_by_name()
    zone_id = None

    for z in zones['HostedZones']:
        if z['Name'] == zone_name:
            zone_id = z['Id']
            break
    return route53.list_resource_record_sets(HostedZoneId=zone_id) if zone_id else []


def detect_etcd_discovery_domain_for_region(dbaas_zone, user_region):
    """ Query DNS zone for the etcd record corresponding to a given region. """
    user_region = user_region.split('-')[1]  # leave only 'west' out of 'eu-west-1'
    records = get_records_for_hosted_zone(dbaas_zone)
    if not records:
        fatal_error("Unable to list records for {0}: make sure you are logged into the DBaaS account".
                    format(dbaas_zone))
    for r in records['ResourceRecordSets']:
        if r['Type'] == 'SRV' and r['Name'] == '_etcd._tcp.{region}.{zone}'.format(region=user_region,
                                                                                   zone=dbaas_zone):
            return "{region}.{zone}".format(region=user_region,
                                            zone=dbaas_zone[:-1])
    return None


def detect_eu_team_nat_gateways(team_zone_name):
    """
        Detect NAT gateways. Since the complete zone is not hosted by DBaaS and
        not accessible, try to figure out individual NAT endpoints by name.
    """
    resolver = dns.resolver.Resolver()
    nat_gateways = []
    for region in ('eu-west-1', 'eu-central-1'):
        for az in ('a', 'b', 'c'):
            try:
                answer = resolver.query('nat-{region}{az}.{zone}'.
                                        format(region=region, az=az, zone=team_zone_name), 'A')
                nat_gateways.extend(str(rdata) for rdata in answer)
            except dns.resolver.NXDOMAIN:
                continue
    if not nat_gateways:
        fatal_error("Unable to detect nat gateways: make sure {0} account is set up correctly".format(team_zone_name))
    return nat_gateways


def detect_eu_team_odd_instances(team_zone_name):
    """
      Detect the odd instances by name. Same reliance on a convention as with
      the detect_eu_team_nat_gateways.
    """
    resolver = dns.resolver.Resolver()
    odd_hosts = []
    for region in ('eu-west-1', 'eu-central-1'):
        try:
            answer = resolver.query('odd-{region}.{zone}'.format(region=region, zone=team_zone_name))
            odd_hosts.extend(str(rdata) for rdata in answer)
        except dns.resolver.NXDOMAIN:
            continue

    if not odd_hosts:
        fatal_error("Unable to detect odd hosts: make sure {0} account is set up correctly".format(team_zone_name))
    return odd_hosts


def detect_security_group(region, sg_regex):
    ec2 = boto3.client('ec2', region)

    sgs = [sg for sg in ec2.describe_security_groups()['SecurityGroups'] if re.match(sg_regex, sg['GroupName'])]

    if len(sgs) == 0:
        fatal_error('Could not find security group which matches regex {}'.format(sg_regex))
    if len(sgs) > 1:
        fatal_error('More than one security group found for regex {}'.format(sg_regex))

    return sgs[0]['GroupId']


def get_on_demand_price(act, region, instance_type):
    """
        Calculate prices on demand for a given region and instance type
        Fetch the SKU of the desired on-demand instance from AWS API,
        then use the SKU to fetch the acutal price.
        XXX: the API returns a json of 45MB, takes long to parse
    """
    if region == 'eu-central-1':
        region = 'EU (Ireland)'
    elif region == 'eu-west-1':
        region = 'EU (Frankfurt)'
    else:
        act.fatal_error("Region {0} is not supported for EC2 by this template".format(region))
    try:
        prices_request = requests.get(PRICE_URL)
    except RequestException as e:
        act.fatal_error("Could not get AWS EC2 pricing API {0}: {1}".format(PRICE_URL, e))

    if prices_request.ok:
        prices = prices_request.json()
        for p in prices['products'].values():
            if (p['productFamily'] == 'Compute Instance' and
                    p['attributes'].get('location') == region and
                    p['attributes']['instanceType'] == instance_type and
                    p['attributes']['operatingSystem'] == 'Linux' and
                    p['attributes']['tenancy'] == 'Shared'):
                sku = p['sku']
                break
        else:
            act.fatal_error("Cannot fetch SKU for the price of instance {0}".format(instance_type))
        price_object = prices['terms']['OnDemand'][sku]
        if len(price_object) != 1:
            act.fatal_error("Format error: more than one entry for SKU {0}: {1}".format(sku, price_object))
        price_dimension = price_object.popitem()[1]['priceDimensions'].popitem()[1]
        if 'pricePerUnit' in price_dimension:
            instance_price = price_dimension['pricePerUnit'].get('USD', '0')
            return float(instance_price)
        else:
            act.fatal_error("Unable to find a single instance price for instance {0} sku {1}".format(
                             instance_type,
                             sku))
    else:
        act.fatal_error("Request to AWS EC2 pricing API {0} did not succeed: {1}".format(PRICE_URL, prices.status_code))
