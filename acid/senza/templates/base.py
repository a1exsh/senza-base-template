'''
The template for the PostgreSQL-based Database as a Service.
'''

import random
import string
from urllib.parse import urlparse

import boto3
import requests
import dns.resolver
from clickclick import fatal_error
from senza.aws import encrypt, list_kms_keys, get_security_group
from senza.utils import pystache_render


from senza.templates._helper import check_s3_bucket, get_account_alias

POSTGRES_PORT = 5432
HEALTHCHECK_PORT = 8008
SPILO_IMAGE_ADDRESS = "registry.opensource.zalan.do/acid/spilo-9.5"
ODD_SG_NAME = 'Odd (SSH Bastion Host)'

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
        Minimum: 3
        Maximum: 3
        MetricType: CPU
      InstanceType: {{instance_type}}
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
            postgresql:
                {{#postgresqlconf}}
                parameters:
                    {{postgresqlconf}}
                {{/postgresqlconf}}
                pg_hba:
                    - hostnossl all all all reject
                    {{#ldap_suffix}}
                    - hostssl   all +zalandos all ldap ldapserver="localhost" ldapprefix="uid=" ldapsuffix=",{{ldap_suffix}}"
                    {{/ldap_suffix}}
                    - hostssl   all all all md5
        root: True
        sysctl:
          vm.overcommit_memory: 2
          vm.overcommit_ratio: 60
          vm.dirty_ratio: 8
          vm.dirty_background_ratio: 1
          vm.swappiness: 1
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
        {{#scalyr_account_key}}
        scalyr_account_key: "{{scalyr_account_key}}"
        {{/scalyr_account_key}}
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
      LoadBalancerName: "spilo-{{version}}-replica"
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
    variables.setdefault('instance_type', 't2.medium')
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
    variables.setdefault('scalyr_account_key', None)
    variables.setdefault('snapshot_id', None)
    variables.setdefault('use_ebs', True)
    variables.setdefault('volume_iops', None)
    variables.setdefault('volume_size', 10)
    variables.setdefault('volume_type', 'gp2')
    variables.setdefault('wal_s3_bucket', None)
    variables.setdefault('zmon_sg_id', None)

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

    # make sure all DNS names belong to the hosted zone
    for v in ('master_dns_name', 'replica_dns_name'):
        if variables[v] and not check_dns_name(variables[v], variables['hosted_zone'][:-1]):
            fatal_error("{0} should end with {1}".format(v.replace('_', ' '), variables['hosted_zone'][:-1]))

    # if master DNS name is specified but not the replica one - derive the replica name from the master
    if variables['master_dns_name'] and not variables['replica_dns_name']:
        replica_dns_components = variables['master_dns_name'].split('.')
        replica_dns_components[0] += '-repl'
        variables['replica_dns_name'] = '.'.join(replica_dns_components)

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

    odd_sg = get_security_group(region.Region, ODD_SG_NAME)
    variables['odd_sg_id'] = odd_sg.group_id

    # Find all Security Groups attached to the zmon worker with 'zmon' in their name
    variables['zmon_sg_id'] = detect_zmon_security_group(region.Region)

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

    for key in [k for k in variables if k.startswith('pgpassword_')] +\
            (['scalyr_account_key'] if variables.get('scalyr_account_key') else []):
        encrypted = encrypt(region=region.Region, KeyId=kms_keyid, Plaintext=variables[key], b64encode=True)
        variables[key] = 'aws:kms:{}'.format(encrypted)

    check_s3_bucket(variables['wal_s3_bucket'], region.Region)

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


def detect_zmon_security_group(region):
    ec2 = boto3.client('ec2', region)
    filters = [{'Name': 'tag-key', 'Values': ['StackName']}, {'Name': 'tag-value', 'Values': ['zmon-worker']}]
    zmon_sgs = list()
    for reservation in ec2.describe_instances(Filters=filters).get('Reservations', []):
        for instance in reservation.get('Instances', []):
            zmon_sgs += [sg['GroupId'] for sg in instance.get('SecurityGroups', []) if 'zmon' in sg['GroupName']]

    if len(zmon_sgs) == 0:
        fatal_error('Could not find zmon security group')

    if len(zmon_sgs) > 1:
        fatal_error("More than one security group found for zmon")

    return zmon_sgs[0]
