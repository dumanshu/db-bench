"""Shared AWS infrastructure functions: VPC, subnet, SG, EC2, cleanup."""

import time
import botocore

import common.util as _u
from common.util import SSH_PORT, ec2, log, tags_common, ensure_tags
from common.types import InstanceInfo


# ---------------------------------------------------------------------------
# VPC / Network
# ---------------------------------------------------------------------------

def get_vpcs():
    resp = ec2().describe_vpcs(
        Filters=[{"Name": "tag:Project", "Values": [_u.STACK]}]
    )
    return resp.get("Vpcs", [])


def ensure_vpc(vpc_cidr):
    v = get_vpcs()
    if v:
        vpc_id = v[0]["VpcId"]
        log(f"REUSED  vpc: {vpc_id}")
        return vpc_id
    resp = ec2().create_vpc(CidrBlock=vpc_cidr, TagSpecifications=[{
        "ResourceType": "vpc",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{_u.STACK}-vpc"}]
    }])
    vpc_id = resp["Vpc"]["VpcId"]
    log(f"CREATED vpc: {vpc_id}")
    try:
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    except botocore.exceptions.ClientError:
        pass
    return vpc_id


def get_igw_for_vpc(vpc_id):
    resp = ec2().describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )
    igws = resp.get("InternetGateways", [])
    return igws[0]["InternetGatewayId"] if igws else None


def ensure_igw(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"REUSED  igw (attached): {igw}")
        return igw
    resp = ec2().create_internet_gateway(TagSpecifications=[{
        "ResourceType": "internet-gateway",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{_u.STACK}-igw"}]
    }])
    igw_id = resp["InternetGateway"]["InternetGatewayId"]
    ec2().attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    log(f"CREATED igw: {igw_id}")
    return igw_id


def name_variants(base):
    return {base, f"{base}-a", f"{base}-b"}


def find_subnet(vpc_id, name, cidr):
    candidates = name_variants(name)
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    subnets = resp.get("Subnets", [])

    def tag_dict(sn):
        return {t["Key"]: t["Value"] for t in sn.get("Tags", [])}

    for sn in subnets:
        tags = tag_dict(sn)
        if tags.get("Project") == _u.STACK and tags.get("Name") in candidates:
            return sn["SubnetId"], tags
    for sn in subnets:
        tags = tag_dict(sn)
        if tags.get("Name") in candidates:
            return sn["SubnetId"], tags
    for sn in subnets:
        if sn.get("CidrBlock") == cidr:
            return sn["SubnetId"], tag_dict(sn)
    return None, None


def ensure_subnet(vpc_id, name, cidr, az=None, public=False):
    sn, tags = find_subnet(vpc_id, name, cidr)
    if sn:
        kv_updates = []
        project_missing = not tags or tags.get("Project") != _u.STACK
        if not tags or tags.get("Name") not in name_variants(name):
            kv_updates.append({"Key": "Name", "Value": name})
        if kv_updates or project_missing:
            ensure_tags([sn], kv_updates)
        log(f"REUSED  subnet {('public' if public else 'private')}: {sn}")
        return sn
    availability_zone = az or f"{_u.REGION}a"
    resp = ec2().create_subnet(
        VpcId=vpc_id,
        CidrBlock=cidr,
        AvailabilityZone=availability_zone,
        TagSpecifications=[{
            "ResourceType": "subnet",
            "Tags": tags_common() + [{"Key": "Name", "Value": name}]
        }],
    )
    subnet_id = resp["Subnet"]["SubnetId"]
    if public:
        ec2().modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    log(f"CREATED subnet {('public' if public else 'private')}: {subnet_id} (AZ: {availability_zone})")
    return subnet_id


def find_rtb_by_name(name):
    resp = ec2().describe_route_tables(
        Filters=[{"Name": "tag:Name", "Values": [name]},
                 {"Name": "tag:Project", "Values": [_u.STACK]}]
    )
    rtbs = resp.get("RouteTables", [])
    return rtbs[0]["RouteTableId"] if rtbs else None


def ensure_public_rtb(vpc_id, igw_id, subnet_ids):
    name = f"{_u.STACK}-rtb-public"
    rtb = find_rtb_by_name(name)
    if not rtb:
        resp = ec2().create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "route-table",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        rtb = resp["RouteTable"]["RouteTableId"]
        ec2().create_route(RouteTableId=rtb, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
        log(f"CREATED rtb public: {rtb}")
    else:
        log(f"REUSED  rtb public: {rtb}")
    if isinstance(subnet_ids, str):
        subnet_ids = [subnet_ids]
    for subnet_id in subnet_ids:
        try:
            ec2().associate_route_table(RouteTableId=rtb, SubnetId=subnet_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "Resource.AlreadyAssociated":
                raise
    return rtb


# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

def ensure_sg(vpc_id, name, description):
    resp = ec2().describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [name]},
                 {"Name": "vpc-id", "Values": [vpc_id]}]
    )
    sgs = resp.get("SecurityGroups", [])
    if sgs:
        sg_id = sgs[0]["GroupId"]
        log(f"REUSED  sg {name}: {sg_id}")
    else:
        description = description[:200]
        resp = ec2().create_security_group(
            GroupName=name, Description=description, VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "security-group",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        sg_id = resp["GroupId"]
        log(f"CREATED sg {name}: {sg_id}")
    return sg_id


def ensure_ingress_tcp_cidr(sg, port, cidr):
    try:
        ec2().authorize_security_group_ingress(
            GroupId=sg, IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                "IpRanges": [{"CidrIp": cidr}]
            }]
        )
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            return False
        raise


def refresh_ssh_rule(sg_id, cidr):
    if not cidr:
        return
    resp = ec2().describe_security_groups(GroupIds=[sg_id])
    perms = resp["SecurityGroups"][0].get("IpPermissions", [])
    need_authorize = True
    to_revoke = []
    for p in perms:
        if p.get("IpProtocol") == "tcp" and p.get("FromPort") == SSH_PORT and p.get("ToPort") == SSH_PORT:
            for r in p.get("IpRanges", []):
                ip = r.get("CidrIp")
                if ip == cidr:
                    need_authorize = False
                else:
                    to_revoke.append(ip)
    if to_revoke:
        try:
            ec2().revoke_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": SSH_PORT,
                    "ToPort": SSH_PORT,
                    "IpRanges": [{"CidrIp": ip} for ip in to_revoke]
                }]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise
    if need_authorize:
        try:
            ec2().authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": SSH_PORT,
                    "ToPort": SSH_PORT,
                    "IpRanges": [{"CidrIp": cidr}]
                }]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                raise


# ---------------------------------------------------------------------------
# EC2 Instances
# ---------------------------------------------------------------------------

def find_instance_id_by_name(name):
    resp = ec2().describe_instances(
        Filters=[{"Name": "tag:Name", "Values": [name]},
                 {"Name": "tag:Project", "Values": [_u.STACK]},
                 {"Name": "instance-state-name",
                  "Values": ["pending", "running", "stopping", "stopped"]}]
    )
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst["InstanceId"]
    return None


def find_all_stack_instances():
    resp = ec2().describe_instances(
        Filters=[{"Name": "tag:Project", "Values": [_u.STACK]},
                 {"Name": "instance-state-name",
                  "Values": ["pending", "running", "stopping", "stopped"]}]
    )
    instances = []
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            instances.append({
                "id": inst["InstanceId"],
                "name": tags.get("Name", ""),
                "role": tags.get("Role", "unknown"),
                "state": inst["State"]["Name"],
            })
    return instances


def wait_running(iid):
    ec2().get_waiter("instance_running").wait(InstanceIds=[iid])


def describe_instance(iid):
    resp = ec2().describe_instances(InstanceIds=[iid])
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst
    raise RuntimeError(f"Instance {iid} not found.")


def ensure_instance(name, role, itype, subnet_id, sg_id,
                    resolved_ami_id_fn, key_name, ensure_keypair_fn,
                    root_volume_size=100, associate_public_ip=True,
                    user_data=None):
    iid = find_instance_id_by_name(name)
    if iid:
        log(f"REUSED  instance {role}: {iid}")
        return iid
    ensure_keypair_fn()
    ni = [{
        "DeviceIndex": 0,
        "SubnetId": subnet_id,
        "AssociatePublicIpAddress": associate_public_ip,
        "Groups": [sg_id]
    }]
    block_devices = [{
        "DeviceName": "/dev/xvda",
        "Ebs": {
            "VolumeSize": root_volume_size,
            "VolumeType": "gp3",
            "DeleteOnTermination": True
        }
    }]
    kwargs = dict(
        ImageId=resolved_ami_id_fn(),
        InstanceType=itype,
        KeyName=key_name,
        NetworkInterfaces=ni,
        BlockDeviceMappings=block_devices,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": tags_common() + [
                {"Key": "Name", "Value": name},
                {"Key": "Role", "Value": role}
            ]
        }],
        MinCount=1, MaxCount=1,
    )
    if user_data:
        kwargs["UserData"] = user_data
    resp = ec2().run_instances(**kwargs)
    iid = resp["Instances"][0]["InstanceId"]
    log(f"CREATED instance {role}: {iid}")
    wait_running(iid)
    return iid


def instance_info_from_id(iid, role) -> InstanceInfo:
    inst = describe_instance(iid)
    return InstanceInfo(
        role=role,
        instance_id=iid,
        public_ip=inst.get("PublicIpAddress", ""),
        private_ip=inst.get("PrivateIpAddress", ""),
        availability_zone=inst.get("Placement", {}).get("AvailabilityZone", ""),
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def terminate_stack_instances():
    instances = find_all_stack_instances()
    if not instances:
        log("No stack instances found to terminate.")
        return

    ids_to_terminate = [
        inst["id"] for inst in instances
        if inst["state"] not in ("terminated", "shutting-down")
    ]
    if not ids_to_terminate:
        log("All instances already terminated.")
        return

    for inst in instances:
        if inst["id"] in ids_to_terminate:
            log(f"TERMINATING instance {inst['role']} ({inst['name']}): {inst['id']}")

    try:
        ec2().terminate_instances(InstanceIds=ids_to_terminate)
        waiter = ec2().get_waiter("instance_terminated")
        waiter.wait(InstanceIds=ids_to_terminate)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidInstanceID.NotFound":
            raise
    except botocore.exceptions.WaiterError:
        log("Warning: Timeout while waiting for instances to terminate; continuing.")


def revoke_all_permissions(sg_id):
    desc = ec2().describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
    ingress = desc.get("IpPermissions", [])
    if ingress:
        try:
            ec2().revoke_security_group_ingress(GroupId=sg_id, IpPermissions=ingress)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise
    egress = desc.get("IpPermissionsEgress", [])
    if egress:
        try:
            ec2().revoke_security_group_egress(GroupId=sg_id, IpPermissions=egress)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise


def remove_group_references(group_ids):
    if not group_ids:
        return
    resp = ec2().describe_security_groups()
    for sg in resp.get("SecurityGroups", []):
        sg_id = sg["GroupId"]
        ingress_revoke = []
        for perm in sg.get("IpPermissions", []):
            matching = [pair for pair in perm.get("UserIdGroupPairs", [])
                        if pair["GroupId"] in group_ids]
            if matching:
                entry = {
                    "IpProtocol": perm.get("IpProtocol"),
                    "UserIdGroupPairs": [{"GroupId": pair["GroupId"]} for pair in matching],
                }
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                ingress_revoke.append(entry)
        if ingress_revoke:
            log(f"REVOKING ingress references from {sg_id}")
            try:
                ec2().revoke_security_group_ingress(GroupId=sg_id, IpPermissions=ingress_revoke)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in (
                    "InvalidPermission.NotFound", "InvalidGroup.NotFound"
                ):
                    raise
        egress_revoke = []
        for perm in sg.get("IpPermissionsEgress", []):
            matching = [pair for pair in perm.get("UserIdGroupPairs", [])
                        if pair["GroupId"] in group_ids]
            if matching:
                entry = {
                    "IpProtocol": perm.get("IpProtocol"),
                    "UserIdGroupPairs": [{"GroupId": pair["GroupId"]} for pair in matching],
                }
                if "FromPort" in perm:
                    entry["FromPort"] = perm["FromPort"]
                if "ToPort" in perm:
                    entry["ToPort"] = perm["ToPort"]
                egress_revoke.append(entry)
        if egress_revoke:
            log(f"REVOKING egress references from {sg_id}")
            try:
                ec2().revoke_security_group_egress(GroupId=sg_id, IpPermissions=egress_revoke)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in (
                    "InvalidPermission.NotFound", "InvalidGroup.NotFound"
                ):
                    raise


def delete_stack_security_groups():
    resp = ec2().describe_security_groups(
        Filters=[{"Name": "tag:Project", "Values": [_u.STACK]}]
    )
    groups = [sg for sg in resp.get("SecurityGroups", []) if sg.get("GroupName") != "default"]
    if not groups:
        return
    target_ids = [sg["GroupId"] for sg in groups]
    remove_group_references(target_ids)
    for sg in groups:
        sg_id = sg["GroupId"]
        name = sg.get("GroupName", sg_id)
        revoke_all_permissions(sg_id)
        try:
            log(f"DELETING security group {name}: {sg_id}")
            ec2().delete_security_group(GroupId=sg_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("InvalidGroup.NotFound", "DependencyViolation"):
                raise


def delete_route_table_by_name(name):
    rtb = find_rtb_by_name(name)
    if not rtb:
        return
    log(f"DELETING route table {name}: {rtb}")
    desc = ec2().describe_route_tables(RouteTableIds=[rtb])["RouteTables"][0]
    for assoc in desc.get("Associations", []):
        if not assoc.get("Main"):
            assoc_id = assoc.get("RouteTableAssociationId")
            try:
                ec2().disassociate_route_table(AssociationId=assoc_id)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "InvalidAssociationID.NotFound":
                    raise
    try:
        ec2().delete_route_table(RouteTableId=rtb)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "InvalidRouteTableID.NotFound":
            raise


def delete_stack_subnets(vpc_id):
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]},
                 {"Name": "tag:Project", "Values": [_u.STACK]}]
    )
    for subnet in resp.get("Subnets", []):
        subnet_id = subnet["SubnetId"]
        log(f"DELETING subnet: {subnet_id}")
        try:
            ec2().delete_subnet(SubnetId=subnet_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in (
                "InvalidSubnetID.NotFound", "DependencyViolation"
            ):
                raise


def delete_stack_igw_and_vpc(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"DETACHING internet gateway: {igw}")
        try:
            ec2().detach_internet_gateway(InternetGatewayId=igw, VpcId=vpc_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in (
                "Gateway.NotAttached", "InvalidInternetGatewayID.NotFound"
            ):
                raise
        log(f"DELETING internet gateway: {igw}")
        try:
            ec2().delete_internet_gateway(InternetGatewayId=igw)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidInternetGatewayID.NotFound":
                raise
    log(f"DELETING VPC: {vpc_id}")
    for attempt in range(12):
        try:
            ec2().delete_vpc(VpcId=vpc_id)
            return
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "InvalidVpcID.NotFound":
                return
            if code == "DependencyViolation" and attempt < 11:
                log("VPC deletion blocked by DependencyViolation; waiting 5s and retrying...")
                time.sleep(5)
                continue
            raise


def cleanup_stack():
    log(f"Cleanup requested for stack: {_u.STACK} in {_u.REGION}")
    vpcs = get_vpcs()
    if not vpcs:
        log("No tagged VPC found; nothing to clean up.")
        return
    terminate_stack_instances()
    delete_stack_security_groups()
    delete_route_table_by_name(f"{_u.STACK}-rtb-public")
    for vpc in vpcs:
        delete_stack_subnets(vpc["VpcId"])
        delete_stack_igw_and_vpc(vpc["VpcId"])
    log("Cleanup complete.")
