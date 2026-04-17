
import base64
import json
import os
from datetime import datetime, timezone, timedelta
import boto3
import requests

# Import shared utilities (same pattern as your JS code)
from common.sf_auth import get_access_token, sf_query
from common.sf_utils import safe_json

# =====================================================
# DynamoDB setup 
# =====================================================

dynamodb = boto3.resource("dynamodb")

SUMMARY_TABLE = dynamodb.Table(
    os.environ.get("SUMMARY_TABLE", "ConnectCallTimeline")
)
ROOTMAP_TABLE = dynamodb.Table(
    os.environ.get("ROOTMAP_TABLE", "ConnectContactRootMap")
)

print(f"[INIT] SUMMARY_TABLE={SUMMARY_TABLE.name}")
print(f"[INIT] ROOTMAP_TABLE={ROOTMAP_TABLE.name}")

REGION_NAME = os.environ.get('REGION_NAME', 'us-east-1')

connect_client = boto3.client('connect', region_name=REGION_NAME)

# =====================================================
# Utilities
# =====================================================

def get_salesforce_user(username):
    try:


        ENGINEERING_CS_PROFILE_ID = os.environ.get('ENGINEERING_CS_PROFILE_ID', '')
        ENGINEERING_FA_PROFILE_ID = os.environ.get('ENGINEERING_FA_PROFILE_ID', '')
        SYSTEM_ADMINISTRATOR_PROFILE_ID = os.environ.get('SYSTEM_ADMINISTRATOR_PROFILE_ID', '')

        profile_ids = [ENGINEERING_CS_PROFILE_ID, ENGINEERING_FA_PROFILE_ID, SYSTEM_ADMINISTRATOR_PROFILE_ID]

        # Convert list ['A', 'B'] into string "('A', 'B')"
        formatted_ids = "('" + "','".join(profile_ids) + "')"

        query = f"""
        SELECT Id 
        FROM User 
        WHERE ProfileId IN {formatted_ids} AND Name = '{username}'
        LIMIT 1"""
        
        result = sf_query(query)
        print(result)
        data = result['data']

        if data['totalSize'] == 0:
            print(f"No user found for: {username} with profile ids: {formatted_ids}")
            return None

        resp = data['records'][0]
        print(resp)
        id = resp['Id']
        return id

    except Exception as e:
        print(f"Error in find_contact_and_cases_by_phone: {str(e)}")
        raise


def get_aws_connect_user(user_id, instance_id):
    try:
        response = connect_client.describe_user(
            UserId=user_id,
            InstanceId=instance_id
        )
        first_name = response['User']['IdentityInfo']['FirstName']
        last_name = response['User']['IdentityInfo']['LastName']
        name = first_name + " " + last_name
        print(f"Agent Name: {name}")
        return name
    except Exception as e:
        print(f"Error in get_aws_connect_user: {str(e)}")
        raise

def update_case_owner(case_id, owner_id):
    try:
        token_res = get_access_token()
        case_url = f"{token_res['instance_url']}/services/data/v59.0/sobjects/Case/{case_id}"
        payload = {
            "OwnerId": owner_id
        }
        response = requests.patch(
            case_url,
            headers={
                'Authorization': f"Bearer {token_res['access_token']}",
                'Content-Type': 'application/json'
            },
            json=payload
        )
        print(response.status_code)
        if response.status_code != 204:
            error_data = safe_json(response.text)
            print(error_data)
            error_msg = error_data.get('message') if error_data else f"Failed to update case owner: {response.status_code}"
            raise Exception(error_msg)
        # 204 No Content - successful update with no response body
        return {"success": True, "message": "Case owner updated successfully"}
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to Update Case owner: {str(e)}")


def update_task(ctr):
    """
    Update an existing task in Salesforce

    Args:
        task_id: Salesforce Task ID
        task_data: Dictionary with fields to update

    Returns:
        Update result
    """

    initiation_method = ctr.get("InitiationMethod")

    queue_name = deep_get(ctr, "Queue.Name")
    agent = deep_get(ctr, "Agent.Username")
    routing_profile = deep_get(ctr, "Agent.RoutingProfile.Name")
    call_start_time = deep_get(ctr, "Agent.ConnectedToAgentTimestamp")
    call_end_time = ctr.get("DisconnectTimestamp")
    call_end_time = datetime.fromisoformat(call_end_time.replace('Z', '+00:00'))
    call_duration = deep_get(ctr, "Agent.AgentInteractionDuration")
    initiation_time = ctr.get("InitiationTimestamp")
    
    # Parse timestamps
    initiation_time = datetime.fromisoformat(initiation_time.replace('Z', '+00:00'))
    connected_time = datetime.fromisoformat(call_start_time.replace('Z', '+00:00'))

    # Calculate wait time
    wait_time = connected_time - initiation_time
    wait_time_seconds = wait_time.total_seconds()

    # Call date, day and time
    call_date = connected_time.date()
    call_day = connected_time.strftime("%A")
    call_week = connected_time.strftime("%W")
    call_time = connected_time.strftime("%H:%M:%S")

    inital_time_formatted = initiation_time.strftime('%Y-%m-%dT%H:%M:%S.000+0000')
    disconnect_time_formatted = call_end_time.strftime('%Y-%m-%dT%H:%M:%S.000+0000')

    # Format for Salesforce - SEPARATE date and time
    initiation_date = initiation_time.strftime('%Y-%m-%d')      # "2026-01-22"
    initiation_time_str = initiation_time.strftime('%H:%M:%S')  # "18:09:54"
    disconnect_date = call_end_time.strftime('%Y-%m-%d')        # "2026-01-22"
    disconnect_time_str = call_end_time.strftime('%H:%M:%S')    # "18:10:14"

    # Sample agent arn: arn:aws:connect:us-east-1:201403186351:instance/6a3cd615-1a59-4c91-8667-d409b61c29bf/agent/76e5c37a-5c8b-4b58-86da-21268dddff0a
    agent_arn = deep_get(ctr, "Agent.ARN")
    agent_id = agent_arn.split('/')[-1]
    instance_id = agent_arn.split('/')[-3]
    Agent_Name_AWS = get_aws_connect_user(agent_id, instance_id)
    Owner_id = get_salesforce_user(Agent_Name_AWS)

    isNewCase = deep_get(ctr, "Attributes.isNewCaseCSE")

    task_data = {
        "Agent_Name__c": agent,
        "Queue__c": queue_name,
        "Routing_Profile__c": routing_profile,
        "Call_Start_Time__c": call_start_time,  # Already a string from CTR
        "Call_End_Time__c": disconnect_time_formatted,  # Use formatted string, not datetime object
        "Call_Duration__c": call_duration,
        "Call_Waiting_Time__c": wait_time_seconds,
        "Call_Date__c": initiation_date,  # Use date string, not date object
        "Call_Day__c": call_day,
        "Call_Week__c": call_week,
        "Initial_TimeStamp__c": inital_time_formatted,
        "Disconnect_timestamp__c": disconnect_time_formatted,
        "Call_Type__c": initiation_method
    }

    task_id = None
    case_id = None
    if initiation_method == "INBOUND":
        print("Update CSE task form")
        task_id = deep_get(ctr, "Attributes.cseTaskId")
        case_id = deep_get(ctr, "Attributes.cseCaseId")
    elif initiation_method == "TRANSFER":
        print("Update FAE task form")
        task_id = deep_get(ctr, "Attributes.faeTaskId")
        case_id = deep_get(ctr, "Attributes.faeCaseId")

    # Get access token using shared function
    token_res = get_access_token()

    if task_id:
        try:
            if Owner_id:
                task_data["OwnerId"] = Owner_id
                # update salesforce case owner also
                if isNewCase:
                    resp = update_case_owner(case_id, Owner_id)
                    print(resp)
            task_url = f"{token_res['instance_url']}/services/data/v59.0/sobjects/Task/{task_id}"
            print(task_url)
            response = requests.patch(
                task_url,
                headers={
                    'Authorization': f"Bearer {token_res['access_token']}",
                    'Content-Type': 'application/json'
                },
                json=task_data
            )
            print(response)
            if response.status_code != 204:
                error_data = safe_json(response.text)
                print(error_data)
                error_msg = error_data.get('message') if error_data else f"Failed to update task: {response.status_code}"
                raise Exception(error_msg)

            # 204 No Content - successful update with no response body
            return {"success": True, "message": "Task updated successfully"}
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to update task: {str(e)}")
    return None

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_ttl_epoch() -> int:
    """
    Returns Unix epoch timestamp 30 days from now for DynamoDB TTL.
    Changed to 1 day just for testing.
    """
    return int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp())

def deep_get(d, path):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur

def has_audio_stream(ctr):
    return any(
        isinstance(s, dict) and s.get("Type") == "AUDIO"
        for s in ctr.get("MediaStreams", [])
    )

def has_available_audio_recording(ctr):
    rec = ctr.get("Recording")
    return (
        isinstance(rec, dict)
        and rec.get("Status") == "AVAILABLE"
        and rec.get("Type") == "AUDIO"
    )

def agent_connected_ts(ctr):
    return (
        deep_get(ctr, "Agent.ConnectedToAgentTimestamp")
        or ctr.get("ConnectedToAgentTimestamp")
    )

def extract_recording_url(ctr):
    """
    Prefer Recording.Location when present.
    Otherwise, fallback to the first AVAILABLE 'Recordings[i].Location'.
    """
    url = deep_get(ctr, "Recording.Location")
    if isinstance(url, str) and url:
        return url

    recs = ctr.get("Recordings", [])
    if isinstance(recs, list):
        for r in recs:
            if not isinstance(r, dict):
                continue
            if r.get("Status") == "AVAILABLE":
                loc = r.get("Location")
                if isinstance(loc, str) and loc:
                    return loc
    return None

# =====================================================
# RootContactId resolution
# =====================================================

def get_root(contact_id):
    row = ROOTMAP_TABLE.get_item(
        Key={"ContactId": contact_id},
        ConsistentRead=True,
    ).get("Item")
    return row["RootContactId"] if row else None

def put_root(contact_id, root_id):
    ROOTMAP_TABLE.put_item(
        Item={
            "ContactId": contact_id,
            "RootContactId": root_id,
            "LastUpdated": now_utc(),
            "ExpiresAt": get_ttl_epoch(),
        }
    )

def resolve_root_contact_id(ctr):
    cid = ctr["ContactId"]

    if ctr.get("InitialContactId"):
        put_root(cid, ctr["InitialContactId"])
        return ctr["InitialContactId"]

    if not ctr.get("PreviousContactId"):
        put_root(cid, cid)
        return cid

    parent = ctr["PreviousContactId"]
    root = get_root(parent)
    if root:
        put_root(cid, root)
        return root

    put_root(cid, parent)
    return parent

# =====================================================
# Aggregation logic (3‑pass, no overlaps)
# =====================================================

def update_summary(root_id, ctr):
    contact_id = ctr["ContactId"]
    now = now_utc()

    ttl = get_ttl_epoch()

    initiation_method = ctr.get("InitiationMethod")

    task_form_id = None

    if initiation_method == "INBOUND":
        task_form_id = deep_get(ctr, "Attributes.cseTaskId")
        case_id = deep_get(ctr, "Attributes.cseCaseId")
    elif initiation_method == "TRANSFER":
        task_form_id = deep_get(ctr, "Attributes.faeTaskId")
        case_id = deep_get(ctr, "Attributes.faeCaseId")

    queue_name = deep_get(ctr, "Queue.Name")
    agent = deep_get(ctr, "Agent.Username")
    customer_endpoint = deep_get(ctr, "CustomerEndpoint.Address")

    disconnect_ts = ctr.get("DisconnectTimestamp")
    disconnect_reason = ctr.get("DisconnectReason")
    transfer_completed_ts = ctr.get("TransferCompletedTimestamp")

    agent_join_ts = agent_connected_ts(ctr)

    # Set lastLeave whenever this agent's CTR has a DisconnectTimestamp (root or child)
    agent_leave_ts = disconnect_ts if agent and disconnect_ts else None

    # Recording URL (Recording.Location preferred, otherwise first AVAILABLE in Recordings[])
    recording_url = extract_recording_url(ctr)
    # =================================================
    # PASS 1 — Ensure top-level maps
    # =================================================

    SUMMARY_TABLE.update_item(
        Key={"RootContactId": root_id},
        UpdateExpression=(
            "SET #legs = if_not_exists(#legs, :m), "
            "#agents = if_not_exists(#agents, :m), "
            "ExpiresAt = if_not_exists(ExpiresAt, :ttl)"
        ),
        ExpressionAttributeNames={
            "#legs": "ContactLegs",
            "#agents": "Agents",
        },
        ExpressionAttributeValues={":m": {}, ":ttl": ttl},
    )

    # =================================================
    # PASS 2A — Ensure per-leg and per-agent parent maps
    # =================================================

    expr = ["#legs.#leg = if_not_exists(#legs.#leg, :m)"]
    names = {"#legs": "ContactLegs", "#leg": contact_id}
    values = {":m": {}}

    if agent:
        expr.append("#agents.#a = if_not_exists(#agents.#a, :m)")
        names["#agents"] = "Agents"
        names["#a"] = agent

    SUMMARY_TABLE.update_item(
        Key={"RootContactId": root_id},
        UpdateExpression="SET " + ", ".join(expr),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )

    # =================================================
    # PASS 2B — Write child attributes only (safe)
    # =================================================

    expr = []
    names = {"#legs": "ContactLegs", "#leg": contact_id}
    values = {
        ":now": now,
        ":emptyList": [],
        ":thisLeg": [contact_id],
    }

    # ---- ContactLegs (per-leg facts)
    expr.append("#legs.#leg.LastUpdatedTs = :now")

    if initiation_method:
        expr.append("#legs.#leg.InitiationMethod = :im")
        values[":im"] = initiation_method

    if queue_name:
        expr.append("#legs.#leg.QueueName = :qn")
        values[":qn"] = queue_name

    if agent:
        expr.append("#legs.#leg.AgentUsername = :agent")
        values[":agent"] = agent

    if customer_endpoint:
        expr.append("#legs.#leg.CustomerEndpoint = :cust")
        values[":cust"] = customer_endpoint

    if disconnect_ts:
        expr.append("#legs.#leg.DisconnectTimestamp = :dts")
        values[":dts"] = disconnect_ts

    if disconnect_reason:
        expr.append("#legs.#leg.DisconnectReason = :dr")
        values[":dr"] = disconnect_reason

    # ✅ Recording URL (set once; change to plain assignment if you prefer latest-wins)
    if recording_url:
        expr.append("#legs.#leg.RecordingUrl = if_not_exists(#legs.#leg.RecordingUrl, :rurl)")
        values[":rurl"] = recording_url

    # Transfer completion belongs to INBOUND leg (source)
    if initiation_method == "INBOUND" and transfer_completed_ts:
        expr.append("#legs.#leg.TransferCompletedTimestamp = :tcts")
        values[":tcts"] = transfer_completed_ts

    # ---- Agents (per-agent timeline)
    if agent:
        names["#agents"] = "Agents"
        names["#a"] = agent

        expr.append("#agents.#a.LastUpdatedTs = :now")

        expr.append(
            "#agents.#a.contactIds = "
            "list_append(if_not_exists(#agents.#a.contactIds, :emptyList), :thisLeg)"
        )

        if task_form_id:
            expr.append("#agents.#a.TaskFormId = :tfid")
            values[":tfid"] = task_form_id

        if case_id:
            expr.append("#agents.#a.CaseId = :caseid")
            values[":caseid"] = case_id

        if agent_join_ts:
            expr.append("#agents.#a.firstJoin = if_not_exists(#agents.#a.firstJoin, :jts)")
            values[":jts"] = agent_join_ts

        if agent_leave_ts:
            expr.append("#agents.#a.lastLeave = :lts")
            values[":lts"] = agent_leave_ts

    # ---- Root (set-once fields at the call level)
    # START: set once from root InitiationTimestamp
    if not ctr.get("PreviousContactId") and ctr.get("InitiationTimestamp"):
        expr.append("CallStartedTs = if_not_exists(CallStartedTs, :start)")
        values[":start"] = ctr["InitiationTimestamp"]

    # END: set once from root DisconnectTimestamp
    if not ctr.get("PreviousContactId") and ctr.get("DisconnectTimestamp"):
        expr.append("CallEndedTs = if_not_exists(CallEndedTs, :end)")
        values[":end"] = ctr["DisconnectTimestamp"]

    expr.append("LastUpdated = :now")

    SUMMARY_TABLE.update_item(
        Key={"RootContactId": root_id},
        UpdateExpression="SET " + ", ".join(expr),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )

# =====================================================
# Lambda entry point
# =====================================================

def lambda_handler(event, context):
    
    for r in event.get("Records", []):
        try:
            payload = json.loads(base64.b64decode(r["kinesis"]["data"]))
            ctr = payload.get("Event", payload)

            if not isinstance(ctr, dict) or "ContactId" not in ctr:
                continue

            # All CTRs must be voice/audio
            if not has_audio_stream(ctr):
                continue

            # Root CTR requires recording; child legs do not
            is_child = bool(ctr.get("PreviousContactId"))
            if not is_child and not has_available_audio_recording(ctr):
                continue

            print(f"[START] Records={len(event.get('Records', []))}")
            print(f"CTR Event: {event}")

            root_id = resolve_root_contact_id(ctr)
            update_summary(root_id, ctr)
            resp = update_task(ctr)
            print(f"Task updated: {resp}")

        except Exception as e:
            print("[ERROR]", e)

    print("[END] Lambda completed")
    return {"status": "OK"}
