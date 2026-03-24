#!/usr/bin/env bash
set -euo pipefail

NEW_TYPE="${1:?Usage: $0 <new-instance-type>  e.g. db.r6i.24xlarge}"
PROFILE="sandbox-storage"
REGION="us-east-1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_FILE="$SCRIPT_DIR/aurora-bench-state.json"
KEY="$SCRIPT_DIR/aurora-bench-key.pem"

WRITER_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['writer_id'])")

echo "[$(date -u +%H:%M:%S)] Modifying $WRITER_ID -> $NEW_TYPE ..."
aws rds modify-db-instance \
    --db-instance-identifier "$WRITER_ID" \
    --db-instance-class "$NEW_TYPE" \
    --apply-immediately \
    --profile "$PROFILE" \
    --region "$REGION" \
    --output text --query 'DBInstance.DBInstanceClass' 2>&1

echo "[$(date -u +%H:%M:%S)] Waiting for instance to become available (10-15 min)..."
aws rds wait db-instance-available \
    --db-instance-identifier "$WRITER_ID" \
    --profile "$PROFILE" \
    --region "$REGION"

ACTUAL=$(aws rds describe-db-instances \
    --db-instance-identifier "$WRITER_ID" \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query 'DBInstances[0].DBInstanceClass' --output text)
echo "[$(date -u +%H:%M:%S)] Instance is now: $ACTUAL"

python3 -c "
import json
with open('$STATE_FILE') as f:
    d = json.load(f)
d['aurora_instance_type'] = '$ACTUAL'
with open('$STATE_FILE', 'w') as f:
    json.dump(d, f, indent=2)
print(f'Updated state file: aurora_instance_type={d[\"aurora_instance_type\"]}')
"

ENDPOINT=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['endpoint'])" 2>/dev/null || echo "")
EC2_IP=$(python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d['client_public_ip'])" 2>/dev/null || echo "")
if [[ -n "$EC2_IP" && -n "$ENDPOINT" && -f "$KEY" ]]; then
    echo "[$(date -u +%H:%M:%S)] Refreshing /etc/hosts on EC2 client..."
    ssh -i "$KEY" -o StrictHostKeyChecking=no ec2-user@"$EC2_IP" bash -c "'
        sudo sed -i \"/$ENDPOINT/d\" /etc/hosts
        IP=\$(dig +short $ENDPOINT | tail -1)
        echo \"\$IP $ENDPOINT\" | sudo tee -a /etc/hosts
        echo \"Updated /etc/hosts: \$IP -> $ENDPOINT\"
    '" 2>&1
fi
