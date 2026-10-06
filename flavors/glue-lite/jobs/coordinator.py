"""Claims between runs (migration step 2).

Runs of different groups, or several shards of one group, may overlap in
time. Three kinds of claim keep them from stepping on each other; each is a
conditional write that expires on its own, so a run that dies never blocks
the next one for longer than the claim's lifetime.

  group run     one run per (group, shard) at a time; a second run starting
                while the first holds it exits quietly (overlapping schedules)
  table         one run works on a table at a time (operation: scan or act)
  housekeeping  one run at a time does the store's retention pass

Backends:

  LocalCoordinator   grants everything (single runs, tests, no DynamoDB)
  DynamoCoordinator  conditional PutItem / DeleteItem on one DynamoDB table
                     (pk, sk strings); Floci's DynamoDB on the homelab, real
                     DynamoDB on AWS. The table is created if missing.

Items (pk / sk):

  <target>#group#<group>    run#<i>/<n>         run_id, expires_at, started_at
  <target>#table#<table>    claim#<operation>   run_id, expires_at, claimed_at
  <target>#housekeeping     claim               run_id, expires_at, last_done
"""
import os
import time


def now_s():
    return int(time.time())


class Coordinator:
    def __init__(self, target, run_id):
        self.target, self.run_id = target, run_id
        self.stats = {"group": None, "tables_claimed": 0, "tables_refused": 0}

    # implemented by backends
    def _acquire(self, pk, sk, ttl_s, extra=None):
        raise NotImplementedError

    def _release(self, pk, sk, extra=None):
        """Free a claim this run holds (expires_at = 0), setting `extra` fields."""
        raise NotImplementedError

    def _get(self, pk, sk):
        return None

    # claims
    def claim_group(self, group, shard, ttl_s=3 * 3600):
        ok = self._acquire(f"{self.target}#group#{group}", f"run#{shard}", ttl_s,
                           {"started_at": now_s()})
        self.stats["group"] = (group, shard, ok)
        return ok

    def release_group(self, group, shard):
        self._release(f"{self.target}#group#{group}", f"run#{shard}")

    def claim_table(self, table, operation="scan", ttl_s=1800):
        ok = self._acquire(f"{self.target}#table#{table}", f"claim#{operation}", ttl_s,
                           {"claimed_at": now_s()})
        self.stats["tables_claimed" if ok else "tables_refused"] += 1
        return ok

    def release_table(self, table, operation="scan"):
        self._release(f"{self.target}#table#{table}", f"claim#{operation}")

    def claim_housekeeping(self, every_hours=6, ttl_s=3600):
        """True when this run should do the retention pass: nobody else is
        doing it and it hasn't been done in the last every_hours."""
        pk, sk = f"{self.target}#housekeeping", "claim"
        item = self._get(pk, sk) or {}
        last = int(item.get("last_done") or 0)
        if last and now_s() - last < every_hours * 3600:
            return False
        return self._acquire(pk, sk, ttl_s, {"last_done": last})

    def housekeeping_done(self):
        """Record when the pass finished and free the claim."""
        self._release(f"{self.target}#housekeeping", "claim", {"last_done": now_s()})


class LocalCoordinator(Coordinator):
    """Grants every claim (one run at a time, tests)."""

    def __init__(self, target="default", run_id="local"):
        super().__init__(target, run_id)
        self.items = {}

    def _acquire(self, pk, sk, ttl_s, extra=None):
        cur = self.items.get((pk, sk))
        if cur and cur["run_id"] != self.run_id and cur["expires_at"] > now_s():
            return False
        self.items[(pk, sk)] = dict(extra or {}, run_id=self.run_id, expires_at=now_s() + ttl_s)
        return True

    def _release(self, pk, sk, extra=None):
        cur = self.items.get((pk, sk))
        if cur and cur["run_id"] == self.run_id:
            cur.update(extra or {}, expires_at=0)

    def _get(self, pk, sk):
        return self.items.get((pk, sk))


class DynamoCoordinator(Coordinator):
    def __init__(self, target, run_id, table="advisor-coordination", endpoint=None, region=None,
                 client=None):
        super().__init__(target, run_id)
        self.table = table
        if client is None:
            import boto3
            client = boto3.client(
                "dynamodb", region_name=region or os.environ.get("AWS_REGION", "us-east-1"),
                endpoint_url=endpoint or os.environ.get("GL_AWS_ENDPOINT") or None)
        self.db = client
        self._ensure_table()

    def _ensure_table(self):
        try:
            self.db.describe_table(TableName=self.table)
        except self.db.exceptions.ResourceNotFoundException:
            try:
                self.db.create_table(
                    TableName=self.table, BillingMode="PAY_PER_REQUEST",
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                          {"AttributeName": "sk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                               {"AttributeName": "sk", "KeyType": "RANGE"}])
            except self.db.exceptions.ResourceInUseException:
                pass                                   # another run created it first
            self.db.get_waiter("table_exists").wait(TableName=self.table)

    @staticmethod
    def _av(v):
        return {"N": str(v)} if isinstance(v, (int, float)) and not isinstance(v, bool) else {"S": str(v)}

    def _acquire(self, pk, sk, ttl_s, extra=None):
        item = {"pk": {"S": pk}, "sk": {"S": sk}, "run_id": {"S": self.run_id},
                "expires_at": {"N": str(now_s() + ttl_s)}}
        for k, v in (extra or {}).items():
            item[k] = self._av(v)
        try:
            self.db.put_item(
                TableName=self.table, Item=item,
                ConditionExpression="attribute_not_exists(pk) OR expires_at < :now OR run_id = :me",
                ExpressionAttributeValues={":now": {"N": str(now_s())}, ":me": {"S": self.run_id}})
            return True
        except self.db.exceptions.ConditionalCheckFailedException:
            return False

    def _release(self, pk, sk, extra=None):
        sets, vals = ["expires_at = :zero"], {":zero": {"N": "0"}, ":me": {"S": self.run_id}}
        for i, (k, v) in enumerate((extra or {}).items()):
            sets.append(f"{k} = :x{i}")
            vals[f":x{i}"] = self._av(v)
        try:      # expire it rather than delete, so a housekeeping item keeps last_done
            self.db.update_item(
                TableName=self.table, Key={"pk": {"S": pk}, "sk": {"S": sk}},
                UpdateExpression="SET " + ", ".join(sets),
                ConditionExpression="run_id = :me", ExpressionAttributeValues=vals)
        except self.db.exceptions.ConditionalCheckFailedException:
            pass                                       # taken over after it expired: not ours

    def _get(self, pk, sk):
        got = self.db.get_item(TableName=self.table, Key={"pk": {"S": pk}, "sk": {"S": sk}},
                               ConsistentRead=True).get("Item")
        if not got:
            return None
        return {k: (int(v["N"]) if "N" in v else v.get("S")) for k, v in got.items()}


def make(profile, run_id):
    """The coordinator a profile asks for (default: local)."""
    c = (profile or {}).get("coordination") or {}
    target = (profile or {}).get("target", "default")
    if c.get("backend") == "dynamodb":
        return DynamoCoordinator(target, run_id, table=c.get("table", "advisor-coordination"),
                                 endpoint=c.get("endpoint"), region=c.get("region"))
    return LocalCoordinator(target, run_id)
