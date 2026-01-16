# Databricks notebook source

from databricks.sdk import WorkspaceClient

client = WorkspaceClient()
print("Workspace Client Information")

print("Prod Whoami")
whoami = client.current_user.me()
print(whoami)

print("Successfully set up environment variables and workspace clients")


def get_policies_by_name(client: WorkspaceClient):
    return {p.name: p for p in client.cluster_policies.list()}


def summarize_diff(dev_def, target_def):
    try:
        dev_json = json.loads(dev_def)
        target_json = json.loads(target_def)

        diff = DeepDiff(target_json, dev_json, ignore_order=True)

        if not diff:
            return []

        changed_fields = set()

        for section in ("values_changed", "dictionary_item_added", "dictionary_item_removed"):
            if section in diff:
                for key in diff[section]:
                    # Format: "root['spark_version']" → spark_version
                    cleaned = key.replace("root['", "").replace("']", "")
                    changed_fields.add(cleaned)

        return sorted(list(changed_fields))

    except Exception:
        # If parsing fails, treat whole definition as changed
        return ["<entire policy definition>"]


def sync_policies(dev_client, target_client, dry_run=True):
    dev_policies = get_policies_by_name(dev_client)
    target_policies = get_policies_by_name(target_client)
    
    print(target_policies)

    env = target_client.config.host

    print(f"\n--- Syncing DEV → {env} | DRY RUN = {dry_run} ---\n")

    for name, dev_policy in dev_policies.items():
        dev_def = dev_policy.definition

        if name not in target_policies:
            print(f"[CREATE] {name}")
            if not dry_run:
                target_client.cluster_policies.create(
                    name=name,
                    definition=dev_def
                )
            continue

        # Compare existing policies
        target_policy = target_policies[name]
        target_def = target_policy.definition

        changed_fields = summarize_diff(dev_def, target_def)

        if changed_fields:
            print(f"[UPDATE] {name}  (changed fields: {', '.join(changed_fields)})")
            if not dry_run:
                target_client.cluster_policies.edit(
                    policy_id=target_policy.policy_id,
                    name=name,
                    definition=dev_def
                )
        else:
            print(f"[NO CHANGE] {name}")


# sync_policies(dev_client, poc_client, dry_run=False)
# sync_policies(dev_client, test_client, dry_run=False)
# sync_policies(dev_client, prod_client, dry_run=True)




# COMMAND ----------

get_policies_by_name(client)
