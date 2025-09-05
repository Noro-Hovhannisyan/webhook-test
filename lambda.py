import json
import boto3
import os

# AWS clients
secrets_client = boto3.client("secretsmanager")
codepipeline = boto3.client("codepipeline")

def get_project_token(project):
    """
    Retrieve GitLab webhook token from Secrets Manager
    """
    secret_name = f"gitlab/{project}/token"
    print(f"Looking up secret: {secret_name}")
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response["SecretString"]
    except secrets_client.exceptions.ResourceNotFoundException:
        return None

def verify_gitlab_signature(payload, headers):
    """
    Verify GitLab webhook secret
    """
    headers_lower = {k.lower(): v for k, v in headers.items()}
    token = headers_lower.get("x-gitlab-token", "")

    project = payload.get("project", {}).get("path_with_namespace")
    if not project:
        return False, "No project info in payload"

    expected_token = get_project_token(project)
    if not expected_token:
        return False, f"No token registered for project {project}"

    if token != expected_token:
        return False, "Invalid secret token"

    return True, None

def trigger_pipeline(pipeline_name, branch_name):
    """
    Trigger CodePipeline execution using a branch override via pipeline variables.
    """
    response = codepipeline.start_pipeline_execution(
        name=pipeline_name,
        variables=[
            {"name": "SOURCE_BRANCH", "value": branch_name}
        ]
    )
    print(f"Triggered CodePipeline {pipeline_name} for branch {branch_name}")
    return response


def lambda_handler(event, context):
    body = event.get("body", "{}")
    headers = event.get("headers", {})

    # Parse webhook JSON
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON payload"})}

    # Verify secret token
    is_valid, error_msg = verify_gitlab_signature(payload, headers)
    if not is_valid:
        return {"statusCode": 403, "body": json.dumps({"error": error_msg})}

    # Only handle merge request events
    if payload.get("object_kind") != "merge_request":
        return {"statusCode": 200, "body": json.dumps({"message": "Not a merge request event"})}

    # Extract labels and source branch
    labels = payload.get("labels", [])
    label_titles = [label["title"].lower() for label in labels]
    print("Labels extracted from merge request:", label_titles)

    source_branch = payload.get("object_attributes", {}).get("source_branch")
    print("MR source branch:", source_branch)

    # Load label -> pipeline mapping from environment variable
    # Example: {"q1": "PipelineQ1", "q2": "PipelineQ2"}
    label_pipeline_mapping_str = os.environ.get("LABEL_PIPELINE_MAPPING", "{}")
    try:
        label_pipeline_mapping = json.loads(label_pipeline_mapping_str)
    except json.JSONDecodeError:
        return {"statusCode": 500, "body": json.dumps({"error": "Invalid LABEL_PIPELINE_MAPPING env var"})}

    triggered_pipelines = []

    # Trigger pipelines for labels
    for label in label_titles:
        pipeline_name = label_pipeline_mapping.get(label)
        if pipeline_name:
            response = trigger_pipeline(pipeline_name, source_branch)
            triggered_pipelines.append({
                "label": label,
                "pipeline": pipeline_name,
                "execution_id": response["pipelineExecutionId"]
            })

    if not triggered_pipelines:
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No matching label for pipeline, skipped"}),
            "headers": {"Content-Type": "application/json"}
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"triggered_pipelines": triggered_pipelines}),
        "headers": {"Content-Type": "application/json"}
    }
