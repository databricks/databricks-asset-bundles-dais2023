import sys

import dlt
from databricks.sdk import WorkspaceClient

def main():
    import get_metrics
    import ingest

    pipeline_id = sys.argv[1]
    update_id = sys.argv[2]

    w = WorkspaceClient(host="https://e2-demo-west.cloud.databricks.com/")
    events = w.pipelines.list_pipeline_events(
        pipeline_id=pipeline_id,
    )

    for event in events:
        # Focus on most recent update.
        if event.origin.update_id != update_id:
            break

        if event.event_type != 'flow_progress':
            continue

        flow_name = event.origin.flow_name
        flow = dlt.get_flow(flow_name)

        flow_progress = event.details['flow_progress']
        flow_status = flow_progress['status']

        if flow_status == 'COMPLETED':
            data_quality = flow_progress['data_quality']

            if not 'expectations' in data_quality:
                continue

            for exp in data_quality['expectations']:
                passed_records = exp['passed_records']
                failed_records = exp['failed_records']
                total_records = passed_records + failed_records
                name = exp['name']
                if failed_records == 0:
                    message = f"Expectation '{name}' passed for {passed_records} records"
                    print(f"::notice file={flow.relpath},line={flow.lineno}::{message}")
                else:
                    message = f"Expectation '{name}' failed for {failed_records}/{total_records} records"
                    print(f"::warning file={flow.relpath},line={flow.lineno}::{message}")

        if flow_status == 'FAILED':
            message = f"Flow '{flow_name}' failed"
            print(f"::error file={flow.relpath},line={flow.lineno}::{message}")


if __name__ == '__main__':
    main()
