import time
import random

def call_sap_erp(request_id):
    print(f"[SAP] Request received for ID: {request_id}")

    # Random wait between 0 and 360 seconds (0–6 minutes)
    wait_time = random.randint(0, 60)
    print(f"[SAP] Simulated execution time: {wait_time}s. Executing...")

    fail = random.randint(1,4)
    if fail == 1:
        print(f"[SAP] Execution FAILED for ID: {request_id}")
        raise RuntimeError(f"SAP execution failed for ID: {request_id}")

    time.sleep(wait_time)
    print(f"[SAP] Execution complete for ID: {request_id}")
    return {"status": "SUCCESS", "id": request_id, "duration": wait_time}