import time
import random

def call_sap_erp(request_id):
    print(f"[SAP] Request received for ID: {request_id}")

    # Random wait between 0 and 360 seconds (0–6 minutes)
    wait_time = random.randint(0, 360)
    print(f"[SAP] Simulated execution time: {wait_time}s. Executing...")

    fail = random.randint(1,100)
    if fail == 100:
        print(f"[SAP] Execution FAILED for ID: {request_id}")
        return {"status": "ERROR", "id": request_id, "duration": 0}
    else:
        time.sleep(wait_time)

        print(f"[SAP] Execution complete for ID: {request_id}")
        return {"status": "SUCCESS", "id": request_id, "duration": wait_time}