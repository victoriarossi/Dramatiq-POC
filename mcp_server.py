from sap_api import call_sap_erp


def sync_to_sap(data: str) -> dict:
    """Standardized tool to send data to SAP ERP."""
    print(f"[MCP] Request received. Invoking SAP tool for: {data}")
    result = call_sap_erp(data)
    if result.get("status") != "SUCCESS":
        raise RuntimeError(f"SAP returned non-success status: {result}")
    print(f"[MCP] SAP returned confirmation ID: {result['id']}")
    return result

if __name__ == "__main__":
    mcp.run()