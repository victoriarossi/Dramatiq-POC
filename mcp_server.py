from fastmcp import FastMCP
from sap_api import call_sap_erp

mcp = FastMCP("SAP-Orchestrator")

@mcp.tool()
def sync_to_sap(data: str) -> dict:
    """Standardized tool to send data to SAP ERP."""
    print(f"[MCP] Request received. Invoking SAP tool for: {data}")
    result = call_sap_erp(data)
    print(f"[MCP] SAP returned confirmation ID: {result['id']}")
    return result

if __name__ == "__main__":
    mcp.run()