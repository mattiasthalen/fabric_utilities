import os

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential

def get_access_token(
    audience: str
) -> str:
    """
    Get an access token for the specified audience.
    
    Attempts to use Fabric's notebookutils if available (when running in a Fabric notebook),
    otherwise falls back to DefaultAzureCredential for local/other environments.
    
    Args:
        audience: The target audience for the token (e.g., "https://storage.azure.com")
        
    Returns:
        Access token string
        
    Raises:
        Azure authentication errors if credential acquisition fails
    """
    try:
        import notebookutils # type: ignore

        token: str = notebookutils.credentials.getToken(audience)

    except ModuleNotFoundError:
        token: str = DefaultAzureCredential().get_token(f"{audience}/.default").token
    
    return token

def get_azure_storage_access_token() -> str:
    """
    Get an access token for Azure Storage operations.
    
    First checks for AZURE_STORAGE_TOKEN environment variable, then attempts
    to acquire a token programmatically using available credential methods.
    
    Returns:
        Access token string for Azure Storage
        
    Raises:
        ClientAuthenticationError: If authentication fails, with additional
            troubleshooting guidance for common issues
    """
    token: str | None = os.environ.get("AZURE_STORAGE_TOKEN")

    if token:
        return token
    
    try:
        audience: str = "https://storage.azure.com"
        return get_access_token(audience)
    
    except ClientAuthenticationError as e:
        raise ClientAuthenticationError(
            f"{str(e)}\n\n"
            "Additional troubleshooting steps:\n"
            "1. Ensure you can use any of the credentials methods to get an access token\n"
            "2. Set the `AZURE_STORAGE_TOKEN` environment variable with a valid access token"
        ) from e

def get_storage_options(
        table_uri: str
) -> dict | None:
    """
    Get storage options for Delta Lake operations with Fabric.
    
    Returns configured storage options for Fabric endpoints (abfss://) 
    with authentication and Fabric-specific settings. Returns None for 
    non-Azure URIs to use default Polars behavior.
    
    Args:
        table_uri: URI of the target table/file (e.g., "abfss://...")
        
    Returns:
        Dictionary of storage options for Fabric, or None for other URIs
        
    Raises:
        ClientAuthenticationError: If Azure authentication fails
    """
    storage_options: dict | None = {
        "allow_unsafe_rename": "true",
        "use_fabric_endpoint": "true",
        "bearer_token": get_azure_storage_access_token(),
        "allow_invalid_certificates": "true"
    }

    return storage_options if table_uri.startswith("abfss://") else None