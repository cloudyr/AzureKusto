# Azure Active Directory app used to talk to Kusto
.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'


#' Manage AAD authentication tokens for Kusto clusters
#'
#' @param server The URI of your Kusto cluster. If not supplied, it is obtained from the `clustername` and `location` arguments.
#' @param clustername The cluster name.
#' @param location The cluster location. Leave this blank for a Microsoft-internal Kusto cluster like "help".
#' @param tenant Your Azure Active Directory (AAD) tenant. Can be a GUID, a name ("myaadtenant") or a fully qualified domain name ("myaadtenant.com").
#' @param hash For `delete_kusto_tenant`, the MD5 hash of the token. This is used to identify the token if provided.
#'
#' @details
#' These functions are for working with authentication tokens obtained from the main KustoClient Active Directory app. This app can be used to authenticate with any Kusto cluster (assuming, of course, you have the proper credentials).
#'
#' `get_kusto_token` returns an authentication token for the given cluster, caching its value on disk. `delete_kusto_token` deletes a cached token, and `list_kusto_tokens` lists all cached tokens.
#'
#' @return
#' `get_kusto_token` returns an object of class AzureRMR::AzureToken representing the authentication token, while `list_kusto_tokens` returns a list of such objects. `delete_azure_token` returns NULL on a successful delete.
#' @export
get_kusto_token <- function(server=NULL, clustername, location=NULL, tenant)
{
    tenant <- AzureRMR::normalize_tenant(tenant)
    if(is.null(server))
    {
        location <- normalize_location(location)
        cluster <- normalize_cluster(clustername, location)
        server <- paste0("https://", cluster, ".kusto.windows.net")
    }

    AzureRMR::get_azure_token(resource=server,
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        aad_host="https://login.microsoftonline.com/")
}


#' @export
delete_kusto_token <- function(server=NULL, clustername, location=NULL, tenant, hash=NULL, confirm=TRUE)
{
    # use hash if provided
    if(!is.null(hash))
        return(AzureRMR::delete_azure_token(hash=hash, confirm=confirm))

    tenant <- AzureRMR::normalize_tenant(tenant)
    if(is.null(server))
    {
        location <- normalize_location(location)
        cluster <- normalize_cluster(clustername, location)
        server <- paste0("https://", cluster, ".kusto.windows.net")
    }

    AzureRMR::delete_azure_token(resource=server,
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        aad_host="https://login.microsoftonline.com/",
        hash=hash, confirm=confirm)
}


#' @export
list_kusto_tokens <- function()
{
    lst <- AzureRMR::list_azure_tokens()

    is_kusto <- sapply(lst, function(tok)
        grepl("kusto.windows.net", tok$credentials$resource, fixed=TRUE))

    lst[is_kusto]
}


# Kusto prettifies location eg "West US" instead of "westus", unprettify it to be on the safe side
normalize_location <- function(location)
{
    if(is.null(location))
        return(NULL)
    else tolower(gsub(" ", "", location))
}


normalize_cluster <- function(clustername, location=NULL)
{
    if(is.null(location))
        clustername
    else paste(clustername, location, sep=".")
}


