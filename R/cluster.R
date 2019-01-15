# Azure Active Directory app used to talk to Kusto
.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'


#' Manage AAD authentication tokens for Kusto clusters
#'
#' @param cluster The cluster name.
#' @param location The cluster location. Leave this blank for a Microsoft-internal Kusto cluster like "help".
#' @param tenant Your Azure Active Directory (AAD) tenant. Can be a GUID, a name ("myaadtenant") or a fully qualified domain name ("myaadtenant.com").
#'
#' @details
#' These functions are for working with authentication tokens obtained from the main KustoClient Active Directory app. This app can be used to authenticate with any Kusto cluster (assuming, of course, you have the proper credentials).
#'
#' @return
#' `get_kusto_token` returns an object of class AzureRMR::AzureToken representing the authentication token. `delete_azure_token` returns NULL on a successful delete.
#' @export
get_kusto_token <- function(cluster, location=NULL, tenant)
{
    tenant <- AzureRMR::normalize_tenant(tenant)
    location <- normalize_location(location)
    cluster <- normalize_cluster(cluster, location)
    server <- paste0("https://", cluster, ".kusto.windows.net/")

    AzureRMR::get_azure_token(resource=server,
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        aad_host="https://login.microsoftonline.com/")
}


#' @export
delete_kusto_token <- function(cluster, location=NULL, tenant, confirm=TRUE)
{
    tenant <- AzureRMR::normalize_tenant(tenant)
    location <- normalize_location(location)
    cluster <- normalize_cluster(cluster, location)
    if(confirm && interactive())
    {
        yn <- readline(paste0("Do you really want to delete the authentication token for Kusto cluster ",
            cluster, "? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    AzureRMR::delete_azure_token(resource=server,
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        aad_host="https://login.microsoftonline.com/", confirm=FALSE)
}


#' @export
list_kusto_tokens <- function()
{
    lst <- AzureRMR::list_azure_tokens()

    is_kusto <- sapply(lst, function(tok)
        grepl("kusto.windows.net", tok$params$user_params$resource, fixed=TRUE))

    lst[is_kusto]
}


# Kusto prettifies location eg "West US" instead of "westus", unprettify it to be on the safe side
normalize_location <- function(location)
{
    tolower(gsub(" ", "", location))
}


normalize_cluster <- function(cluster, location=NULL)
{
    if(is.null(location))
        cluster
    else paste(cluster, location, sep=".")
}


