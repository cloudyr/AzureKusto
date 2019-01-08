#' @export
ade_query_endpoint <- function(..., .connection_string=NULL, .azure_token=NULL)
{
    props <- list(...)
    names(props) <- tolower(names(props))

    if(!is.null(.connection_string))
    {
        # simplified connection string handling: ignores quotes
        conn_props <- strsplit(.connection_string, ";")[[1]]
        names(conn_props) <- tolower(sapply(conn_props, function(x) sub("=.+$", "", x)))
        conn_props <- lapply(conn_props, function(x) sub("^[^=]+=", "", x))

        props <- utils::modifyList(props, conn_props)
    }

    # TODO: normalize all property names, values

    if(!is.null(.azure_token))
    {
        if(!(inherits(.azure_token, "AzureToken") && inherits(.azure_token, "R6")))
            stop(".azure_token should be an object of class AzureRMR::AzureToken")

        props$token <- .azure_token
    }
    else
    {
        props$fed <- as.logical(props$fed)
        if(!isTRUE(props$fed))
        {
            warning("Only AAD federated authentication supported at this time", call.=FALSE)
            props$fed <- TRUE
        }

        url <- httr::parse_url(props$server)
        subnames <- strsplit(url$host, ".", fixed=TRUE)[[1]]
        props$token <- get_ade_token(cluster=subnames[1], location=subnames[2], tenant=props$tenantid)
    }

    class(props) <- "ade_database_endpoint"
    props
}


