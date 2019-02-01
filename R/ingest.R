#' Ingestion functions for Kusto
#'
#' @param database A Kusto database endpoint object, created with [kusto_database_endpoint].
#' @param src The source file or URL.
#' @param dest_table The name of the destination table.
#' @param key,token,sas Authentication arguments for the Azure storage ingestion methods. If multiple arguments are supplied, a key takes priority over a token, which takes priority over a SAS.
#' @param async For the URL ingestion functions, whether to do the ingestion asychronously. If TRUE, the function will return immediately while the server handles the operation in the background.
#' @param streaming_ingest For file ingestion, whether to upload the file directly to the ingestion endpoint. If FALSE (the default), the file is instead uploaded to blob storage, and ingested from there.
#' @param ... Named arguments to be treated as ingestion parameters.
#'
#' @rdname ingest
#' @export
ingest_from_file <- function(database, src, dest_table, streaming_ingest=FALSE, ...)
{
    if(!streaming_ingest)
    {
        # upload to blob and ingest from there
        if(!requireNamespace("AzureStor"))
            stop("AzureStor package not available")

        return(ingest_from_blob(database, stor, cont, file, dest_table=dest_table, key=key...))
    }
    stop("not yet implemented")
}


#' @rdname ingest
#' @export
ingest_from_url <- function(database, src, dest_table, async=FALSE, ...)
{
    prop_list <- get_ingestion_properties(...)

    cmd <- paste(".ingest",
        if(async) "async" else NULL,
        "into table",
        dest_table,
        "(", obfuscate_string(src), ")",
        prop_list)

    call_kusto(database, cmd, ...)
}


#' @rdname ingest
#' @export
ingest_from_blob <- function(database, src, key=NULL, token=NULL, sas=NULL,
                             dest_table, async=FALSE, ...)
{
    if(!is.null(key))
        src <- paste0(src, ";", key)
    else if(!is.null(token))
        src <- paste0(src, ";token=", validate_token(token))
    else if(!is.null(sas))
        src <- paste0(src, "?", sas)

    ingest_from0_url(database, src, dest_table, async, ...)
}


#' @rdname ingest
#' @export
ingest_from_adls2 <- function(database, src, key=NULL, token=NULL, sas=NULL,
                              dest_table, async=FALSE, ...)
{
    if(!is.null(key))
        src <- paste0(src, ";sharedkey=", key)
    else if(!is.null(token))
        src <- paste0(src, ";token=", validate_token(token))
    else if(!is.null(sas))
        stop("ADLSgen2 does not support use of shared access signatures")
    else src <- paste0(src, ";impersonate")

    ingest_from0_url(database, src, dest_table, async, ...)
}


obfuscate_string <- function(string)
{
    paste0("h'", string, "'")
}


get_ingestion_properties <- function(...)
{
    props <- list(...)
    if(is_empty(props))
        return(NULL)

    prop_list <- mapply(function(name, value)
    {
        if(is.character(value))
            value <- shQuote(value, type="sh")
        else if(is.logical(value))
            value <- tolower(as.character(value))
        paste(name, value, sep="=")
    }, names(props), props)

    paste("with (", paste(prop_list, collapse=", "), ")")
}
