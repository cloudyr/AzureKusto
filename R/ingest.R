#' @export
ingest_from_file <- function(database, src_file, dest_table, streaming_ingest=FALSE, ...)
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


#' @export
ingest_from_url <- function(database, src, dest_table, async=FALSE, ...)
{
    src_str <- obfuscate_str(src)
    prop_list <- get_ingestion_properties(...)

    cmd <- paste(".ingest",
        if(async) "async" else NULL,
        "into table",
        dest_table,
        "(", src_str, ")",
        prop_list)
    return(cmd)
    call_kusto(database, cmd, ...)
}


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


obfuscate_str <- function(string)
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
