ingest_from_file <- function(database, ...)
{
    UseMethod("ingest_from_file")
}

ingest_from_file.kusto_ingestion_endpoint <- function(database, src_file, dest_table,
    streaming_ingest=TRUE, ...)
{
    if(!streaming_ingest)
    {
        if(!requireNamespace("AzureStor"))
            stop("AzureStor package not available")

        return(ingest_from_azure(database, dest_table=dest_table, ...))
    }

}


ingest_from_azure <- function(database, ...)
{

}


ingest_from_azure.kusto_database_endpoint <- function(database, src_uri, dest_table, ...)
{

}
