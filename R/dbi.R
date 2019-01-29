## subclasses

setOldClass("kusto_database_endpoint")

#' @export
setClass("AzureKustoDriver", contains="DBIDriver")

#' @export
setClass("AzureKustoConnection",
         contains=c("DBIConnection", "kusto_database_endpoint"))

#' @export
setClass("AzureKustoResult", contains="DBIResult")


## methods

#' @export
setMethod("dbUnloadDriver", "AzureKustoDriver", function(drv, ...)
{
    TRUE
})

setMethod("show", "AzureKustoDriver", function(object){
    cat("<AzureKustoDriver>\n")
    invisible(object)
})

AzureKusto <- function()
{
    new("AzureKustoDriver")
}

#' @export
#' @examples
#' \dontrun{
#' db <- dbConnect(AzureKusto::AzureKusto())
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "mtcars | where cyl == 4")
#' }
setMethod("dbConnect", "AzureKustoDriver", function(drv, ...)
{
    new("AzureKustoConnection", host=host, ...)
})

#' @export
setMethod("dbSendQuery", "AzureKustoConnection", function(conn, statement, ...)
{
    # TODO
    new("AzureKustoResult", ...)
})

#' @export
setMethod("dbClearResult", "AzureKustoResult", function(res, ...)
{
})

#' Retrieve records from query
#' @export
setMethod("dbFetch", "AzureKustoResult", function(res, n = -1, ...)
{
})

#' @export
setMethod("dbHasCompleted", "AzureKustoResult", function(res, ...)
{
})
