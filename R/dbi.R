## subclasses

setOldClass("kusto_database_endpoint")

#' @keywords internal
#' @export
setClass("AzureKustoDriver", contains="DBIDriver")

#' Azure Kusto connection class
#'
#' @keywords internal
#' @export
setClass("AzureKustoConnection", contains="DBIConnection", slots=list(
    endpoint="kusto_database_endpoint"
))

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

#' @export
AzureKusto <- function()
{
    new("AzureKustoDriver")
}

#' @examples
#' \dontrun{
#' db <- dbConnect(AzureKusto::AzureKusto(),
#'                 server="https://mycluster.location.kusto.windows.net", database="database"...)
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "mtcars | where cyl == 4")
#' }
#' @rdname AzureKusto
#' @export
setMethod("dbConnect", "AzureKustoDriver", function(drv, ...)
{
    endpoint <- kusto_database_endpoint(...)
    new("AzureKustoConnection", endpoint=endpoint)
})

#' @export
setMethod("dbSendQuery", "AzureKustoConnection", function(conn, statement, ...)
{
    # TODO
    new("AzureKustoResult", ...)
})

