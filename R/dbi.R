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



## methods

setMethod("show", "AzureKustoConnection", function(object){
    cat("<AzureKustoConnection>\n")
    cat("Endpoint:\n")
    print(object@endpoint)
    invisible(object)
})


#' @export
AzureKusto <- function()
{
    new("AzureKustoDriver")
}


#' Connect to a Kusto cluster
#'
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
setMethod("dbGetQuery", c(conn="AzureKustoConnection", statement="character"), function(conn, statement, ...)
{
    run_query(conn@endpoint, statement, ...)
})


#' @export
setMethod("dbCreateTable", "AzureKustoConnection", function(conn, name, fields, ..., row.names=NULL, temporary=FALSE)
{
    build_fields <- function()
    {
        if(is.character(fields) &&
           !is.null(names(fields)) &&
           !any(is.na(names(fields))))
        {
            # build a dummy list of values
            # converting string "x" -> object of class x effectively guards against injection
            fields <- sapply(fields, function(value)
            {
                switch(tolower(value),
                    bool=logical(0),
                    int=integer(0),
                    date=, datetime=structure(numeric(0), class=c("POSIXct", "POSIXt")),
                    real=numeric(0),
                    long=structure(numeric(0), class="integer64"),
                    character(0))
            })
        }
        else if(!is.list(fields))
            stop("Bad fields specification", call.=FALSE)

        get_param_types(fields)
    }

    stopifnot(is.null(row.names))
    stopifnot(temporary == FALSE)
    cmd <- paste(".create table",
        escape(ident(name)),
        build_fields())
    run_query(conn@endpoint, cmd)
})


#' @export
setMethod("dbRemoveTable", "AzureKustoConnection", function(conn, name, ...)
{
    cmd <- paste(".drop table", escape(ident(name)))
    run_query(conn@endpoint, cmd)
})

