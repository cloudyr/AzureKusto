#' @export
kql_render <- function(query, con = NULL, ...)
{
    UseMethod("kql_render")
}

#' @export
kql_render.kql_query <- function(q)
{
    tblname <- sprintf("database(%s).%s", q$src$db, q$ops[[1]])
    q_str <- paste(unlist(q$ops[-1]), collapse = "\n| ")
    q_str <- paste(tblname, q_str, sep="\n| ")

    cat(q_str, "\n")

    invisible(q_str)
}

#' @export
kql_render.tbl_abstract <- function(query, con = query$con, ...)
{
    # only used for testing
    qry <- kql_build(query$ops, con = con, ...)
    kql_render(qry, con = con, ...)
}

#' @export
kql_render.op <- function(query, con = NULL, ...)
{
    kql_render(kql_build(query, con = con, ...), con = con, ...)
}

#' @export
kql_render.ident <- function(query, con = NULL, ..., root = TRUE)
{
    query
}

#' @export
kql_render.kql <- function(query, con = NULL, ...)
{
    query
}
