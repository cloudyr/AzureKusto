#' Create a local lazy tbl
#'
#' Useful for testing KQL generation without a remote connection.
#'
#' @keywords internal
#' @export
#' @examples
#' library(dplyr)
#' df <- data.frame(x = 1, y = 2)
#'
#' df <- tbl_lazy(df, src = simulate_ade())
#' df %>% summarise(x = sd(x)) %>% show_query()
tbl_abstract <- function(df, src = NULL) {
  make_tbl("abstract", ops = op_base_local(df), src = src)
}

setOldClass(c("tbl_abstract", "tbl"))

#' @export
select.tbl_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("select", .data, dots = dots)
}

distinct.tbl_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("distinct", .data, dots = dots)
}

filter.tbl_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("filter", .data, dots = dots)
}

mutate.tbl_abstract <- function(.data, ...)
{
    dots <- quos(..., .named=TRUE)
    add_op_single("mutate", .data, dots = dots)
}

simulate_ade <- function()
{
  structure(
      list(
          db = "local_df",
          cluster = "local_df"
      ),
    class = "ade_database_endpoint"
  )
}

#' @export
show_query.tbl_abstract <- function(tbl)
{
    qry <- kql_build(tbl)
    kql_render(qry)
}

## TODO: Create an AzureDataExplorer table based on the below dbplyr code
## #'
## #' @keywords internal
## #' @export
## #' @param subclass name of subclass
## #' @param ... needed for agreement with generic. Not otherwise used.
## tbl_ade <- function(subclass, src, from, ...) {
##   # If not literal sql, must be a table identifier
##   #from <- as.sql(from)

##   vars <- vars %||% db_query_fields(src$con, from)
##   ops <- op_base_remote(from, vars)

##   make_tbl(c(subclass, "ade", "abstract"), src = src, ops = ops)
## }
