#' @export
#' @keywords internal
#' @param op A sequence of operations
#' @param con (optional) A database connection.
kql_build <- function(op, con = NULL)
{
    UseMethod("kql_build")
}

#' @export
kql_build.tbl_abstract <- function(op, con = NULL)
{
    # only used for testing
    q <- flatten_query(op$ops)
    built_q <- purrr::map(q, kql_build)
    kql_query(built_q, src=op$src)
}


#' @export
kql_build.op_base_local <- function(op, con, ...)
{
    ident("df")
}

#' @export
kql_build.op_select <- function(op, con, ...)
{
    kql_clause_select(translate_kql(!!! op$dots))
}

#' @export
kql_build.op_filter <- function(op, con, ...)
{
    dots <- mapply(get_expr, op$dots)
    dot_names <- mapply(all_names, dots)
    cols <- tidyselect::vars_select(op$vars, !!! dot_names)

    translated_dots <- purrr::map(dots, translate_kql)
    built_dots <- purrr::map(translated_dots, build_kql)
    clauses <- purrr::map(built_dots, kql_clause_filter)

    clauses
}

#' @export
kql_build.op_distinct <- function(op, con, ...)
{
    if (length(op$dots) == 0){
        cols <- op$vars
    } else
    {
        cols <- tidyselect::vars_select(op$vars, !!! op$dots)
    }

    kql_clause_distinct(ident(cols))
}

#' @export
kql_build.op_mutate <- function(op, con, ...)
{
    assigned_exprs <- purrr::map(op$dots, rlang::get_expr)
    stmts <- purrr::map(assigned_exprs, translate_kql)
    pieces <- lapply(seq_along(assigned_exprs), function(i) sprintf("%s = %s", names(assigned_exprs)[i], stmts[i]))
    kql(paste0("extend ", pieces))
}

#' @export
kql_build.op_arrange <- function(op, con, ...)
{
    dots <- mapply(append_asc, op$dots)
    order_vars <- translate_kql(!!! dots)
    build_kql("order by ", build_kql(escape(order_vars, collapse = ", ", con = con)))
}

append_asc <- function(dot)
{
    if (class(dot[[2]]) == "name")
    {
        dot[[2]] <- call2(expr(asc), dot[[2]])
    }
    else if (class(dot[[2]]) == "call")
    {
        if (dot[[2]][[1]] != expr("desc"))
        {
            dot[[2]] <- call2(expr(asc), dot[[2]])
        } else
        {
            dot
        }
    } else {
        dot
    }
}

#' @export
#' Walks the tree of ops and builds a stack.
flatten_query <- function(op, ops=list())
{
    flat_op <- op
    flat_op$x <- NULL
    flat_op$vars <- op_vars(op)

    if (length(ops) == 0) {
        new_ops <- list(flat_op)
    } else {
        new_ops <- prepend(ops, list(flat_op))
    }
    if (inherits(op, "op_base")){
        return(new_ops)
    } else {
        flatten_query(op$x, new_ops)
    }
}

kql_clause_select <- function(select, con)
{
    stopifnot(is.character(select))
    if (is_empty(select)) {
        abort("Query contains no columns")
    }

    build_kql(
        "project ",
        escape(select, collapse = ", ", con = con)
    )
}

kql_clause_distinct <- function(distinct, con)
{
    stopifnot(is.character(distinct))

    build_kql(
        "distinct ",
        escape(distinct, collapse = ", ", con = con)
    )
}

kql_clause_filter <- function(where)
{
    if (length(where) > 0L) {
        where_paren <- escape(where, parens = FALSE, con = con)
        build_kql("where ", kql_vector(where_paren, collapse = " and "))
  }
}

kql_query <- function(ops, src)
{
    structure(
        list(
            ops = ops,
            src = src
        ),
        class = "kql_query"
    )
}
