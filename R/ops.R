#' @export
op_base <- function(x, vars, class = character())
{
    stopifnot(is.character(vars))

    structure(
        list(
            x = x,
            vars = vars
        ),
        class = c(paste0("op_base_", class), "op_base", "op")
    )
}

op_base_local <- function(df)
{
    op_base(df, names(df), class = "local")
}

#' @export
#' @rdname lazy_ops
op_single <- function(name, x, dots = list(), args = list())
{
    structure(
        list(
            name = name,
            x = x,
            dots = dots,
            args = args
        ),
        class = c(paste0("op_", name), "op_single", "op")
    )
}

#' @export
#' @rdname lazy_ops
add_op_single <- function(name, .data, dots = list(), args = list())
{
    .data$ops <- op_single(name, x = .data$ops, dots = dots, args = args)
    .data
}

#' @export
#' @rdname lazy_ops
op_grps <- function(op) UseMethod("op_grps")
#' @export
op_grps.op_base <- function(op) character()
#' @export
op_grps.op_group_by <- function(op) {
    if (isTRUE(op$args$add)) {
        union(op_grps(op$x), names(op$dots))
    } else {
        names(op$dots)
    }
}
#' @export
op_grps.op_ungroup <- function(op)
{
    character()
}
#' @export
op_grps.op_summarise <- function(op)
{    
    grps <- op_grps(op$x)
    if (length(grps) == 1) {
        character()
    } else {
        grps[-length(grps)]
    }
}

#' @export
op_grps.op_rename <- function(op)
{
    names(tidyselect::vars_rename(op_grps(op$x), !!! op$dots, .strict = FALSE))
}

#' @export
op_grps.op_single <- function(op)
{
    op_grps(op$x)
}
#' @export
op_grps.op_double <- function(op)
{
    op_grps(op$x)
}

#' @export
op_grps.tbl_lazy <- function(op)
{
    op_grps(op$ops)
}


#' @export
#' @rdname lazy_ops
op_vars <- function(op) UseMethod("op_vars")

#' @export
op_vars.op_base <- function(op)
{
    op$vars
}
#' @export
op_vars.op_select <- function(op)
{
    names(tidyselect::vars_select(op_vars(op$x), !!! op$dots, .include = op_grps(op$x)))
}

#' @export
op_vars.op_rename <- function(op)
{
    names(rename_vars(op_vars(op$x), !!! op$dots))
}
#' @export
op_vars.op_summarise <- function(op)
{
    c(op_grps(op$x), names(op$dots))
}
#' @export
op_vars.op_distinct <- function(op)
{
    if (length(op$dots) == 0) {
        op_vars(op$x)
    } else  {
                                        #c(op_grps(op$x), names(op$dots))
        unique(c(op_vars(op$x), names(op$dots)))
    }
}
#' @export
op_vars.op_mutate <- function(op)
{
    unique(c(op_vars(op$x), names(op$dots)))
}
#' @export
op_vars.op_single <- function(op)
{
    op_vars(op$x)
}
#' @export
op_vars.op_join <- function(op)
{
    op$args$vars$alias
}
#' @export
op_vars.op_semi_join <- function(op)
{
    op_vars(op$x)
}
#' @export
op_vars.op_set_op <- function(op)
{
    union(op_vars(op$x), op_vars(op$y))
}
#' @export
op_vars.tbl_lazy <- function(op)
{
    op_vars(op$ops)
}


kql_env <- function(expr, vars)
{
    data_env <- as_environment(set_names(vars))

    calls <- all_calls(expr)
    call_list <- purrr::map(set_names(calls), unknown_op)
    call_env <- as_environment(call_list, parent = data_env)

    op_env <- env_clone(operator_env, call_env)
    op_env
}

expr_type <- function(x)
{
    if (is_syntactic_literal(x))
    {
        if (is.character(x))
        {
            "string"
        } else
        {
            "literal"
        }
    } else if (is.symbol(x))
    {
        "symbol"
    } else if (is.call(x))
    {
        "call"
    } else if (is.pairlist(x))
    {
        "pairlist"
    } else
    {
        typeof(x)
    }
}

switch_expr <- function(x, ...)
{
    switch(expr_type(x),
           ...,
           stop("Don't know how to handle type ", typeof(x), call. = FALSE)
           )
}

all_calls_rec <- function(x)
{
    switch_expr(x,
                constant = ,
                literal = ,
                string = ,
                symbol = character(),
                call = {
                    fname <- as.character(x[[1]])
                    children <- purrr::flatten_chr(purrr::map(as.list(x[-1]), all_calls))
                    c(fname, children)
                }
                )
}

all_calls <- function(x)
{
    unique(all_calls_rec(x))
}

quote_strings_rec <- function(x)
{
    if (expr_type(x) == "string")
    {
        kql_quote(x, "'")
    } else if (expr_type(x) == "call")
    {
        as.call(lapply(x, quote_strings))
    }
    else
    {
        x
    }
}

quote_strings <- function(x)
{
    quote_strings_rec(x)
}

unknown_op <- function(op)
{
    new_function(
        exprs(... = ),
        expr({
            prefix(op)(...)
        })
    )
}



#' @export
render <- function(query, con = NULL, ...)
{
    UseMethod("render")
}

#' @export
render.op_select <- function(op, vars)
{
    cols <- tidyselect::vars_select(vars, !!! op$dots)
    cols <- paste(cols, collapse=", ")
    paste0("project ", cols)
}

#' @export
render.op_distinct <- function(op, vars)
{
    cols <- tidyselect::vars_select(vars, !!! op$dots)
    cols <- paste(cols, collapse=", ")
    paste0("distinct ", cols)
}

#' @export
render.op_filter <- function(op, vars)
{
    dots <- purrr::map(op$dots, get_expr)
    translated_dots <- purrr::map(dots, to_kql, vars = vars)
    paste0("where ", translated_dots)
}

render.op_mutate <- function(op, vars)
{
    #assigned_names <- names(op$dots)
    assigned_exprs <- purrr::map(op$dots, get_expr)
    vars <- append(vars, names(assigned_exprs))
    stmts <- purrr::map(assigned_exprs, to_kql, vars = vars)
    pieces <- lapply(seq_along(assigned_exprs), function(i) sprintf("%s = %s", names(assigned_exprs)[i], stmts[i]))
    paste0("extend ", pieces)
}
