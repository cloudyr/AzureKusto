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
add_op_single <- function(name, .data, dots = list(), args = list())
{
    .data$ops <- op_single(name, x = .data$ops, dots = dots, args = args)
    .data
}

#' @export
op_grps <- function(op) UseMethod("op_grps")

#' @export
op_grps.op_base <- function(op) character()

#' @export
op_grps.op_group_by <- function(op)
{
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
    ## if (length(grps) == 1)
    ## {
    ##     character()
    ## } else {
    ##     grps[-length(grps)]
    ## }
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
