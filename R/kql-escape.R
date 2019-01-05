#' Escape/quote a string.
#'
#' @param x An object to escape. Existing kql vectors will be left as is,
#'   character vectors are escaped with single quotes, numeric vectors have
#'   trailing `.0` added if they're whole numbers, identifiers are
#'   escaped with double quotes.
#' @param parens,collapse Controls behaviour when multiple values are supplied.
#'   `parens` should be a logical flag, or if `NA`, will wrap in
#'   parens if length > 1.
#'
#'   Default behaviour: lists are always wrapped in parens and separated by
#'   commas, identifiers are separated by commas and never wrapped,
#'   atomic vectors are separated by spaces and wrapped in parens if needed.
#' @param con (optional) Database connection. 
#' @rdname escape
#' @export
#' @examples
#' # Doubles vs. integers
#' escape(1:5)
#' escape(c(1, 5.4))
#'
#' # String vs known kql vs. kql identifier
#' escape("X")
#' escape(kql("X"))
#' escape(ident("X"))
#'
#' # Escaping is idempotent
#' escape("X")
#' escape(escape("X"))
#' escape(escape(escape("X")))
escape <- function(x, parens = NA, collapse = " ", con = NULL)
{
    UseMethod("escape")
}

#' @export
escape.ident <- function(x, parens = FALSE, collapse = ", ", con = NULL)
{
    y <- kql_escape_ident(x)
    kql_vector(y, parens, collapse)
}

#' @export
escape.ident_q <- function(x, parens = FALSE, collapse = ", ", con = NULL)
{
    kql_vector(x, parense, collapse)
}

#' @export
escape.logical <- function(x, parens = NA, collapse = ", ", con = NULL) {
  kql_vector(kql_escape_logical(x), parens, collapse, con = con)
}

#' @export
escape.factor <- function(x, parens = NA, collapse = ", ", con = NULL) {
  x <- as.character(x)
  escape.character(x, parens = parens, collapse = collapse, con = con)
}

#' @export
escape.Date <- function(x, parens = NA, collapse = ", ", con = NULL) {
  x <- as.character(x)
  escape.character(x, parens = parens, collapse = collapse, con = con)
}

#' @export
escape.POSIXt <- function(x, parens = NA, collapse = ", ", con = NULL) {
  x <- strftime(x, "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
  escape.character(x, parens = parens, collapse = collapse, con = con)
}

#' @export
escape.character <- function(x, parens = NA, collapse = ", ", con = NULL) {
    # Kusto doesn't support null strings, instead use empty string
    out <- x
    out[is.na(x)] <- ""
    out[is.null(x)] <- ""
    kql_vector(kql_escape_string(out), parens, collapse, con = con)
}

#' @export
escape.double <- function(x, parens = NA, collapse = ", ", con = NULL)
{
  #  out <- ifelse(is.wholenumber(x), sprintf("%.1f", x), as.character(x))
    out <- as.character(x)
  # Special values
    out[is.na(x)] <- "real(null)"
    inf <- is.infinite(x)
    out[inf & x > 0] <- "'real(+inf)'"
    out[inf & x < 0] <- "'real(-inf)'"

    kql_vector(out, parens, collapse)
}

#' @export
escape.integer <- function(x, parens = NA, collapse = ", ", con = NULL) {
  x[is.na(x)] <- "int(null)"
  kql_vector(x, parens, collapse)
}

#' @export
escape.integer64 <- function(x, parens = NA, collapse = ", ", con = NULL) {
  x <- as.character(x)
  x[is.na(x)] <- "long(null)"
  kql_vector(x, parens, collapse)
}

#' @export
escape.NULL <- function(x, parens = NA, collapse = " ", con = NULL) {
  kql("null")
}

#' @export
escape.kql <- function(x, parens = NULL, collapse = NULL, con = NULL) {
  kql_vector(x, isTRUE(parens), collapse, con = con)
}

#' @export
escape.list <- function(x, parens = TRUE, collapse = ", ", con = NULL) {
  pieces <- vapply(x, escape, character(1), con = con)
  kql_vector(pieces, parens, collapse)
}

#' @export
#' @rdname escape
kql_vector <- function(x, parens = NA, collapse = " ", con = NULL)
{
    if (length(x) == 0) {
        if (!is.null(collapse)) {
            return(if (isTRUE(parens)) kql("()") else kql(""))
        } else {
            return(kql())
        }
    }

    if (is.na(parens)) {
        parens <- length(x) > 1L
    }

    x <- paste(x, collapse = collapse)
    if (parens) x <- paste0("(", x, ")")
    kql(x)
}

#' Build a KQL string.
#'
#' @param ... input to convert to KQL. Use [kql()] to preserve
#'   user input as is (dangerous), and [ident()] to label user
#'   input as kql identifiers (safe)
#' @param .env the environment in which to evalute the arguments. Should not
#'   be needed in typical use.
#' @param con database connection; used to select correct quoting characters.
#' @export
build_kql <- function(..., .env = parent.frame(), con = NULL) {
  escape_expr <- function(x) {
    # If it's a string, leave it as is
    if (is.character(x)) return(x)

    val <- eval_bare(x, .env)
    # Skip nulls, so you can use if statements like in paste
    if (is.null(val)) return("")

    escape(val, con = con)
  }

  pieces <- vapply(dots(...), escape_expr, character(1))
  kql(paste0(pieces, collapse = ""))
}

#' Helper function for quoting kql elements.
#'
#' If the quote character is present in the string, it will be doubled.
#' `NA`s will be replaced with NULL.
#'
#' @export
#' @param x Character vector to escape.
#' @param quote Single quoting character.
#' @export
#' @keywords internal
#' @examples
#' kql_quote("abc", "'")
#' kql_quote("I've had a good day", "'")
#' kql_quote(c("abc", NA), "'")
kql_quote <- function(x, quote) {
  if (length(x) == 0) {
    return(x)
  }

  y <- gsub(quote, paste0(quote, quote), x, fixed = TRUE)
  y <- paste0(quote, y, quote)
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)

  y
}

#' @export
kql_escape_string <- function(x)
{
    kql_quote(x, "'")
}

#' @export
kql_escape_ident <- function(x)
{
    x
}

#' @export
kql_escape_logical <- function(x) {
  y <- tolower(as.character(x))
  y[is.na(x)] <- "null"

  y
}
