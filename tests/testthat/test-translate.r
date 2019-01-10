context("translate")
library(dplyr)

tbl_iris <- tibble::as.tibble(iris)
names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
tbl_iris <- tbl_abstract(tbl_iris, src = simulate_kusto())

test_that("select is translated to project",
{
    q <- tbl_iris %>%
        select(Species, SepalLength) %>%
        show_query()

    expect_equal(q, "database(local_df).df\n| project Species, SepalLength")
})

test_that("distinct is translated to distinct",
{
    q <- tbl_iris %>%
        distinct(Species, SepalLength) %>%
        show_query()

    expect_equal(q, "database(local_df).df\n| distinct Species, SepalLength")
})

test_that("kql_infix formats correctly",
{
    fn <- kql_infix("==")
    expr <- fn(translate_kql(foo), translate_kql(bar))
    expect_equal(as.character(expr), "foo == bar")
})

test_that("kql_prefix formats correctly",
{
    fn <- kql_prefix("sum")
    expr <- fn(translate_kql(foo), translate_kql(bar), translate_kql(baz))
    expect_equal(as.character(expr), "sum(foo, bar, baz)")
})

test_that("filter is translated to where with a single expression",
{
    q <- tbl_iris %>%
        filter(Species == "setosa")

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| where Species == 'setosa'")
})

test_that("multiple arguments to filter() become multiple where clauses",
{
    q <- tbl_iris %>%
        filter(Species == "setosa", SepalLength > 4.1)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| where Species == 'setosa'\n| where SepalLength > 4.1")
})

test_that("filter errors on missing symbols",
{
    q <- tbl_iris %>%
        filter(Speciess == "setosa")

    expect_error(show_query(q), "Unknown column `Speciess` ")
    #expect_error(show_query(q), "object 'Speciess' not found")
})

test_that("select and filter can be combined",
{
    q <- tbl_iris %>%
        filter(Species == "setosa") %>%
        select(Species, SepalLength)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| where Species == 'setosa'\n| project Species, SepalLength")
})

test_that("select errors on column after selected away",
{
    q <- tbl_iris %>%
        select(Species) %>%
        select(SepalLength)

    expect_error(show_query(q), "object 'SepalLength' not found")
})

test_that("mutate translates to extend",
{
    q <- tbl_iris %>%
        mutate(Species2 = Species)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| extend Species2 = Species")
})

test_that("multiple arguments to mutate() become multiple extend clauses",
{
    q <- tbl_iris %>%
        mutate(Species2 = Species, Species3 = Species2, Foo = 1 + 2)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| extend Species2 = Species\n| extend Species3 = Species2\n| extend Foo = 1 + 2")
})

test_that("sum() translated correctly",
{
    expect_equal(as.character(translate_kql(MeanSepalLength = mean(SepalLength, na.rm = TRUE))),
                 "avg(SepalLength)"
                 )
})

test_that("arrange() generates order by ",
{
    q <- tbl_iris %>%
        arrange(Species, desc(SepalLength))

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).df\n| order by Species asc, SepalLength desc")
})

test_that("group_by() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        summarize(MaxSepalLength = max(SepalLength))

    q_str <- q %>% show_query()

    expect_equal(q_str, "database(local_df).df\n| summarize MaxSepalLength = max(SepalLength) by Species")
})

test_that("group_by() followed by ungroup() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        summarize(MaxSepalLength = max(SepalLength)) %>%
        ungroup() %>%
        summarize(MeanOfMaxSepalLength = mean(MaxSepalLength))

    q_str <- q %>% show_query()

    expect_equal(q_str, "database(local_df).df\n| summarize MaxSepalLength = max(SepalLength) by Species\n| summarize MeanOfMaxSepalLength = avg(MaxSepalLength)")
})

test_that("group_by() followed by mutate() partitions the mutation by the grouping variables",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        mutate(SpeciesMaxSepalLength = max(SepalLength))

    q_str <- q %>% show_query()

    expect_equal(q_str, "database(local_df).df\n| as tmp | join kind=leftouter (tmp | summarize SpeciesMaxSepalLength = max(SepalLength) by Species) on Species\n| project SepalLength, SepalWidth, PetalLength, PetalWidth, Species, SpeciesMaxSepalLength")
})

test_that("mutate() with an agg function and no group_by() groups by all other columns",
{

    q <- tbl_iris %>%
        mutate(MaxSepalLength = max(SepalLength))

    q_str <- q %>% show_query()

    expect_equal(q_str, "database(local_df).df\n| summarize MaxSepalLength = max(SepalLength) by SepalLength, SepalWidth, PetalLength, PetalWidth, Species")
})

test_that("is_agg works with symbols and strings", {

    expect_true(is_agg(n))
    expect_true(is_agg("n"))
    expect_false(is_agg(o))
    expect_false(is_agg("o"))
    expect_false(is_agg(`+`))
    expect_false(is_agg(abs))
    expect_false(is_agg(TRUE))
})
