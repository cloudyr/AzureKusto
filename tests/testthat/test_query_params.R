context("Building Query Parameters")

test_that("Query parameter types can be assigned appropriately",
{
    types <- get_param_types(
        foo="hi",
        bar=1L,
        baz=FALSE,
        quux=1.1,
        dt=as.Date('2019-01-01'))

    expect_equal(
        types,
        "declare query_parameters (foo:string, bar:int, baz:bool, quux:real, dt:datetime); ")
})
