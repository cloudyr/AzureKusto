context("Querying")

# use persistent testing server
tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
srvloc <- Sys.getenv("AZ_TEST_KUSTO_SERVER_LOCATION")
dbname <- Sys.getenv("AZ_TEST_KUSTO_DATABASE")
if(tenant == "" || srvname == "" || srvloc == "" || dbname == "")
    skip("Database endpoint tests skipped: server info not set")

server <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)
db <- kusto_query_endpoint(server=server, database=dbname, tenantid=tenant)

test_that("Queries work",
{
    out <- run_query(db, "iris | summarize count() by species")
    expect_is(out, "data.frame")

    db2 <- db
    db2$token <- NULL
    expect_error(run_query(db2, "iris | summarize count() by species"))
})

test_that("Commands work",
{
    out <- run_command(db, ".show cluster")
    expect_is(out, "data.frame")

    db2 <- db
    db2$token <- NULL
    expect_error(run_command(db2, ".show cluster"))
})
