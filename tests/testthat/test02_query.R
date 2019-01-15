context("Querying")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Tests skipped: ARM credentials not set")

# use persistent testing server
rgname <- Sys.getenv("AZ_TEST_KUSTO_SERVER_RG")
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
dbname <- Sys.getenv("AZ_TEST_KUSTO_DATABASE")
if(rgname == "" || srvname == "" || dbname == "")
    skip("Database endpoint tests skipped: server info not set")

db <- AzureRMR::az_rm$
    new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_kusto_cluster(srvname)$
    get_database(dbname)$
    get_query_endpoint()

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
