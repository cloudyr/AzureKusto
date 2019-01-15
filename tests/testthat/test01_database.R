context("Databases")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Tests skipped: ARM credentials not set")

srvname <- Sys.getenv("AZ_TEST_KUSTO")
if(srvname == "")
    skip("Database endpoint tests skipped: resource name not set")

username <- Sys.getenv("AZ_TEST_KUSTO_USER")
if(username = "")
    skip("Database tests skipped, username not set")


rgname <- Sys.getenv("AZ_TEST_RG")
rg <- AzureRMR::az_rm$
        new(tenant=tenant, app=app, password=password)$
        get_subscription(subscription)$
        get_resource_group(rgname)



test_that("Database resource functions work",
{
    srv <- rg$get_kusto_cluster(srvname))
    expect_true(is_kusto_cluster(srv))

    dbname <- paste(sample(letters, 10, replace=TRUE), collapse="")
    db <- srv$create_database(dbname, retention_period=1000, cache_period=30)
    expect_true(is_kusto_database(db))

    dbs <- srv$list_databases()
    expect_true(is.list(dbs) && all(sapply(dbs, is_kusto_database))))
})


test_that("Database endpoint functions work",
{
    srv <- rg$get_kusto_cluster(srvname))
    expect_true(is_kusto_cluster(srv))
})
