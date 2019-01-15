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

username <- Sys.getenv("AZ_TEST_KUSTO_USERNAME")
if(username == "")
    skip("Database tests skipped, username not set")


rgname <- Sys.getenv("AZ_TEST_RG")
rg <- AzureRMR::az_rm$
    new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)


dbname <- paste(sample(letters, 10, replace=TRUE), collapse="")

test_that("Database resource functions work",
{
    srv <- rg$get_kusto_cluster(srvname))
    expect_true(is_kusto_cluster(srv))

    db <- srv$create_database(dbname, retention_period=1000, cache_period=30)
    expect_true(is_kusto_database(db))

    dbs <- srv$list_databases()
    expect_true(is.list(dbs) && all(sapply(dbs, is_kusto_database)))
})


test_that("Database endpoint functions work",
{
    srv <- rg$get_kusto_cluster(srvname))
    expect_true(is_kusto_cluster(srv))

    db <- srv$get_database(dbname)
    endp1 <- db$get_query_endpoint()

    server <- srv$properties$queryUri
    endp2 <- kusto_query_endpoint(server=server, database=dbname, tenant=tenant)

    endp3 <- kusto_query_endpoint(server=server, database=dbname,
        .azure_token=get_kusto_token(srvname, "australiasoutheast", tenant))

    conn_str <- sprintf("server=%s;database=%s;tenant=%s", srvname, dbname, tenant)
    endp4 <- kusto_query_endpoint(conn_str)

    expect_identical(endp1$token$hash(), endp2$token$hash())
    expect_identical(endp1$token$hash(), endp3$token$hash())
    expect_identical(endp1$token$hash(), endp4$token$hash())
})


rg$delete(confirm=FALSE)
