context("ARM interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Resource creation tests skipped: ARM credentials not set")

username <- Sys.getenv("AZ_TEST_KUSTO_USERNAME")
if(username == "")
    skip("Resource creation tests skipped: database user not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)

# random resource group/server names
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
srvname <- paste(sample(letters, 10, replace=TRUE), collapse="")


# should only get one devicecode prompt here
test_that("AAD token functions work",
{
    tok1 <- get_kusto_token(clustername=srvname, location="australiaeast", tenant=tenant)
    expect_true(AzureRMR::is_azure_token(tok1))

    srvuri <- paste0("https://", srvname, ".australiaeast.kusto.windows.net")
    tok2 <- get_kusto_token(srvuri, tenant=tenant)
    expect_true(AzureRMR::is_azure_token(tok2))

    expect_identical(tok1$hash(), tok2$hash())
    expect_identical(tok1$hash(), tok2$hash())

    Sys.setenv(AZ_TEST_KUSTO_TOKEN_HASH=tok1$hash())

    expire <- as.numeric(tok1$credentials$expires_on)
    Sys.sleep(5)
    tok1$refresh()
    expect_true(as.numeric(tok1$credentials$expires_on) > expire)

    toks <- list_kusto_tokens()
    expect_true(is.list(toks) && all(sapply(toks, AzureRMR::is_azure_token)))
})


test_that("ARM interface works",
{
    expect_false(sub$resource_group_exists(rgname))
    rg <- sub$create_resource_group(rgname, location="australiaeast")
    expect_true(sub$resource_group_exists(rgname))

    # this takes 10-15 minutes
    srv <- rg$create_kusto_cluster(srvname, wait=TRUE)
    expect_true(is_kusto_cluster(srv))

    srv <- rg$get_kusto_cluster(srvname)
    expect_true(is_kusto_cluster(srv))

    default_tenant <- srv$get_default_tenant()
    expect_type(default_tenant, "character")
    expect_true(AzureRMR::normalize_tenant(tenant) == default_tenant)

    tok <- srv$get_aad_token()
    expect_true(AzureRMR::is_azure_token(tok))
    expect_identical(tok$hash(), Sys.getenv("AZ_TEST_KUSTO_TOKEN_HASH"))

    db <- srv$create_database("db")
    expect_true(is_kusto_database(db))

    dbs <- srv$list_databases()
    expect_true(is.list(dbs) && all(sapply(dbs, is_kusto_database)))

    db2 <- srv$get_database("db")
    expect_true(is_kusto_database(db2))
    expect_identical(db$name, db2$name)

    db$add_principals(username, role="Admin", fqn=sprintf("aaduser=%s", username))

    pr <- db$list_principals()
    expect_is(pr, "data.frame")

    # role mismatch -> principal not removed
    db$remove_principals(username, fqn=sprintf("aaduser=%s", username))

    pr2 <- db$list_principals()
    expect_true(nrow(pr) == nrow(pr2))

    db$remove_principals(username, role="Admin", fqn=sprintf("aaduser=%s", username))

    pr3 <- db$list_principals()
    expect_true(nrow(pr) > nrow(pr3))

    srv$delete_database("db", confirm=FALSE)

    expect_null(delete_kusto_token(hash=tok$hash(), confirm=FALSE))
})


teardown(sub$delete_resource_group(rgname, confirm=FALSE))

