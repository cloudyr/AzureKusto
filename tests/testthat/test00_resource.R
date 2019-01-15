context("ARM interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)

Sys.setenv(AZ_TEST_RG=paste(sample(letters, 20, replace=TRUE), collapse=""))
Sys.setenv(AZ_TEST_KUSTO=paste(sample(letters, 10, replace=TRUE), collapse=""))


test_that("ARM interface works",
{
    rgname <- Sys.getenv("AZ_TEST_RG")
    expect_false(sub$resource_group_exists(rgname))
    rg <- sub$create_resource_group(rgname, location="australiaeast")
    expect_true(sub$resource_group_exists(rgname))

    srvname <- Sys.getenv("AZ_TEST_KUSTO")
    srv <- rg$create_kusto_cluster(srvname, wait=TRUE)

    expect_true(is_kusto_cluster(srv)))
})


test_that("Cluster resource functions work",
{
    srv <- rg$get_kusto_cluster(srvname))
    expect_true(is_kusto_cluster(srv))

    default_tenant <- srv$get_default_tenant()
    expect_type(default_tenant, "character")
    expect_true(tenant == default_tenant)

    tok <- srv$get_aad_token(tenant)
    expect_true(AzureRMR::is_azure_token(tok) && tok$tenant == tenant)

    conf_dir <- rappdirs::user_config_dir(appname="AzureKusto", appauthor="AzureR", roaming=FALSE)
    expect_true(length(dir(conf_dir) >= 1))
})


test_that("AAD token functions work",
{
    srvname <- Sys.getenv("AZ_TEST_KUSTO")
    tok <- get_kusto_token(cluster=srvname, location="australiaeast", tenant=tenant)
    expect_true(AzureRMR::is_azure_token(tok))

    toks <- list_kusto_tokens()
    expect_true(is.list(toks) && all(sapply(toks, AzureRMR::is_azure_token)))
})
