#############################################################################################
# ingestion can have significant and unpredictable latencies, do not run in automated fashion
#############################################################################################


context("Ingesting")

# skip from testing
skip("Automated database ingestion tests skipped: run manually")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Tests skipped: ARM credentials not set")

# this will add AzureStor to search path
if(!require("AzureStor"))
    skip("Database ingestion tests skipped: AzureStor package not found")

# use persistent testing resources
rgname <- Sys.getenv("AZ_TEST_KUSTO_SERVER_RG")
username <- Sys.getenv("AZ_TEST_KUSTO_USERNAME")
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
srvloc <- Sys.getenv("AZ_TEST_KUSTO_SERVER_LOCATION")
blobacct <- Sys.getenv("AZ_TEST_KUSTO_BLOBACCT")
blobcont <- Sys.getenv("AZ_TEST_KUSTO_BLOBCONT")
adlsacct <- Sys.getenv("AZ_TEST_KUSTO_ADLSACCT")
adlscont <- Sys.getenv("AZ_TEST_KUSTO_ADLSCONT")

storage_app <- Sys.getenv("AZ_TEST_KUSTO_STORAGE_APP")
storage_pwd <- Sys.getenv("AZ_TEST_KUSTO_STORAGE_PWD")

if(rgname == "" || username == "" ||
   srvname == "" || srvloc == "" ||
   blobacct == "" || blobcont == "" ||
   adlsacct == "" || adlscont == "")
    skip("Database ingestion tests skipped: server info not set")

rg <- AzureRMR::az_rm$
    new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)

srv <- rg$get_kusto_cluster(srvname)

# generate random ingestion database
dbname <- paste0("ingest", paste0(sample(letters, 5), collapse=""))
dbres <- srv$create_database(dbname)

dbres$add_principals(name="user1", role="Admin", fqn=paste0("aaduser=", username))
db <- dbres$get_database_endpoint()


blobstor <- rg$get_storage_account(blobacct)
adlsstor <- rg$get_storage_account(adlsacct)


test_that("blob ingestion works",
{
    bloburl <- sprintf("https://%s.blob.core.windows.net/%s/iris.csv", blobacct, blobcont)

    # blob with key
    blobkey <- blobstor$list_keys()[1]
    run_query(db, ".create table irisblobkey (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_blob(db, bloburl, "irisblobkey", key=blobkey, ignoreFirstRecord=TRUE)
    expect_equal(run_query(db, "irisblobkey | count")$Count, 150)

    # blob with sas
    blobsas <- blobstor$get_account_sas()
    run_query(db, ".create table irisblobsas (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_blob(db, bloburl, "irisblobsas", sas=blobsas, ignoreFirstRecord=TRUE)
    expect_equal(run_query(db, "irisblobsas | count")$Count, 150)
})

test_that("ADLSgen2 ingestion works",
{
    adlsurl <- sprintf("https://%s.dfs.core.windows.net/%s/iris.csv", adlsacct, adlscont)

    # adls2 with key
    adlskey <- adlsstor$list_keys()[1]
    run_query(db, ".create table irisadlskey (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_adls2(db, adlsurl, "irisadlskey", key=adlskey, ignoreFirstRecord=TRUE)
    expect_equal(run_query(db, "irisadlskey | count")$Count, 150)

    # adls2 with token
    adlstok <- AzureAuth::get_azure_token("https://storage.azure.com/",
        tenant=tenant, app=storage_app, password=storage_pwd)
    run_query(db, ".create table irisadlstok (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_adls2(db, adlsurl, "irisadlstok", tok=adlstok, ignoreFirstRecord=TRUE)
    expect_equal(run_query(db, "irisadlstok | count")$Count, 150)
})

test_that("local indirect ingestion works",
{
    bl <- blobstor$get_blob_endpoint()
    stagecont <- AzureStor::create_blob_container(bl, db$database)

    run_query(db, ".create table irisfileindirect (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, "../resources/iris.csv", "irisfileindirect", method="indirect",
                 staging_container=stagecont)
    expect_equal(run_query(db, "irisfileindirect | count")$Count, 150)

    run_query(db, ".create table irisdfindirect (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, iris, "irisdfindirect", method="indirect",
                 staging_container=stagecont)
    expect_equal(run_query(db, "irisdfindirect | count")$Count, 150)
})

test_that("local streaming ingestion works",
{
    run_query(db,
        sprintf(".alter database %s policy streamingingestion '{\"NumberOfRowStores\": 10}'", db$database))

    run_query(db, ".create table irisfilestream (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, "../resources/iris.csv", "irisfilestream", method="streaming",
                 streamFormat="Csv")
    expect_equal(run_query(db, "irisfilestream | count")$Count, 150)

    run_query(db, ".create table irisdfstream (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, iris, "irisdfstream", method="streaming")
    expect_equal(run_query(db, "irisdfstream | count")$Count, 150)
})

test_that("local inline ingestion works",
{
    run_query(db, ".create table irisfileinline (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, "../resources/iris.csv", "irisfileinline", method="inline")
    expect_equal(run_query(db, "irisfileinline | count")$Count, 150)

    run_query(db, ".create table irisdfinline (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, iris, "irisdfinline", method="inline")
    expect_equal(run_query(db, "irisdfinline | count")$Count, 150)
})

test_that("ingestion via compute() verb works",
{
    run_query(db, ".create table irisfileinline2 (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, "../resources/iris.csv", "irisfileinline2", method="inline")
    irisfileinline2 <- tbl_kusto(db, "irisfileinline2")

    q <- irisfileinline2 %>%
        dplyr::group_by(species) %>%
        dplyr::summarize(max_sepal_length = max(sl))

    new_tbl <- dplyr::compute(q, "irismaxsepallength")
    expect_equal(new_tbl$src$table, "['irismaxsepallength']")
})

test_that("ingestion via copy_to() verb works",
{
    tbl_iris <- iris
    names(tbl_iris) <- c('SepalLength', 'SepalWidth', 'PetalLength', 'PetalWidth', 'Species')
    iris_copy_to <- dplyr::copy_to(db, tbl_iris, "iris_copy_to")
    expect_equal(iris_copy_to$src$table, "['iris_copy_to']")
})

test_that("copy_to uses compute() when dest and src are in same Kusto database",
{
    run_query(db, ".create table irisfileinline3 (sl:real, sw:real, pl:real, pw:real, species:string)")
    ingest_local(db, "../resources/iris.csv", "irisfileinline3", method="inline")
    irisfileinline3 <- tbl_kusto(db, "irisfileinline3")

    q <- irisfileinline3 %>%
        dplyr::group_by(species) %>%
        dplyr::summarize(max_sepal_length = max(sl))

    irismaxsepallength2 <- dplyr::copy_to(db, q, 'irismaxsepallength2')
    expect_equal(irismaxsepallength2$src$table, "['irismaxsepallength2']")
})

srv$delete_database(dbname, confirm=FALSE)
delete_blob_container(blobstor$get_blob_endpoint(), dbname, confirm=FALSE)
