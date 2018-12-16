.onLoad <- function(libname, pkgname)
{
    ## extending AzureRMR classes

    AzureRMR::az_resource_group$set("public", "create_data_explorer", overwrite=TRUE,
    function(name, location=self$location, compute="D13_v2", ...)
    {
        az_data_explorer$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name, location=location,
            sku=list(name=compute, tier="Standard"), ...)
    })


    AzureRMR::az_resource_group$set("public", "get_data_explorer", overwrite=TRUE,
    function(name)
    {
        az_data_explorer$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name)
    })


    AzureRMR::az_resource_group$set("public", "delete_data_explorer", overwrite=TRUE,
    function(name, confirm=TRUE, wait=FALSE)
    {
        self$get_data_explorer(name)$delete(confirm=confirm, wait=wait)
    })
}