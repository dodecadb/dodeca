package org.dozendb.cloud

import org.neo4j.cloud.storage.StorageSystemProviderFactory

/*
 * The schemes Neo4j's storage interface resolves through DozenDB, registered in
 * `META-INF/services`. Each names its provider's class rather than referring to it, as the interface
 * asks: a cloud's SDK loads only when a URI of its scheme is first resolved.
 */

/** `s3://<bucket>/<key>`: Amazon S3 and S3-compatible stores. */
class S3StorageFactory : StorageSystemProviderFactory("s3") {
    override fun storageSystemProviderClass() = "org.dozendb.cloud.s3.S3StorageProvider"
}

/** `gs://<bucket>/<key>`: Google Cloud Storage. */
class GcsStorageFactory : StorageSystemProviderFactory("gs") {
    override fun storageSystemProviderClass() = "org.dozendb.cloud.gcs.GcsStorageProvider"
}

/** `azb://<storage account>/<container>/<key>`: Azure Blob Storage. */
class AzureStorageFactory : StorageSystemProviderFactory("azb") {
    override fun storageSystemProviderClass() = "org.dozendb.cloud.azure.AzureStorageProvider"
}
