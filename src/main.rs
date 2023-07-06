use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use azure_identity::{AutoRefreshingTokenCredential, DefaultAzureCredentialBuilder};
use azure_storage::prelude::*;
use azure_storage_datalake::prelude::*;
use lazy_static::lazy_static;
use tokio::sync::{Mutex, RwLock};
use miette::Diagnostic;
use thiserror::Error;

lazy_static! {
    static ref AZ_STORAGE_BACKEND_CACHE: Arc<Mutex<HashMap<String, Arc<RwLock<DataLakeClient>>>>> = Arc::new(Mutex::new(HashMap::new()));
}

/// Cloud backend for Azure ADLS Gen 2 storage. Creates an authenticated client for the supplied storage account with can be reused async
#[derive(Clone, Debug)]
pub struct AzureStorageBackend {
    pub(crate) client: Arc<RwLock<DataLakeClient>>
}


impl AzureStorageBackend {
    fn new<'o, T: AsRef<str> + Send + Sync + 'o>(auth_parameter: T) ->  Pin<Box<dyn Future<Output = Result<Self, miette::Error>> + Send + Sync + 'o>>
        where Self: Sized
    {
        let storage_account_url = auth_parameter
            .as_ref()
            .to_string();

        let cache_clone = Arc::clone(&AZ_STORAGE_BACKEND_CACHE);

        Box::pin(async move {
            let data_lake_client = {
                let mut cache_guard = cache_clone.lock().await;

                match cache_guard.get_mut(&storage_account_url) {
                    Some(existing_client) => {
                        println!("Found existing client");
                        Arc::clone(existing_client)
                    },
                    None => {
                        println!("Found existing client");
                        let token_credential = Arc::new(DefaultAzureCredentialBuilder::default().build());
                        let refresh_token = Arc::new(AutoRefreshingTokenCredential::new(token_credential));
                        let storage_credentials = StorageCredentials::token_credential(refresh_token);
                        let data_lake_client = DataLakeClient::new(storage_account_url.clone(), storage_credentials);

                        let data_lake_client_arc = Arc::new(RwLock::new(data_lake_client));
                        cache_guard.insert(storage_account_url, Arc::clone(&data_lake_client_arc));
                        data_lake_client_arc
                    }
                }
            };

            Ok(Self {
                client: data_lake_client,
            })
        }
        )
    }
}


fn main() {
    println!("Hello, world!");
}


#[cfg(test)]
mod tests {
    use super::*;

    use uuid::Uuid;

    const STORAGE_ACCOUNT: &str = "metastoredevazio";

    fn generate_unique_names() -> (String, String) {
        let container_name = format!("testcontainer-{}", Uuid::new_v4());
        let file_name = format!("testfile-{}", Uuid::new_v4());
        (container_name, file_name)
    }

    async fn create_container(backend: &AzureStorageBackend, container_name: &String) -> Result <(), Box<dyn std::error::Error>> {

        let read_lock = backend.client.read().await;
        let file_system_client = read_lock
            .file_system_client(container_name);
        file_system_client.create().await?;

        drop(read_lock);
        Ok(())
    }

    async fn create_file(backend: &AzureStorageBackend, container_name: &String, file_name: &String) -> Result <(), Box<dyn std::error::Error>> {

        let read_lock = backend.client.read().await;
        let file_client = read_lock
            .file_system_client(container_name)
            .into_file_client(file_name);
        file_client.create().await?;

        drop(read_lock);
        Ok(())
    }

    async fn delete_file(backend: &AzureStorageBackend, container_name: &String, file_name: &String) -> Result <(), Box<dyn std::error::Error>> {

        let read_lock = backend.client.read().await;
        let file_client = read_lock
            .file_system_client(container_name)
            .into_file_client(file_name);
        file_client.delete().await?;

        drop(read_lock);
        Ok(())
    }

    async fn delete_container(backend: &AzureStorageBackend, container_name: &String) -> Result <(), Box<dyn std::error::Error>> {

        let read_lock = backend.client.read().await;
        let file_system_client = read_lock
            .file_system_client(container_name);
        file_system_client.delete().await?;

        drop(read_lock);
        Ok(())
    }

    #[tokio::test]
    async fn test_1() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_2() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_3() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_4() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_5() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_6() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_7() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_8() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_9() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_10() -> Result <(), Box<dyn std::error::Error>> {
        let (container_name, file_name) = generate_unique_names();

        let azure_storage_backend = AzureStorageBackend::new(STORAGE_ACCOUNT).await?;
        println!("Created backend: {:?}", azure_storage_backend);
        println!("Creating container: {}", container_name);
        create_container(&azure_storage_backend, &container_name).await?;
        println!("Creating file: {}", file_name);
        create_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting file: {}", file_name);
        delete_file(&azure_storage_backend, &container_name, &file_name).await?;
        println!("Deleting container: {}", container_name);
        delete_container(&azure_storage_backend, &container_name).await?;

        Ok(())
    }
}
