use std::{collections::VecDeque, fmt::Debug, path::Path, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::Mutex,
};

#[derive(Clone)]
pub enum PersistQueueSettings {
    FilePersist(String, i32),
    MemOnly,
}

pub struct PersistQueue<T>
where
    T: Serialize + DeserializeOwned + Debug,
{
    queue: Arc<Mutex<VecDeque<T>>>,
    queue_name: String,
    queue_settings: PersistQueueSettings,
}

impl<T> PersistQueue<T>
where
    T: Serialize + DeserializeOwned + Debug,
{
    pub async fn new(queue_name: String, queue_settings: PersistQueueSettings) -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queue_name,
            queue_settings,
        }
    }

    pub async fn load_from_backup(
        queue_name: String,
        queue_settings: PersistQueueSettings,
    ) -> Self {
        Self {
            queue: Arc::new(Mutex::new(
                Self::load_persist_snapshot(queue_name.clone(), &queue_settings).await,
            )),
            queue_name,
            queue_settings,
        }
    }

    pub async fn enqueue(&mut self, value: T) {
        self.queue.lock().await.push_back(value);
    }

    pub async fn dequeue(&mut self) -> Option<T> {
        let mut lock = self.queue.lock().await;
        if lock.is_empty() {
            return None;
        }
        return lock.pop_front();
    }

    pub async fn dequeue_all(&mut self) -> Vec<T> {
        let mut lock = self.queue.lock().await;
        let mut result = vec![];

        while !lock.is_empty() {
            result.push(lock.pop_front().unwrap());
        }

        return result;
    }

    pub async fn force_persist(&self){
        persist_snapshot(&self.queue_settings, &self.queue_name, self.queue.clone()).await;
    }

    async fn load_persist_snapshot(name: String, settings: &PersistQueueSettings) -> VecDeque<T> {
        match settings {
            PersistQueueSettings::FilePersist(path, _) => {
                let filename = format!("{}-snapshot.json", name);

                let path = format!("{}/{}", path, filename);
                let snapshot = fs::read_to_string(path).await.unwrap();
                let snapshot: Vec<T> = serde_json::from_str(&snapshot).unwrap();

                return VecDeque::from(snapshot);
            }
            PersistQueueSettings::MemOnly => VecDeque::new(),
        }
    }
}

async fn persist_snapshot<T: Serialize>(
    queue_settings: &PersistQueueSettings,
    queue_name: &str,
    queue: Arc<Mutex<VecDeque<T>>>,
) {
    match queue_settings {
        PersistQueueSettings::FilePersist(path, _) => {
            let filename = format!("{}-snapshot.json", queue_name);
            let path = format!("{}/{}", path, filename);
            let path = Path::new(&path);
            fs::remove_dir_all(&path.parent().unwrap()).await;
            fs::create_dir_all(&path.parent().unwrap()).await;
            let mut file = File::create(path).await.unwrap();
            let to_persist = queue.lock().await;
            let to_persist: Vec<&T> = to_persist.iter().collect();
            let data_to_persist = serde_json::to_vec(&to_persist).unwrap();
            file.write(&data_to_persist.as_slice()).await.unwrap();
        }
        PersistQueueSettings::MemOnly => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_order_cases() {
        let mut queue: PersistQueue<String> = PersistQueue::new(
            "test".to_string(),
            PersistQueueSettings::FilePersist("./test/".to_string(), 2),
        )
        .await;

        let first = "test5".to_string();
        let second = "test".to_string();
        let third = "test1".to_string();
        let fourth = "test3".to_string();
        let fifth = "test4".to_string();

        queue.enqueue(first.clone()).await;
        queue.enqueue(second.clone()).await;
        queue.enqueue(third.clone()).await;
        queue.enqueue(fourth.clone()).await;
        queue.enqueue(fifth.clone()).await;

        assert_eq!(queue.dequeue().await, Some(first));
        assert_eq!(queue.dequeue().await, Some(second));
        assert_eq!(queue.dequeue().await, Some(third));
        assert_eq!(queue.dequeue().await, Some(fourth));
        assert_eq!(queue.dequeue().await, Some(fifth));
        assert_eq!(queue.dequeue().await, None);
    }

    #[tokio::test]
    async fn test_persist() {
        let queue_name = "test".to_string();
        let settings = PersistQueueSettings::FilePersist("./persist_queue/".to_string(), 2);

        let mut queue: PersistQueue<String> =
            PersistQueue::new(queue_name.clone(), settings.clone()).await;

        let first = "test5".to_string();
        let second = "test".to_string();
        let third = "test1".to_string();
        let fourth = "test3".to_string();
        let fifth = "test4".to_string();

        queue.enqueue(first.clone()).await;
        queue.enqueue(second.clone()).await;
        queue.enqueue(third.clone()).await;
        queue.enqueue(fourth.clone()).await;
        queue.enqueue(fifth.clone()).await;

        queue.force_persist().await;

        let mut queue: PersistQueue<String> =
            PersistQueue::load_from_backup(queue_name.clone(), settings.clone()).await;

        assert_eq!(queue.dequeue().await, Some(first));
        assert_eq!(queue.dequeue().await, Some(second));
        assert_eq!(queue.dequeue().await, Some(third));
        assert_eq!(queue.dequeue().await, Some(fourth));
        assert_eq!(queue.dequeue().await, Some(fifth));
        assert_eq!(queue.dequeue().await, None);
    }
}
