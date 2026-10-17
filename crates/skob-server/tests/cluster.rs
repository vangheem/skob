use std::time::Duration;

use serde_json::json;
use skob_core::segment_for;
use skob_core::ObjectId;
use skob_server::cluster::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_writers_two_readers_survive_leader_loss() {
    let mut cluster = Cluster::start(3, 2).await.expect("cluster");
    let owners = cluster.owner_ids().await.expect("owners");
    assert!(
        owners.len() >= 2,
        "raft rebalance should spread segments across writers, got {owners:?}"
    );

    cluster
        .post_following("/v1/projects", &json!({"id": "p1", "name": "Demo"}))
        .await
        .expect("project");
    cluster
        .post_following("/v1/projects/p1/collections", &json!({"id": "docs", "name": "Docs"}))
        .await
        .expect("collection");

    let metrics = cluster.metrics(&cluster.writers[0].addr).await.expect("metrics");
    let (segment, owner_id) = metrics["assignments"]
        .as_object()
        .unwrap()
        .iter()
        .find_map(|(segment, owner)| {
            let owner_id = owner["node_id"].as_u64()?;
            let segment = segment.parse().ok()?;
            (owner_id != cluster.writers[0].id).then_some((segment, owner_id))
        })
        .expect("a segment owned by someone other than writer 1");
    let object_id = id_for_segment(segment, cluster.segment_count);
    let rejected = reqwest::Client::new()
        .put(format!(
            "{}/v1/projects/p1/collections/docs/objects/{object_id}",
            cluster.writers[0].addr
        ))
        .header("content-type", "application/json")
        .json(&json!({"title": "rejected"}))
        .send()
        .await
        .expect("non-owner write");
    assert_eq!(rejected.status().as_u16(), 409, "non-owner must refuse the write");
    let rejected: serde_json::Value = rejected.json().await.expect("error body");
    assert_eq!(rejected["error"], "not_owner");
    assert_eq!(rejected["owner_id"], owner_id);
    assert_eq!(rejected["segment"], segment);

    let written = cluster
        .put_following(
            &format!("/v1/projects/p1/collections/docs/objects/{object_id}"),
            &json!({"title": "harbor", "user": {"address": {"city": "portland"}}}),
        )
        .await
        .expect("owner write");
    assert_eq!(written["id"], object_id.as_str());

    for reader in 0..2 {
        let got = cluster
            .get_reader(
                reader,
                &format!("/v1/projects/p1/collections/docs/objects/{object_id}"),
            )
            .await
            .expect("reader get");
        assert_eq!(got["body"]["title"], "harbor");
        assert_eq!(got["body"]["user"]["address"]["city"], "portland");
    }

    let leader = cluster.leader_addr().await.expect("leader");
    let leader_id = cluster
        .writers
        .iter()
        .find(|node| node.addr == leader)
        .map(|node| node.id)
        .expect("leader node");
    cluster.stop_writer(leader_id).await.expect("stop leader");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut new_leader = None;
    while std::time::Instant::now() < deadline {
        if let Ok(addr) = cluster.leader_addr().await {
            if addr != leader {
                new_leader = Some(addr);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let new_leader = new_leader.expect("a surviving writer must win the election");
    let rebalanced = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{new_leader}/v1/raft/rebalance"))
        .json(&json!({}))
        .send()
        .await
        .expect("rebalance request");
    assert!(
        rebalanced.status().is_success(),
        "rebalance after election: {}",
        rebalanced.text().await.unwrap_or_default()
    );

    let after = id_for_segment(segment, cluster.segment_count);
    cluster
        .put_following(
            &format!("/v1/projects/p1/collections/docs/objects/{after}"),
            &json!({"title": "after-failover"}),
        )
        .await
        .expect("write after failover");
    let got = cluster
        .get_reader(0, &format!("/v1/projects/p1/collections/docs/objects/{after}"))
        .await
        .expect("reader sees post-failover write");
    assert_eq!(got["body"]["title"], "after-failover");

    cluster.shutdown().await;
}

fn id_for_segment(segment: u32, count: u32) -> ObjectId {
    for n in 0..20_000 {
        let id = ObjectId::parse(format!("obj{n}")).unwrap();
        if segment_for(&id, count) == segment {
            return id;
        }
    }
    panic!("no object id maps to segment {segment}");
}
