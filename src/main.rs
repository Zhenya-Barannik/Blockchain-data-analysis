use eyre::Result;
use petgraph::{graph::NodeIndex, Directed};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use macroquad::prelude::*;
use fdg::{
    fruchterman_reingold::{FruchtermanReingold, FruchtermanReingoldConfiguration},
    petgraph::Graph,
    simple::Center,
    Force, ForceGraph,
};

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct TransactionResponse {
    status: String,
    message: String,
    result: Vec<Transaction>,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Clone)]
struct Transaction {
    blockHash: String,
    blockNumber: String,
    from: String,
    to: String,
    gas: String,
    gasPrice: String,
    gasUsed: String,
    hash: String,
    value: String,
    nonce: String,
    transactionIndex: String,
    timeStamp: String,
    isError: String,
    txreceipt_status: String,
    input: String,
    contractAddress: String,
    cumulativeGasUsed: String,
    functionName: String,
    methodId: String,
}

const API_KEY: Option<&str> = None;
const TRAVERSAL_STARTING_ADDRESS: &str = "0x60D170c2b604a4B613b43805aE4657476DCA9E38";
const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 4; // Depth of 1 will always be searched, so max depth of 0 is the same as max depth of 1.
const MAX_TOTAL_TRANSACTIONS: usize = 100; // Limit of transactions at which parsing will be stopped.
const MAX_TRANSACTIONS_FROM_EACH_ADDRESS: usize = 20; // Limit of transactions to parse (from and to) one particular address.
const TO_RUN_VISUALISATION: bool = true; 

async fn get_transactions_for_address(
    address: &str,
    client: &Client,
    api_key: Option<&str>,
) -> Result<TransactionResponse> {
    let start_block = "0";
    let end_block = "99999999";
    let page = "1";
    let sort = "desc";
    let key = if let Some(key) = api_key {key} else {panic!("Please set API_KEY.")};
    let max_transactions_for_each_adress = MAX_TRANSACTIONS_FROM_EACH_ADDRESS.to_string();
    let offset = max_transactions_for_each_adress.as_str();

    let request_url = format!(
        "https://api.etherscan.io/api?module=account&action=txlist&address={}&startblock={}&endblock={}&page={}&offset={}&sort={}&apikey={}",
        address, start_block, end_block, page, offset, sort, key
    );
    let response = client.get(&request_url).send().await.unwrap();

    if response.status().is_success() {
        let body_bytes = response.bytes().await?;
        match serde_json::from_slice::<TransactionResponse>(&body_bytes) {
            Ok(parsed_response) => Ok(parsed_response),
            Err(_) => {
                let error_body = String::from_utf8_lossy(&body_bytes);
                Err(eyre::eyre!(
                    "Failed to decode JSON response: {}",
                    error_body
                ))
            }
        }
    } else {
        Err(eyre::eyre!("Response status errored."))
    }
}

async fn recursive_graph_traversion(
    current_depth: usize,
    blockchain_graph: &mut Graph<String, Transaction, Directed>,
    node_indices: &mut HashMap<String, NodeIndex>,
    edges: &mut HashMap<String, Transaction>,
    client: &Client,
    adress_to_check: Vec<String>,
) {
    let mut new_adresses_to_check: Vec<String> = vec![];
    for address in adress_to_check {
        let response = {
            loop {
                let attempt = get_transactions_for_address(&address, client, API_KEY).await;
                match attempt {
                    Err(e) => {
                        println!("Attempting again for {}...: {}", &address[0..10], e);
                    }
                    Ok(t) => {
                        println!("Correct response for {}...", &address[0..10]);
                        break t;
                    }
                }
            }
        };

        for transaction in response.result.iter() {

            if !edges.contains_key(&transaction.hash) && blockchain_graph.edge_count() < MAX_TOTAL_TRANSACTIONS {
                let origin = *node_indices
                    .entry(transaction.from.clone())
                    .or_insert_with(|| {
                        new_adresses_to_check.push(transaction.from.clone());
                        blockchain_graph.add_node(transaction.from.clone())
                    });
                let target = *node_indices
                    .entry(transaction.to.clone())
                    .or_insert_with(|| {
                        new_adresses_to_check.push(transaction.to.clone());
                        blockchain_graph.add_node(transaction.to.clone())
                    });

                blockchain_graph.add_edge(origin, target, transaction.clone());
                edges.insert(transaction.hash.clone(), transaction.clone());
                println!(
                    "Added transaction {}... --> {}... | from block {}",
                    &transaction.from.as_str()[0..10],
                    &transaction.to.as_str()[0..10],
                    transaction.timeStamp
                );
            }
        }
    }

    if current_depth + 1 < MAX_GRAPH_TRAVERSAL_DEPTH {
        for address in new_adresses_to_check {
            let future = Box::pin(recursive_graph_traversion(
                current_depth + 1,
                blockchain_graph,
                node_indices,
                edges,
                client,
                vec![address],
            ));
            future.await;
        }
    }
}

async fn get_graph() -> Graph<String, Transaction> {
    let client = Client::new();
    let starting_adresses = vec![TRAVERSAL_STARTING_ADDRESS.to_string()];

    let mut blockchain_graph = Graph::<String, Transaction, Directed>::new();
    let mut node_indices: HashMap<String, NodeIndex> = HashMap::new();
    let mut edges: HashMap<String, Transaction> = HashMap::new();
    recursive_graph_traversion(
        0,
        &mut blockchain_graph,
        &mut node_indices,
        &mut edges,
        &client,
        starting_adresses,
    )
    .await;

    blockchain_graph
}

async fn draw_graph(force_graph: &mut ForceGraph<f32, 3, String, Transaction>) {
    let mut angle: f32 = 0.0; 
    let radius = 800.0; 
    
    let mut force = FruchtermanReingold {
        conf: FruchtermanReingoldConfiguration {
            scale: 400.0,
            ..Default::default()
        },
        ..Default::default()
    };

    loop {
        force.apply_many(force_graph, 1);
        Center::default().apply(force_graph);
        clear_background(WHITE);

        angle += 0.01; // Camera angle rotation
        if angle > 2.0 * 3.1416 {
            angle -= 2.0 * 3.1416;
        }
        let camera_x: f32 = radius * angle.cos();
        let camera_z = radius * angle.sin();

        set_camera(&Camera3D {
            position: vec3(camera_x, 0.0, camera_z),
            up: vec3(0., 1., 0.),
            target: vec3(0., 0., 0.), 
            ..Default::default()
        });

        for idx in force_graph.edge_indices() {
            let ((_, source), (_, target)) = force_graph
                .edge_endpoints(idx)
                .map(|(a, b)| {
                    (
                        force_graph.node_weight(a).unwrap(),
                        force_graph.node_weight(b).unwrap(),
                    )
                })
                .unwrap();

            draw_line_3d(
                vec3(source.coords.x, source.coords.y, source.coords.z),
                vec3(target.coords.x, target.coords.y, target.coords.z),
                BLACK,
            );
        }

        for (name, pos) in force_graph.node_weights() {
            draw_sphere(
                vec3(pos.coords.x, pos.coords.y, pos.coords.z),
                if name.as_str() == TRAVERSAL_STARTING_ADDRESS.to_lowercase() {6.0} else {2.0},
                None,
                if name.as_str() == TRAVERSAL_STARTING_ADDRESS.to_lowercase() {BLUE} else {RED},
            );
        }

        next_frame().await
    }
}

#[macroquad::main("Eth local graph")] // Comment if you do not need a visualisation
async fn main() {  // Remove async if you do not need a visualisation
    let rt = Runtime::new().unwrap();
    let graph = rt.block_on(get_graph());
    println!("Parsed Graph:\n{:#?}", &graph);

    if TO_RUN_VISUALISATION { // Comment this block if you do not need a visualisation
        let mut force_graph: ForceGraph<f32, 3, String, Transaction> = fdg::init_force_graph_uniform(graph.clone(), 400.0);
        draw_graph(&mut force_graph).await;
    }
}
