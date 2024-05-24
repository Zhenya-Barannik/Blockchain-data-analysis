use eyre::Result;
use petgraph::{graph::{NodeIndex,EdgeIndex}, Directed};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::runtime::Runtime;
use macroquad::prelude::*;
use fdg::{
    fruchterman_reingold::{FruchtermanReingold, FruchtermanReingoldConfiguration},
    simple::Center,
    Force, ForceGraph,
};
use petgraph::Graph;
use std::fs::File;
use std::io::{Read, Write};
use clap::{ValueEnum, Parser};
use std::fs;
use std::collections::HashSet;

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct TransactionResponse {
    status: String,
    message: String,
    result: Vec<Transaction>,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Serialize, Clone)]
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

const TRAVERSAL_STARTING_ADDRESS: &str = "0x60D170c2b604a4B613b43805aE4657476DCA9E38";
const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 4; // Depth of 1 will always be searched, so max depth of 0 is the same as max depth of 1.
const MAX_TOTAL_TRANSACTIONS: usize = 100000; // Limit of transactions at which parsing will be stopped.
const MAX_TRANSACTIONS_FROM_EACH_ADDRESS: usize = 500; // Limit of transactions to parse (from and to) one particular address.
const DATA_STORAGE_FOLDER: &str = "data";
const SECONDS_IN_DAY: usize = 86400;

async fn get_transactions_for_address(
    address: &str,
    client: &Client,
    api_key: &String,
) -> Result<TransactionResponse> {
    let start_block = "0";
    let end_block = "99999999";
    let page = "1";
    let sort = "desc";
    let offset = MAX_TRANSACTIONS_FROM_EACH_ADDRESS;

    let request_url = format!(
        "https://api.etherscan.io/api?module=account&action=txlist&address={}&startblock={}&endblock={}&page={}&offset={}&sort={}&apikey={}",
        address, start_block, end_block, page, offset, sort, api_key
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
    api_key: &String,
    adresses_to_check: Vec<String>,
) {
    let mut new_adresses_to_check: Vec<String> = vec![];
    
    'address_checking:  for address in adresses_to_check {
        if blockchain_graph.edge_count() >= MAX_TOTAL_TRANSACTIONS {break 'address_checking};
        let response = {
            loop {
                let attempt = get_transactions_for_address(&address, client, api_key).await;
                match attempt {
                    Err(e) => {
                        println!("Incorrect response for {}...:\n{}", &address[0..10], e);
                    }
                    Ok(t) => {
                        println!("Correct response for {}...", &address[0..10]);
                        break t;
                    }
                }
            }
        };
        for transaction in response.result.iter() {
            if transaction.contractAddress == "".to_string()
            && !edges.contains_key(&transaction.hash)  {

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
                    "Added transaction {}... --> {}... from block {}",
                    &transaction.from.as_str()[0..10],
                    &transaction.to.as_str()[0..10],
                    transaction.timeStamp
                );
            }
        }
    }

    if blockchain_graph.edge_count() >= MAX_TOTAL_TRANSACTIONS {return};

    if current_depth + 1 < MAX_GRAPH_TRAVERSAL_DEPTH {
        for address in new_adresses_to_check {
            let future = Box::pin(recursive_graph_traversion(
                current_depth + 1,
                blockchain_graph,
                node_indices,
                edges,
                client,
                api_key,
                vec![address],
            ));
            future.await;
        }
    }
}

async fn parse_blockchain(mut initial_blockchain_graph: Graph::<String, Transaction, Directed>, api_key: &String) -> Graph<String, Transaction> {
    let client = Client::new();
    let starting_adresses = vec![TRAVERSAL_STARTING_ADDRESS.to_string()];

    let mut node_indices = HashMap::new();
    for node_index in initial_blockchain_graph.node_indices() {
        let node_label = initial_blockchain_graph[node_index].clone();
        node_indices.insert(node_label, node_index);
    }

    let mut edges = HashMap::new();
    for edge_index in initial_blockchain_graph.edge_indices() {
        let edge_weight = initial_blockchain_graph[edge_index].clone();
        edges.insert(edge_weight.hash.clone(), edge_weight);
    }

    recursive_graph_traversion(
        0,
        &mut initial_blockchain_graph,
        &mut node_indices,
        &mut edges,
        &client,
        api_key,
        starting_adresses,
    )
    .await;

    initial_blockchain_graph
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

async fn draw_graph_highlighted(
    force_graph: &mut ForceGraph<f32, 3, String, Transaction>, 
    reversed_graph: &Graph<String, Transaction, Directed>
) {
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
            let (source_idx, target_idx) = force_graph.edge_endpoints(idx).unwrap();
            let (source_name, source_pos) = force_graph.node_weight(source_idx).unwrap();
            let (target_name, target_pos) = force_graph.node_weight(target_idx).unwrap();

            let transaction = force_graph.edge_weight(idx).unwrap();
            let color = if reversed_graph.contains_edge(source_idx, target_idx) {
                BLUE
            } else {
                BLACK
            };

            draw_line_3d(
                vec3(source_pos.coords.x, source_pos.coords.y, source_pos.coords.z),
                vec3(target_pos.coords.x, target_pos.coords.y, target_pos.coords.z),
                color,
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


fn calculate_total_transaction_value(graph: &Graph<String, Transaction, Directed>) -> f64 {
    graph.edge_indices()
        .map(|edge_index| {
            let transaction = &graph[edge_index];
            let value: f64 = transaction.value.parse().unwrap_or(0.0);
            value
        })
        .sum()
}


#[derive(Serialize, Deserialize)]
struct SerializableGraph {
    nodes: Vec<String>,
    edges: Vec<(usize, usize, Transaction)>,
}

fn serialize_graph(graph: &Graph<String, Transaction, Directed>, pathname: &str) -> Result<()> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for node in graph.node_indices() {
        nodes.push(graph[node].clone());
    }

    for edge in graph.edge_indices() {
        let (source, target) = graph.edge_endpoints(edge).unwrap();
        edges.push((source.index(), target.index(), graph[edge].clone()));
    }

    let serializable_graph = SerializableGraph { nodes, edges };
    let json = serde_json::to_string(&serializable_graph).unwrap();

    fs::create_dir_all(DATA_STORAGE_FOLDER).unwrap();
    let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
    let mut file = File::create(&file_pathname).unwrap();
    file.write_all(json.as_bytes()).unwrap();
    println!("\nSaved graph with {} Edges and {} Nodes as {}", &graph.edge_count(), &graph.node_count(), &file_pathname);
    Ok(())
}

fn deserialize_graph(pathname: &str) -> Result<Graph<String, Transaction, Directed>> {
    let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
    let mut json = String::new();
    
    println!("\nTrying to load {}", file_pathname);
    let mut file = File::open(&file_pathname).map_err(|_| eyre::eyre!(format!("File {} not found.", file_pathname)))
    .unwrap();

    file.read_to_string(&mut json).unwrap();

    let serializable_graph: SerializableGraph = serde_json::from_str(&json)?;

    let mut graph = Graph::new();
    let mut node_indices = Vec::new();

    for node in serializable_graph.nodes {
        node_indices.push(graph.add_node(node));
    }

    for (source, target, weight) in serializable_graph.edges {
        graph.add_edge(node_indices[source], node_indices[target], weight);
    }

    Ok(graph)
}


fn get_api_key() -> String {
    let mut api_key: String = String::new();
    File::open("api_key.txt")
        .map_err(|_| eyre::eyre!("Please provide an Etherscan API key inside of api_key.txt"))
        .unwrap()
        .read_to_string(&mut api_key).unwrap();
    api_key = api_key.trim().to_string();
    assert_ne!(api_key, "");
    api_key
}

#[derive(Parser, Debug)]
#[clap(name = "eth_parser")]
struct Opt {
    #[clap(value_enum, short, long, default_value_t = Mode::Load)]
    mode: Mode,
    
    #[clap(short, long, default_value_t = true)]
    draw: bool,

    #[clap(short, long, default_value = "example.json")]
    file: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Mode {
    Load,
    ParseSave,
    LoadParseSave,
    Analysis,
} 

fn find_reversed_transactions(
    graph: &Graph<String, Transaction, Directed>,
    max_time_difference_in_seconds: u64, 
) -> Graph<String, Transaction, Directed> {
    println!("Iterating over pairs of edges.");
    let mut reversed_graph = Graph::<String, Transaction, Directed>::new();
    let mut node_indices = HashMap::new();
    let mut paired_transactions = HashSet::new();

    for edge_a in graph.edge_indices() {
        let transaction_a = &graph[edge_a];
        let time_a: u64 = transaction_a.timeStamp.parse().unwrap();
        if paired_transactions.contains(&transaction_a.hash) {continue}

        for edge_b in graph.edge_indices() {
            let transaction_b = &graph[edge_b];
            if paired_transactions.contains(&transaction_b.hash) {continue}
            let time_b: u64 = transaction_b.timeStamp.parse().unwrap();

            if transaction_a.from == transaction_b.to &&
               transaction_a.to == transaction_b.from &&
               transaction_a.hash != transaction_b.hash &&
               time_a <= time_b &&
               time_b <= time_a + max_time_difference_in_seconds {
                    let from_a = *node_indices.entry(transaction_a.from.clone()).or_insert_with(|| reversed_graph.add_node(transaction_a.from.clone()));
                    let to_a = *node_indices.entry(transaction_a.to.clone()).or_insert_with(|| reversed_graph.add_node(transaction_a.to.clone()));
                    reversed_graph.add_edge(from_a, to_a, transaction_a.clone());

                    let from_b = *node_indices.entry(transaction_b.from.clone()).or_insert_with(|| reversed_graph.add_node(transaction_b.from.clone()));
                    let to_b = *node_indices.entry(transaction_b.to.clone()).or_insert_with(|| reversed_graph.add_node(transaction_b.to.clone()));
                    reversed_graph.add_edge(from_b, to_b, transaction_b.clone());

                    paired_transactions.insert(transaction_a.hash.clone());
                    paired_transactions.insert(transaction_b.hash.clone());

                    break;
            }
        }
    }

    reversed_graph
}

#[macroquad::main("Eth local graph")]
async fn main() {
    let opt = Opt::parse();
 
    match opt.mode {
        Mode::Load => {
            let initial_blockchain_graph = deserialize_graph(&opt.file).unwrap();
            if opt.draw {
                let mut force_graph: ForceGraph<f32, 3, String, Transaction> = fdg::init_force_graph_uniform(initial_blockchain_graph.clone(), 400.0);
                draw_graph(&mut force_graph).await;
            } 
        },
        Mode::ParseSave => {
            let api_key = get_api_key();
            let initial_blockchain_graph = Graph::<String, Transaction, Directed>::new();
            let rt = Runtime::new().unwrap();
            let graph = rt.block_on(parse_blockchain(initial_blockchain_graph, &api_key));
            serialize_graph(&graph, &opt.file).unwrap();
            
            if opt.draw {
                let mut force_graph: ForceGraph<f32, 3, String, Transaction> = fdg::init_force_graph_uniform(graph.clone(), 400.0);
                draw_graph(&mut force_graph).await;
            } 
        },

        Mode::LoadParseSave => {
            let api_key = get_api_key();
            let initial_blockchain_graph = deserialize_graph(&opt.file).unwrap();
            let rt = Runtime::new().unwrap();
            let graph = rt.block_on(parse_blockchain(initial_blockchain_graph, &api_key));
            serialize_graph(&graph, &opt.file).unwrap();
            
            if opt.draw {
                let mut force_graph: ForceGraph<f32, 3, String, Transaction> = fdg::init_force_graph_uniform(graph.clone(), 400.0);
                draw_graph(&mut force_graph).await;
            }
        },

        Mode::Analysis =>{
            let initial_blockchain_graph = Graph::<String, Transaction, Directed>::new();
            let api_key = get_api_key();
            let rt = Runtime::new().unwrap();
            let graph = rt.block_on(parse_blockchain(initial_blockchain_graph, &api_key));
            let reversed_transactions = find_reversed_transactions(&graph, SECONDS_IN_DAY as u64); 
            
            let total_transfered = calculate_total_transaction_value(&graph);
            let total_reversed_transfer = calculate_total_transaction_value(&reversed_transactions);
            
            dbg!(&total_transfered);
            dbg!(&total_reversed_transfer);
        
            serialize_graph(&graph, "all.json").unwrap(); 
            serialize_graph(&reversed_transactions, "reversed.json").unwrap(); 
        }
    };

}
