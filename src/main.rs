use eyre::Result;
use petgraph::graph::EdgeIndex;
use petgraph::visit::IntoEdges;
use petgraph::{graph::NodeIndex, visit::EdgeRef, Directed};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::isize;
use tokio::runtime::Runtime;
use petgraph::Graph;
use std::fs::File;
use std::io::{self, Read, Write};
use std::time::Instant;
use priority_queue::PriorityQueue;
use plotters::{coord::Shift, prelude::*};
use core::cmp::min;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct Response {
    status: String,
    message: String,
    result: Vec<RawTransaction>,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, Serialize, Clone)]
struct RawTransaction {
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

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, Serialize, Clone)]
struct Transaction {
    hash: String,
    data: Option<DigestedData>
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct DigestedData {
    payload: Payload,
    usd_value: f64,
    used_onchain_function: OnchainFunction,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct FilteringResultInfo {
    node_count_before_filtering: usize, // Before all nodigested transactions were filtered
    edge_count_before_filtering: usize, // Before all nodigested transactions were filtered
    node_count: usize,
    edge_count: usize,
}

#[derive(Serialize, Deserialize)]
struct SerializableGraph {
    info: FilteringResultInfo,
    nodes: Vec<String>,
    edges: Vec<(usize, usize, Transaction)>,
}

type G = Graph<String, Transaction, Directed>;

#[derive(Hash, PartialEq, Eq, Serialize, Deserialize, Debug, Clone, EnumIter)]
enum OnchainFunction {
    Transfer,
    TransferFrom,
}

#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, EnumIter)]
enum Payload {
    USDT = 1,
    USDC = 2,
}

const TRAVERSAL_STARTING_ADDRESS: &str = "0x760DcE7eA6e8BA224BFFBEB8a7ff4Dd1Ef122BfF";
const MAX_TRANSACTIONS_TO_PARSE: usize = 10_000_000;
const TRANSACTIONS_TO_REQUEST: usize = 10_000; // <= 10000. Limit of transactions to request (from and to) one particular address
const DATA_STORAGE_FOLDER: &str = "json";

static CONTRACT_ADDRESSES: Lazy<Mutex<HashMap<Payload, String>>> = Lazy::new(|| {
    let mut m = HashMap::new();
    let usdt_contract = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F".to_string().to_lowercase();
    let usdc_contract = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359".to_string().to_lowercase();
    m.insert(Payload::USDT, usdt_contract);
    m.insert(Payload::USDC, usdc_contract);
    Mutex::new(m)
});

struct OnchainFunctionDescription {
    method_id: String,
    function_name: String,
    input_lenth: usize,
    value_slice:(usize, usize),
    to_slice: (usize, usize),
    from_slice:Option<(usize, usize)>,
}

static METHOD_IDS: Lazy<Mutex<HashMap<OnchainFunction, OnchainFunctionDescription>>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(OnchainFunction::Transfer,
    OnchainFunctionDescription {
        method_id: "0xa9059cbb".to_string(),
        function_name:  "transfer(address dst, uint256 rawAmount)".to_string(),
        input_lenth: 138,
        to_slice: (10, 74),
        value_slice: (74, 138),
        from_slice: None,
    });
    m.insert(OnchainFunction::TransferFrom,
    OnchainFunctionDescription {
        method_id: "0x23b872dd".to_string(),
        function_name:  "transferFrom(address src, address dst, uint256 rawAmount)".to_string(),
        input_lenth: 202,
        from_slice: Some((10, 74)),
        to_slice: (74, 138),
        value_slice: (138, 202),
    });
    Mutex::new(m)
});

async fn get_transactions(address: &str, client: &Client, api_key: &String) -> Result<Response> {
    let start_block = "0";
    let end_block = "99999999";
    let page = "1";
    let sort = "desc";
    let offset = TRANSACTIONS_TO_REQUEST;

    let request_url = format!(
        "https://api.polygonscan.com/api?module=account&action=txlist&address={}&startblock={}&endblock={}&page={}&offset={}&sort={}&apikey={}",
        address, start_block, end_block, page, offset, sort, api_key
    );
    let response = client.get(&request_url).send().await?;

    if response.status().is_success() {
        let body_bytes = response.bytes().await?;
        match serde_json::from_slice::<Response>(&body_bytes) {
            Ok(parsed_response) => Ok(parsed_response),
            Err(_) => {
                let error_body = String::from_utf8_lossy(&body_bytes);
                Err(eyre::eyre!("Failed to decode JSON response: {}", error_body))
            }
        }
    } else {
        Err(eyre::eyre!("Response status errored."))
    }
}

async fn graph_data_collection_procedure(
    address_priority_pq: &mut PriorityQueue<String, i32>,
    blockchain_graph: &mut G,
    node_indices: &mut HashMap<String, NodeIndex>,
    edges: &mut HashMap<String, Transaction>,
    client: &Client,
    api_key: &String,
    address_to_check: String,
) {

    let response = {
        loop {
            let attempt = get_transactions(&address_to_check, client, api_key).await;
            match attempt {
                Err(e) => {
                    println!("Incorrect response for {}:\n{}", &address_to_check, e);
                }
                Ok(t) => {
                    println!("Correct response for {} with {} transactions", &address_to_check, t.result.len());
                    break t;
                }
            }
        }
    };

    let pq_timer: Instant = Instant::now();
    for transaction in response.result.iter() {
        if transaction.contractAddress == "".to_string()
        && transaction.isError == "0"
        && transaction.from != "GENESIS"
        && !edges.contains_key(&transaction.hash)
        {
            if !address_priority_pq.change_priority_by(&transaction.to, |x: &mut i32| { *x += 1 }){
                address_priority_pq.push(transaction.to.clone(), 1);
            }
            if !address_priority_pq.change_priority_by(&transaction.from, |x: &mut i32| { *x += 1 }){
                address_priority_pq.push(transaction.from.clone(), 1);
            }

            if transaction.value == "0".to_string() {
                'outer: for payload in Payload::iter() {
                    let contract_address_mutex = CONTRACT_ADDRESSES.lock().unwrap();
                    let contract_address = contract_address_mutex.get(&payload).unwrap();

                    for onchain_function in OnchainFunction::iter() {
                        let descriptions = METHOD_IDS.lock().unwrap();
                        let description = descriptions.get(&onchain_function).unwrap();

                        if transaction.to == contract_address.to_string()
                        && transaction.methodId == description.method_id
                        && transaction.input.len() == description.input_lenth
                        {
                            assert_eq!(&transaction.input[0..10], description.method_id, "{:?}", dbg!(transaction));
                            assert!(transaction.functionName == description.function_name, "{:?}", dbg!(transaction));

                            let (value_slice_low, value_slice_high) = description.value_slice;
                            let (to_slice_low, to_slice_high) = description.to_slice;

                            let real_transaction_source =
                                if description.from_slice.is_some() {
                                    let (from_slice_low, from_slice_high) = description.from_slice.unwrap();
                                    transaction.input[from_slice_low..from_slice_high].to_string()
                                } else {
                                    transaction.from.clone()
                                };

                            let real_transaction_destination = transaction.input[to_slice_low..to_slice_high].to_string(); // Real transaction destination
                            let usd_value = primitive_types::U256::from_str_radix(&transaction.input[value_slice_low..value_slice_high], 16)
                                .unwrap()
                                .as_u64()
                                .as_f64() / 1E6;

                            let digested_transaction = Transaction {
                                hash: transaction.hash.clone(),
                                data: Some(
                                    DigestedData {
                                        payload: payload.clone(),
                                        usd_value,
                                        used_onchain_function: onchain_function,
                                    }
                                )
                            };
                            let origin = *node_indices
                            .entry(real_transaction_source.clone())
                            .or_insert_with(|| {
                                blockchain_graph.add_node(real_transaction_source.clone())
                            });

                            let target = *node_indices
                            .entry(real_transaction_destination.clone())
                            .or_insert_with(|| {
                                blockchain_graph.add_node(real_transaction_destination.clone())
                                });

                            edges.insert(transaction.hash.clone(), digested_transaction.clone());
                            blockchain_graph.add_edge(origin, target, digested_transaction);

                            break 'outer
                        }
                    }
                }

                let not_digested_transaction = Transaction {
                    hash: transaction.hash.clone(),
                    data: None
                };

                let origin = *node_indices
                .entry(transaction.from.clone())
                .or_insert_with(|| {
                    blockchain_graph.add_node(transaction.from.clone())
                });

                // WARNING: The "target" may end up being not a real transaction destination, but a contract address.
                // This is a catch-all branch for not digested transactions.
                // Hash will be unique anyway.
                let target = *node_indices
                .entry(transaction.to.clone())
                .or_insert_with(|| {
                    blockchain_graph.add_node(transaction.to.clone())
                });

                edges.insert(transaction.hash.clone(), not_digested_transaction.clone());
                blockchain_graph.add_edge(origin, target, not_digested_transaction);
            }
        }
    }
    println!("Editing priority addresses and graph manipulation took {:<9} mks (PriorityQueue)", pq_timer.elapsed().as_micros());
}


async fn parse_blockchain(path_starting_address: String, api_key: &String) -> Graph<String, Transaction> {
    let client = Client::new();
    let mut blockchain_graph: Graph::<String, Transaction, Directed> = Graph::new();
    let mut node_indices = HashMap::new();
    let mut edges = HashMap::new();

    let mut path_history: Vec<String> = vec![];
    let mut path_priority_pq:PriorityQueue<String, i32> = PriorityQueue::new();
    path_priority_pq.push(path_starting_address.clone().to_lowercase(), 1);

    loop {
        let pq_timer: Instant = Instant::now();
        let next_address = loop {
            let (a, _) = path_priority_pq.pop().unwrap();
                if !path_history.contains(&a) {break a;}
        };
        println!("Searching for the next address took {:<9} mks (PriorityQueue)", pq_timer.elapsed().as_micros());

        path_history.push(next_address.clone());

        let future = graph_data_collection_procedure(
                &mut path_priority_pq,
                &mut blockchain_graph,
                &mut node_indices,
                &mut edges,
                &client,
                api_key,
                next_address.clone(),
            );
            future.await;

            let current_edge_count = blockchain_graph.edge_count();
            if current_edge_count >= MAX_TRANSACTIONS_TO_PARSE {return blockchain_graph};
            println!("Transaction count is {} / {}", current_edge_count, MAX_TRANSACTIONS_TO_PARSE);
        }
}

fn serialize_graph(filtered_graph: &G, info: &FilteringResultInfo, pathname: &str) -> Result<()> {
    assert_eq!(filtered_graph.raw_edges().len(), info.edge_count);
    assert_eq!(filtered_graph.raw_nodes().len(), info.node_count);

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    for node in filtered_graph.node_indices() {
        nodes.push(filtered_graph[node].clone());
    }
    for edge in filtered_graph.edge_indices() {
        let (source, target) = filtered_graph.edge_endpoints(edge).unwrap();
        edges.push((source.index(), target.index(), filtered_graph[edge].clone()));
    }

    let serializable_graph = SerializableGraph {info: info.clone(), nodes, edges };
    let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
    let file = File::create(&file_pathname)?;
    serde_json::to_writer_pretty(file, &serializable_graph)?;
    println!("\nSaved graph as {}\n", &file_pathname);
    Ok(())
}

// #[allow(dead_code)]
// fn deserialize_graph(pathname: &str) -> Result<G> {
//     let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
//     let mut json = String::new();

//     println!("\nTrying to load {}", file_pathname);
//     let mut file = File::open(&file_pathname).map_err(|_| eyre::eyre!(format!("File {} not found.", file_pathname)))
//     .unwrap();

//     file.read_to_string(&mut json).unwrap();

//     let serializable_graph: SerializableGraph = serde_json::from_str(&json)?;

//     let mut graph = Graph::new();
//     let mut node_indices = Vec::new();

//     for node in serializable_graph.nodes {
//         node_indices.push(graph.add_node(node));
//     }

//     for (source, target, weight) in serializable_graph.edges {
//         graph.add_edge(node_indices[source], node_indices[target], weight);
//     }

//     Ok(graph)
// }

fn read_api_key() -> String {
    let mut api_key: String = String::new();
    File::open("api_key.txt")
        .map_err(|_| eyre::eyre!("Please provide an Etherscan API key (put it inside api_key.txt)"))
        .unwrap()
        .read_to_string(&mut api_key).unwrap();
    api_key = api_key.trim().to_string();
    assert_ne!(api_key, "");
    api_key
}

fn filter_stablecoin_transactions_by_value(graph: &G, lower_usd_bound: f64, upper_usd_bound: f64) -> (G, FilteringResultInfo) {
    let filtered_graph_stage1: G = graph
        .filter_map(
            |_node_index, node| Some(node.clone()), // All nodes remain
            |_, transaction|
            if transaction.data.is_some() {
                let usd_value = transaction.data.as_ref().unwrap().usd_value;
                if usd_value >= lower_usd_bound && usd_value <= upper_usd_bound {Some(transaction.clone()) } else {None}
            } else {None}
    );
    let filtered_graph_stage2 = filtered_graph_stage1.filter_map(
            |node_index, node| (filtered_graph_stage1.neighbors_undirected(node_index).count() != 0).then_some(node.clone()),
            |_edge_index, transaction| Some(transaction.clone()), // All transactions remain
    );

    let info = FilteringResultInfo {
            node_count: filtered_graph_stage2.raw_nodes().len(),
            edge_count: filtered_graph_stage2.raw_edges().len(),
            node_count_before_filtering: graph.raw_nodes().len(),
            edge_count_before_filtering: graph.raw_edges().len(),
    };

    (filtered_graph_stage2, info)
}

fn calculate_total_usd_volume(graph: &G) -> (f64, f64) {
    let mut total_volume_usd = 0.0;
    for edge in graph.edge_references() {
        let transaction = edge.weight();
            if transaction.data.is_some() {
                total_volume_usd += transaction.data.as_ref().unwrap().usd_value;
            }
    }
    let mean_value_usd = total_volume_usd / graph.edge_count() as f64;
    (total_volume_usd, mean_value_usd)
}

fn plot_distribution_multicolor(graph: &G, root: &mut DrawingArea<BitMapBackend<'_>, Shift>, min_log_value: f64, description: &str) {
    let colors = vec![BLUE.mix(0.5), RED.mix(0.5)];
    assert_eq!(colors.len(), Payload::iter().len());

    let mixed_transaction_log_values = graph
        .raw_edges()
        .iter()
        .filter(|t|t.weight.data.is_some())
        .map(|t|
                f64::log10(t.weight.data.as_ref().unwrap().usd_value)
            )
        .collect::<Vec<f64>>();

    let mixed_max_log_value = mixed_transaction_log_values
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(&2.0)
        .clone();

    let bucket_count = 200;
    let bucket_width = (mixed_max_log_value-min_log_value) / bucket_count as f64;
    let mut mixed_buckets: Vec<u32> = vec![0u32; bucket_count];

    for log_value in mixed_transaction_log_values.iter() {
        let bucket_index = min((((log_value - min_log_value) / (mixed_max_log_value - min_log_value)) * (bucket_count as f64)).floor() as usize, bucket_count - 1);
        mixed_buckets[bucket_index] += 1;
    }

    let mixed_max_count = mixed_buckets.clone()[1..].iter().max().unwrap().clone(); // slicing to skip 0-valued transactions
    drop(mixed_buckets);

    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .margin(5)
        .caption(description, ("sans-serif", 20))
        .x_label_area_size(40)
        .y_label_area_size(40)
        .build_cartesian_2d(0.0 .. 9.0 , 0 as u32 ..mixed_max_count as u32)
        .unwrap();
    chart.configure_mesh().x_desc("log10(value in USD), at the moment of transaction").y_desc("Number of transactions").draw().unwrap();

      for (i, payload) in Payload::iter().enumerate() {
          let transaction_log_values = graph
          .raw_edges()
          .iter()
          .filter(|t|t.weight.data.is_some() && t.weight.data.as_ref().unwrap().payload == payload)
          .map(|t|
                  f64::log10(t.weight.data.as_ref().unwrap().usd_value)
              )
          .collect::<Vec<f64>>();

          let mut buckets: Vec<u32> = vec![0u32; bucket_count];

          for log_value in transaction_log_values.iter() {
              let bucket_index = min((((log_value - min_log_value) / (mixed_max_log_value - min_log_value)) * (bucket_count as f64)).floor() as usize, bucket_count - 1);
              buckets[bucket_index] += 1;
          }

          let mut rectangles_to_draw = vec![];

          for (bucket_index, &count) in buckets.iter().enumerate() {
              let bar_left = min_log_value + bucket_index as f64 * bucket_width;
              let bar_right = bar_left + bucket_width;
              let bar_top = ((count as f64 / mixed_max_count as f64) * mixed_max_count as f64) as u32;

                  rectangles_to_draw.push(
                      Rectangle::new(
                          [(bar_left, 0), (bar_right, bar_top)],
                          colors[i].filled()
                      )
                  );
          }

          chart.draw_series(rectangles_to_draw).unwrap();
      }

      let (legend_x, legend_y) = ( root.dim_in_pixel().0 as i32 / 5 * 4, root.dim_in_pixel().1 as i32 / 5 * 1);
      for (i, payload) in Payload::iter().enumerate() {
          root.draw(&Text::new(
              format!("{:?}", payload),
              (legend_x + 20, legend_y - i as i32 * 20),
              ("sans-serif", 15),
          ))
          .unwrap();
          root.draw(&Rectangle::new(
              [(legend_x, legend_y - i as i32 * 20), (legend_x + 15, legend_y - i as i32 * 20 + 10)],
              colors[i].filled(),
          ))
          .unwrap();
      }

    root.present().unwrap();
}

fn filtering_by_value(graph: &G, lower_usd_bound: f64, upper_usd_bound: f64, result_log: &mut String) -> (G, FilteringResultInfo) {
    assert!(lower_usd_bound >= 0.0);
    assert!(upper_usd_bound >= lower_usd_bound);
    let (filtered_graph, filtering_info) = filter_stablecoin_transactions_by_value(&graph, lower_usd_bound, upper_usd_bound);
    let (filtered_graph_volume, filtered_graph_mean_value) = calculate_total_usd_volume(&filtered_graph);

    let filtering_log = format!(
        "For transactions filtered in {:.3e} to {:.3e} Range:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, Edges: {}, Nodes: {}\n{:#?}",
        lower_usd_bound, upper_usd_bound, filtered_graph_volume, filtered_graph_mean_value, filtered_graph.edge_count(), filtered_graph.node_count(), filtering_info
    );
    println!("{}", &filtering_log);
    result_log.push_str(&filtering_log);
    result_log.push_str(&"\n");
    (filtered_graph, filtering_info)
}

fn filtering_by_variant(graph: &G, required_variant: Payload, results_log: &mut String) -> (G, FilteringResultInfo) {
    let filtered_graph_stage1: G = graph
        .filter_map(
            |_node_index, node| Some(node.clone()), // All nodes remain
            |_, transaction|
            if transaction.data.is_some() {
                let variant = transaction.data.as_ref().unwrap().payload.clone();
                if variant == required_variant {Some(transaction.clone())} else {None}
            } else {None}
    );
    let filtered_graph_stage2 = filtered_graph_stage1.filter_map(
            |node_index, node| (filtered_graph_stage1.neighbors_undirected(node_index).count() != 0).then_some(node.clone()),
            |_edge_index, transaction| Some(transaction.clone()), // All transactions remain
    );

    let filtering_info = FilteringResultInfo {
            node_count: filtered_graph_stage2.raw_nodes().len(),
            edge_count: filtered_graph_stage2.raw_edges().len(),
            node_count_before_filtering: graph.raw_nodes().len(),
            edge_count_before_filtering: graph.raw_edges().len(),
    };

    let (filtered_graph_volume, filtered_graph_mean_value) = calculate_total_usd_volume(&filtered_graph_stage2);
    let filtering_log = format!(
        "\nFor {:?} transactions:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, Edges: {}, Nodes: {}\n{:#?}",
        required_variant, filtered_graph_volume, filtered_graph_mean_value, filtered_graph_stage2.edge_count(), filtered_graph_stage2.node_count(), filtering_info
    );
    println!("{}", &filtering_log);
    results_log.push_str(&filtering_log);
    results_log.push_str(&"\n");

    (filtered_graph_stage2, filtering_info)
}

fn main() {
    let async_timer: Instant = Instant::now();
    let api_key = read_api_key();
    let rt = Runtime::new().unwrap();
    let mut result_log = String::new();
    let parsed_graph = rt.block_on(parse_blockchain(TRAVERSAL_STARTING_ADDRESS.to_string(), &api_key));
    println!("Async operations took {:.3} s\n", async_timer.elapsed().as_secs_f64());

    let local_timer: Instant = Instant::now();
    let (parsed_graph_volume, _) = calculate_total_usd_volume(&parsed_graph);
    let parsed_s = format!(
        "For all parsed transactions:\nTotal volume: {:.0} USD, Edges: {}, Nodes: {}\n\n",
        parsed_graph_volume, parsed_graph.edge_count(), parsed_graph.node_count()
    );
    print!("{}", &parsed_s);
    result_log.push_str(&parsed_s);

    let (nonzero_graph, nonzero_filtering_info) = filtering_by_value(&parsed_graph, 1.0E-9, f64::MAX, &mut result_log);
    let _ = serialize_graph(&nonzero_graph, &nonzero_filtering_info, "filtered_transactions_polygon.json").unwrap();
    let mut graph_multicolor_root = BitMapBackend::new(&"main_graph_multicolor.png", (720, 480)).into_drawing_area();
    plot_distribution_multicolor(&nonzero_graph, &mut graph_multicolor_root, 0.0, "Value distribution for transactions with tokens (for all parsed transactions)");

    for variant in Payload::iter() {
        let (variant_graph, _variant_filtering_info) = filtering_by_variant(&nonzero_graph, variant.clone(), &mut result_log);
        let (_range_graph, _range_filtering_info) = filtering_by_value(&variant_graph, 10.0, 1000.0, &mut result_log);
    }

    let mut log_file_main= File::create("result.txt").unwrap();
    write!(log_file_main, "{}", result_log).unwrap();

    println!("Local operations took {:.3} s", local_timer.elapsed().as_secs_f64());
    println!("Local + async operations took {:.3} s", async_timer.elapsed().as_secs_f64());
}
