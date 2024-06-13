use eyre::Result;
use petgraph::{graph::NodeIndex, visit::EdgeRef, Directed};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use tokio::runtime::Runtime;
use petgraph::Graph;
use std::fs::File;
use std::io::{Write, Read};
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
    onchain_function: OnchainFunction,
}


#[derive(Serialize, Deserialize)]
struct SerializableGraph {
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
    USDT,
    USDC,
}

const TRAVERSAL_STARTING_ADDRESS: &str = "0x21a31ee1afc51d94c2efccaa2092ad1028285549"; // 0x21a31ee1... is the Binance affiliated address
const MAX_TRANSACTIONS_TO_PARSE: usize = 100_000; // Transaction number at which parsing will be stopped.
const TRANSACTIONS_TO_REQUEST: usize = 10_000; // <= 10000. Limit of transactions to request (from and to) one particular address,
const DATA_STORAGE_FOLDER: &str = "json";

static CONTRACT_ADDRESSES: Lazy<Mutex<HashMap<Payload, &str>>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(Payload::USDT, "0xdac17f958d2ee523a2206206994597c13d831ec7");
    m.insert(Payload::USDC, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    Mutex::new(m)
});

struct FunctionDescription {
    method_id: String,
    function_name: String,
    input_lenth: usize,
    value_slice:(usize, usize),
    to_slice: (usize, usize),
    from_slice:Option<(usize, usize)>,
}

static METHOD_IDS: Lazy<Mutex<HashMap<OnchainFunction, FunctionDescription>>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(OnchainFunction::Transfer,
    FunctionDescription {
        method_id: "0xa9059cbb".to_string(),
        function_name:  "transfer(address _to, uint256 _value)".to_string(),
        input_lenth: 138,
        value_slice: (74, 138),
        to_slice: (10, 74),
        from_slice: None,
    });
    m.insert(OnchainFunction::TransferFrom,
    FunctionDescription {
        method_id: "0x23b872dd".to_string(),
        function_name:  "transferFrom(address _from, address _to, uint256 _value)".to_string(),
        input_lenth: 202,
        value_slice: (138, 202),
        to_slice: (74, 138),
        from_slice: Some((10, 74)),
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
        "https://api.etherscan.io/api?module=account&action=txlist&address={}&startblock={}&endblock={}&page={}&offset={}&sort={}&apikey={}",
        address, start_block, end_block, page, offset, sort, api_key
    );
    let response = client.get(&request_url).send().await?;

    if response.status().is_success() {
        let body_bytes = response.bytes().await?;
        match serde_json::from_slice::<Response>(&body_bytes) {
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
                    let contract_address = *CONTRACT_ADDRESSES.lock().unwrap().get(&payload).unwrap();
                    for onchain_function in OnchainFunction::iter() {

                        let descriptions = METHOD_IDS.lock().unwrap();
                        let description = descriptions.get(&onchain_function).unwrap();

                        if transaction.to == contract_address.to_string()
                        && transaction.methodId == description.method_id
                        && transaction.input.len() == description.input_lenth
                        {
                            assert_eq!(&transaction.input[0..10], description.method_id);
                            assert!(transaction.functionName == description.function_name);

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

                            let simplified_transaction = Transaction {
                                hash: transaction.hash.clone(),
                                data: Some(
                                    DigestedData {
                                        payload: payload.clone(),
                                        usd_value,
                                        onchain_function,
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

                            edges.insert(transaction.hash.clone(), simplified_transaction.clone());
                            blockchain_graph.add_edge(origin, target, simplified_transaction);

                            break 'outer
                        }
                    }
                }

                let not_digested = Transaction {
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

                edges.insert(transaction.hash.clone(), not_digested.clone());
                blockchain_graph.add_edge(origin, target, not_digested);
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

    fn serialize_graph(graph: &G, pathname: &str) -> Result<()> {
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
    let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
    let file = File::create(&file_pathname)?;
    serde_json::to_writer_pretty(file, &serializable_graph)?;

    println!("\nSaved graph with {} edges and {} nodes as {}\n", &graph.edge_count(), &graph.node_count(), &file_pathname);
    Ok(())
}


#[allow(dead_code)]
fn deserialize_graph(pathname: &str) -> Result<G> {
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
        .map_err(|_| eyre::eyre!("Please provide an Etherscan API key (put it inside api_key.txt)"))
        .unwrap()
        .read_to_string(&mut api_key).unwrap();
    api_key = api_key.trim().to_string();
    assert_ne!(api_key, "");
    api_key
}


fn filter_by_transaction_price(graph: &G, lower_usd_bound: f64, higher_usd_bound: f64) -> G {
    let mut filtered_graph = graph.clone();
    filtered_graph.clear_edges();

    for edge in graph.edge_references() {
        let transaction = edge.weight();
        if transaction.data.is_some() {
            if lower_usd_bound <=  transaction.data.as_ref().unwrap().usd_value
            &&  transaction.data.as_ref().unwrap().usd_value <= higher_usd_bound {
                filtered_graph.add_edge(edge.source(), edge.target(), transaction.clone());
            }
        }
    }

    filtered_graph
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

    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .margin(5)
        .caption(description, ("sans-serif", 20))
        .x_label_area_size(40)
        .y_label_area_size(40)
        .build_cartesian_2d(0.0 .. 9.0 , 0 as u32 ..1_000 as u32)
        .unwrap();
    chart.configure_mesh().x_desc("log10(value in USD), at the moment of transaction").y_desc("N").draw().unwrap();

      for (i, payload) in Payload::iter().enumerate() {
          let transaction_log_values = graph
          .raw_edges()
          .iter()
          .filter(|t|t.weight.data.is_some() && t.weight.data.as_ref().unwrap().payload == payload)
          .map(|t|
                  f64::log10(t.weight.data.as_ref().unwrap().usd_value)
              )
          .collect::<Vec<f64>>();

          let max_log_value = transaction_log_values
              .iter()
              .max_by(|a, b| a.partial_cmp(b).unwrap())
              .unwrap_or(&2.0)
              .clone();

          let bucket_count = 200;
          let bucket_width = (max_log_value-min_log_value) / bucket_count as f64;
          let mut buckets: Vec<u32> = vec![0u32; bucket_count];

          for log_value in transaction_log_values.iter() {
              let bucket_index = min((((log_value - min_log_value) / (max_log_value - min_log_value)) * (bucket_count as f64)).floor() as usize, bucket_count - 1);
              buckets[bucket_index] += 1;
          }

          let max_count = buckets.clone()[1..].iter().max().unwrap().clone(); // slicing to skip 0-valued transactions
          let mut rectangles_to_draw = vec![];

          for (bucket_index, &count) in buckets.iter().enumerate() {
              let bar_left = min_log_value + bucket_index as f64 * bucket_width;
              let bar_right = bar_left + bucket_width;
              let bar_top = ((count as f64 / max_count as f64) * max_count as f64) as u32;

                  rectangles_to_draw.push(
                      Rectangle::new(
                          [(bar_left, 0), (bar_right, bar_top)],
                          colors[i].filled()
                      )
                  );

          }

          let (legend_x, legend_y) = ( root.dim_in_pixel().0 as i32 / 5 * 4, root.dim_in_pixel().1 as i32 / 5 * 1); // Starting position for the legend
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
          chart.draw_series(rectangles_to_draw).unwrap();
      }
    root.present().unwrap();
}

fn plot_distribution(graph: &G, root: &mut DrawingArea<BitMapBackend<'_>, Shift>, min_log_value: f64, description: &str) {
    let transaction_log_values = graph
    .raw_edges()
    .iter()
    .filter(|t|t.weight.data.is_some())
    .map(|t|
            f64::log10(t.weight.data.as_ref().unwrap().usd_value)
        )
    .collect::<Vec<f64>>();

    let max_log_value = transaction_log_values
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(&2.0)
        .clone();

    let bucket_count = 200;
    let bucket_width = (max_log_value-min_log_value) / bucket_count as f64;
    let mut buckets: Vec<u32> = vec![0u32; bucket_count];

    for log_value in transaction_log_values.iter() {
        let bucket_index = min((((log_value - min_log_value) / (max_log_value - min_log_value)) * (bucket_count as f64)).floor() as usize, bucket_count - 1);
        buckets[bucket_index] += 1;
    }

    let max_count = buckets.clone()[1..].iter().max().unwrap().clone(); // slicing to skip 0-valued transactions
    let mut rectangles_to_draw = vec![];

    for (bucket_index, &count) in buckets.iter().enumerate() {
        let bar_left = min_log_value + bucket_index as f64 * bucket_width;
        let bar_right = bar_left + bucket_width;
        let bar_top = ((count as f64 / max_count as f64) * max_count as f64) as u32;

            rectangles_to_draw.push(
                Rectangle::new(
                    [(bar_left, 0), (bar_right, bar_top)],
                    BLUE.filled()
                )
            );
    }

    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
    .margin(5)
    .caption(description, ("sans-serif", 20))
    .x_label_area_size(40)
    .y_label_area_size(40)
    .build_cartesian_2d(min_log_value..max_log_value, 0..max_count)
    .unwrap();
    chart.configure_mesh().x_desc("log10(value in USD), at the moment of transaction").y_desc("N").draw().unwrap();



    chart.draw_series(rectangles_to_draw).unwrap();
    root.present().unwrap();
}

fn main() {
    let async_timer: Instant = Instant::now();
    let api_key = get_api_key();
    let rt = Runtime::new().unwrap();
    let graph = rt.block_on(parse_blockchain(TRAVERSAL_STARTING_ADDRESS.to_string(), &api_key));
    println!("Async operations took {:.3} s", async_timer.elapsed().as_secs_f64());
    let timer: Instant = Instant::now();
    let mut result_log = String::new();
    let _ = serialize_graph(&graph, "parsed_tokens.json").unwrap();

    let (graph_volume, graph_mean) = calculate_total_usd_volume(&graph);
    let s = format!(
        "For all parsed transactions:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
        graph_volume, graph_mean, graph.edge_count()
    );
    print!("{}", &s);
    result_log.push_str(&s);
    let mut graph_root = BitMapBackend::new(&"main_graph.png", (720, 480)).into_drawing_area();
    let mut graph_multicolor_root = BitMapBackend::new(&"main_graph_multicolor.png", (720, 480)).into_drawing_area();
    plot_distribution(&graph, &mut graph_root, 0.0, "Token transaction value distribution (for all parsed transactions)");
    plot_distribution_multicolor(&graph, &mut graph_multicolor_root, 0.0, "Token transaction value distribution (for all parsed transactions)");

{
    // Nonzero filtered graph
//     let usd_lower_bound = 0.0000001;
//     let usd_higher_bound = f64::MAX;
//     let price_filtered_graph = filter_by_transaction_price(&graph, usd_lower_bound, usd_higher_bound);
//     let (price_filtered_graph_volume, price_filtered_graph_mean) = calculate_total_usd_volume(&price_filtered_graph);
//     let s = format!(
//         "For transactions in {:.0e} - {:.0e} USD range:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
//         usd_lower_bound, usd_higher_bound, price_filtered_graph_volume, price_filtered_graph_mean, price_filtered_graph.edge_count()
//     );
//     print!("{}", &s);
//     result_log.push_str(&s);
// }

// {
//     // 0 filtered graph
//     let usd_lower_bound = 0.0;
//     let usd_higher_bound = 0.0;
//     let price_filtered_graph = filter_by_transaction_price(&graph, usd_lower_bound, usd_higher_bound);
//     let (price_filtered_graph_volume, price_filtered_graph_mean) = calculate_total_usd_volume(&price_filtered_graph);
//     let s = format!(
//         "For transactions in {}-{} USD range:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
//         usd_lower_bound, usd_higher_bound, price_filtered_graph_volume, price_filtered_graph_mean, price_filtered_graph.edge_count()
//     );
//     print!("{}", &s);
//     result_log.push_str(&s);
}

{
    // 10-1000 filtered graph
    // let usd_lower_bound = 10.0;
    // let usd_higher_bound = 1000.0;
    // let price_filtered_graph = filter_by_transaction_price(&graph, usd_lower_bound, usd_higher_bound);
    // let (price_filtered_graph_volume, price_filtered_graph_mean) = calculate_total_usd_volume(&price_filtered_graph);
    // let s = format!(
    //     "For transactions in {}-{} USD range:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
    //     usd_lower_bound, usd_higher_bound, price_filtered_graph_volume, price_filtered_graph_mean, price_filtered_graph.edge_count()
    // );
    // print!("{}", &s);
    // result_log.push_str(&s);
    // let mut price_filtered_graph_root = BitMapBackend::new(&"price_filtered_graph.png", (720, 480)).into_drawing_area();
    // plot_distribution(&price_filtered_graph, &mut price_filtered_graph_root, 1.0, "Token value distribution (for transactions in the 10-1000 USD range)");
}

let mut log_file_main= File::create("result.txt").unwrap();
write!(log_file_main, "{}", result_log).unwrap();

println!("Local operations took {:.3} s", timer.elapsed().as_secs_f64());
println!("Local + async operations took {:.3} s", async_timer.elapsed().as_secs_f64());
}
