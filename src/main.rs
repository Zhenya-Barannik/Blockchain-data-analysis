use eyre::Result;
use petgraph::{graph::NodeIndex, visit::EdgeRef, Directed};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::runtime::Runtime;
use petgraph::Graph;
use std::fs::{self, File};
use std::io::{Write, Read};
use std::collections::HashSet;
use std::time::Instant;

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

const TRAVERSAL_STARTING_ADDRESS: &str = "0x4976A4A02f38326660D17bf34b431dC6e2eb2327"; // Binance affiliated address
const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 4; // Depth of 1 will always be searched, so max depth of 0 is the same as max depth of 1.
const MAX_TOTAL_TRANSACTIONS: usize = 100_000_000; // Limit of transactions at which parsing will be stopped.
const MAX_TRANSACTIONS_FROM_EACH_ADDRESS: usize = 10_000; // Limit of transactions to parse (from and to) one particular address.
const DATA_STORAGE_FOLDER: &str = "json";

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
                    "Added transaction {}... --> {}... at {} unix epoch",
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
    fs::create_dir_all(DATA_STORAGE_FOLDER).unwrap();
    let file_pathname = format!("{}/{}", DATA_STORAGE_FOLDER, pathname);
    let file = File::create(&file_pathname).unwrap();
    serde_json::to_writer_pretty(file, &serializable_graph).unwrap();

    println!("\nSaved graph with {} edges and {} nodes as {}\n", &graph.edge_count(), &graph.node_count(), &file_pathname);
    Ok(())
}


#[allow(dead_code)]
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
        .map_err(|_| eyre::eyre!("Please provide an Etherscan API key (put it inside api_key.txt)"))
        .unwrap()
        .read_to_string(&mut api_key).unwrap();
    api_key = api_key.trim().to_string();
    assert_ne!(api_key, "");
    api_key
}

fn filter_twoway_edges(graph: &Graph<String, Transaction, Directed>) -> Graph<String, Transaction, Directed> {
    let mut filtered_graph = graph.clone();
    filtered_graph.clear_edges();

    for edge in graph.edge_references() {
        let (source, target) = (edge.source(), edge.target());
        if graph.find_edge(target, source).is_some() {
            let transaction = edge.weight().clone();
            filtered_graph.add_edge(source, target, transaction);
        }
    }

    filtered_graph
}

fn calculate_two_way_flow(graph: &Graph<String, Transaction, Directed>, prices: &Vec<Record>) -> (f64, f64, f64, String) {
    let mut detailed_log = String::new();
    let mut total_volume_usd = 0.0;
    let mut total_flow_usd = 0.0;

    let mut visited_pairs = HashSet::new();
    for first_edge in graph.edge_references() {
        let node_a = first_edge.source();
        let node_b = first_edge.target();

        if visited_pairs.contains(&(node_a, node_b)) {continue}

        let mut pair_volume_usd = 0.0;
        let mut sum_a_to_b_usd = 0.0;
        let mut sum_b_to_a_usd = 0.0;
        
        let edges_a_to_b = graph.edges_connecting(node_a, node_b).count();
        let edges_b_to_a = graph.edges_connecting(node_b, node_a).count();
        let edges_info: String = if node_a != node_b {
                format!("{} + {}", edges_a_to_b, edges_b_to_a)
            } else {             
                format!("{} self", edges_a_to_b)
            };
        
        detailed_log.push_str(&format!(
            "Two-way transaction set for addresses {:?}... <-> {:?}... ({}):\n",
            &graph[node_a][0..10], &graph[node_b][0..10], &edges_info
        ));

        for edge in graph.edges(node_a) {
            if edge.target() == node_b {
                let volume_wei: f64 = edge.weight().value.parse().unwrap();
                let timestamp: u64 = edge.weight().timeStamp.parse().unwrap();
                let volume_in_usd = (volume_wei / 1e18) * get_price_at_timestamp(timestamp, prices);
                pair_volume_usd += volume_in_usd;
                sum_a_to_b_usd += volume_in_usd;
                detailed_log.push_str(&format!("      |-> hash: {} at {} unix epoch, volume: {:.0} USD\n", &edge.weight().hash, timestamp, volume_in_usd));
            }
        }

        if node_a != node_b { 
            for edge in graph.edges(node_b) {
                if edge.target() == node_a {
                    let volume_wei: f64 = edge.weight().value.parse().unwrap();
                    let timestamp: u64 = edge.weight().timeStamp.parse().unwrap();
                    let volume_in_usd = (volume_wei / 1e18) * get_price_at_timestamp(timestamp, prices);
                    pair_volume_usd += volume_in_usd;
                    sum_b_to_a_usd += volume_in_usd;
                    detailed_log.push_str(&format!("      <-| hash: {} at {} unix epoch, volume: {:.0} USD\n", &edge.weight().hash, timestamp, volume_in_usd));
                }
            }
        } 
        let pair_flow_usd = if node_a != node_b {
            (sum_a_to_b_usd - sum_b_to_a_usd).abs()
        } else {
            0.0
        };
        total_volume_usd += pair_volume_usd;
        total_flow_usd += pair_flow_usd;

        visited_pairs.insert((node_a, node_b));
        visited_pairs.insert((node_b, node_a));

        detailed_log.push_str(&format!(
            "Volume for this set: {:.0} USD, Flow for this set: {:.0} USD\n\n",
            pair_volume_usd, pair_flow_usd
        ));

    }

    detailed_log.push_str(&format!("Total volume: {:.0} USD\n", total_volume_usd));
    detailed_log.push_str(&format!("Total flow: {:.0} USD\n", total_flow_usd));

    (total_volume_usd, total_volume_usd/graph.edge_count() as f64, total_flow_usd, detailed_log)
}



#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct Record {
    unix_epoch_at_the_start_of_averaging_period: u64,
    average_price_in_usd: f64,
    symbol: String,
}

fn get_eth_hourly_prices(file_path: &str) -> Result<Vec<Record>> {
    let mut reader = csv::Reader::from_path(file_path).unwrap();
    let mut records = Vec::new();

    for result in reader.records() {
        let record = result.unwrap();
        let unix_epoch_at_the_start_of_averaging_period: u64 = record[0].parse::<f64>().unwrap().round() as u64;
        let average_price_in_usd: f64 = record[1].parse().unwrap();
        let symbol = record[2].to_string();

        records.push(Record {
            unix_epoch_at_the_start_of_averaging_period,
            average_price_in_usd,
            symbol,
        });
    }

    Ok(records)
}

fn get_price_at_timestamp(timestamp: u64, prices: &Vec<Record>) -> f64 {
    let maybe_price = prices.iter().find(|&price| {
        let period_start = price.unix_epoch_at_the_start_of_averaging_period as u64;
        let period_end = period_start + 3600;
        period_start <= timestamp && timestamp < period_end
    }).map(|price| price.average_price_in_usd);

    match maybe_price {
        Some(price) => price,
        None => {
            let last_record = prices.iter()
            .max_by(|record_a, record_b| record_a.unix_epoch_at_the_start_of_averaging_period.cmp(&record_b.unix_epoch_at_the_start_of_averaging_period));
            let last_timestamp = last_record.unwrap().unix_epoch_at_the_start_of_averaging_period;
            let last_price = last_record.unwrap().average_price_in_usd;

            if timestamp > last_timestamp {last_price} else {panic!("No price found")}
        }
    }

}

fn filter_by_transaction_price(
    graph: &Graph<String, Transaction, Directed>,
    prices: &Vec<Record>,
    lower_usd_bound: f64,
    higher_usd_bound: f64
) -> Graph<String, Transaction, Directed> {
    let mut filtered_graph = graph.clone();
    filtered_graph.clear_edges();

    for edge in graph.edge_references() {
        let transaction = edge.weight();
        let timestamp: u64 = transaction.timeStamp.parse().unwrap();
        let eth_price = get_price_at_timestamp(timestamp, prices);
        let transaction_value_in_usd = (transaction.value.parse::<f64>().unwrap() / 1e18) * eth_price;
        if lower_usd_bound <= transaction_value_in_usd && transaction_value_in_usd <= higher_usd_bound {
            filtered_graph.add_edge(edge.source(), edge.target(), transaction.clone());
        }
    }

    filtered_graph
}

fn calculate_total_usd_volume(graph: &Graph<String, Transaction, Directed>, prices: &Vec<Record>) -> (f64, f64) {
    let mut total_volume_usd = 0.0;

    for edge in graph.edge_references() {
        let transaction = edge.weight();
        let timestamp: u64 = transaction.timeStamp.parse().unwrap();
        let eth_price = get_price_at_timestamp(timestamp, prices);
        let transaction_value_in_usd = (transaction.value.parse::<f64>().unwrap() / 1e18) * eth_price;
        total_volume_usd += transaction_value_in_usd;
    }
    let mean_value_usd = total_volume_usd / graph.edge_count() as f64;

    (total_volume_usd, mean_value_usd)
}

#[test]
fn test_main() ->Result<(), ()> {  
    let graph = deserialize_graph("handcrafted_for_testing.json").unwrap();
    let prices = get_eth_hourly_prices("eth_prices.csv").unwrap();

    let (graph_volume, graph_mean) = calculate_total_usd_volume(&graph, &prices);
    assert_eq!(graph_volume.ceil(), 21011.0);
    assert_eq!(graph_mean.ceil(), 2627.0);
    assert_eq!(graph.edge_count(), 8);

    // Price filtered graph
    let usd_lower_bound = 10.0;
    let usd_higher_bound = 1000.0;
    let price_filtered_graph = filter_by_transaction_price(&graph, &prices, usd_lower_bound, usd_higher_bound);
    let (price_filtered_graph_volume, price_filtered_graph_mean) = calculate_total_usd_volume(&price_filtered_graph, &prices);
    assert_eq!(price_filtered_graph_volume.ceil(), 1127.0);
    assert_eq!(price_filtered_graph_mean.ceil(), 564.0);
    assert_eq!(price_filtered_graph.edge_count(), 2);
    
    // Two-way filtered graph
    let twoway_filtered_graph = filter_twoway_edges(&graph);
    let (twoway_filtered_graph_volume, twoway_filtered_graph_mean_value, twoway_filtered_graph_flow, _) = calculate_two_way_flow(&twoway_filtered_graph, &prices);
    assert_eq!(twoway_filtered_graph_volume.ceil(), 12009.0) ;
    assert_eq!(twoway_filtered_graph_mean_value.ceil(), 2002.0);
    assert_eq!(twoway_filtered_graph_flow.ceil(), 3755.0);
    assert_eq!(twoway_filtered_graph.edge_count(), 6);
    
    // Two-way and price filtered graph
    let twoway_price_filtered_graph = filter_twoway_edges(&price_filtered_graph);
    let (twoway_price_filtered_graph_volume, _, twoway_price_filtered_graph_flow, _) = calculate_two_way_flow(&twoway_price_filtered_graph, &prices);
    assert_eq!(twoway_price_filtered_graph_volume, 0.0);
    assert_eq!(twoway_price_filtered_graph_flow, 0.0);
    assert_eq!(twoway_price_filtered_graph.edge_count(), 0);
    
    Ok(())
}

fn main() {  
    let timer: Instant = Instant::now();
    let api_key = get_api_key();
    let rt = Runtime::new().unwrap();
    let initial_graph = Graph::<String, Transaction, Directed>::new();
    let graph = rt.block_on(parse_blockchain(initial_graph, &api_key));
    
    println!("Async operations took {:.3} s", timer.elapsed().as_secs_f64());
    let mut main_log = String::new();
    serialize_graph(&graph, "parsed.json").unwrap(); 
    let prices = get_eth_hourly_prices("eth_prices.csv").unwrap();

    let (graph_volume, graph_mean) = calculate_total_usd_volume(&graph, &prices);
    let s = format!(
        "For all parsed transactions:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
        graph_volume, graph_mean, graph.edge_count()
    );
    print!("{}", &s);
    main_log.push_str(&s);

    // Price filtered graph
    let usd_lower_bound = 10.0;
    let usd_higher_bound = 1000.0;
    let price_filtered_graph = filter_by_transaction_price(&graph, &prices, usd_lower_bound, usd_higher_bound);
    let (price_filtered_graph_volume, price_filtered_graph_mean) = calculate_total_usd_volume(&price_filtered_graph, &prices);
    let s = format!(
        "For transactions in {}-{} USD range:\nTotal volume: {:.0} USD, Mean value: {:.0} USD, N: {}\n\n",
        usd_lower_bound, usd_higher_bound, price_filtered_graph_volume, price_filtered_graph_mean, price_filtered_graph.edge_count()
    );
    print!("{}", &s);
    main_log.push_str(&s);   

    // Two-way filtered graph
    let twoway_filtered_graph = filter_twoway_edges(&graph);
    let (twoway_filtered_graph_volume, twoway_filtered_graph_mean_value, twoway_filtered_graph_flow, twoway_filtered_graph_logs) = calculate_two_way_flow(&twoway_filtered_graph, &prices);
    let s = format!(
        "For two-way transactions: \nTotal volume: {:.0} USD, Mean value: {:.0} USD, Total flow: {:.0} USD, N: {}\n\n",
        twoway_filtered_graph_volume, twoway_filtered_graph_mean_value, twoway_filtered_graph_flow, twoway_filtered_graph.edge_count()
    );
    print!("{}", &s);
    main_log.push_str(&s);
    
    // Two-way and price filtered graph
    let twoway_price_filtered_graph = filter_twoway_edges(&price_filtered_graph);
    let (twoway_price_filtered_graph_volume, twoway_price_filtered_graph_mean_value, twoway_price_filtered_graph_flow, twoway_price_filtered_graph_logs) = calculate_two_way_flow(&twoway_price_filtered_graph, &prices);
    let s = format!(
        "For two-way transactions in {}-{} USD range: \nTotal volume: {:.0} USD, Mean value: {:.0} USD, Total flow: {:.0} USD, N: {}\n\n",
        usd_lower_bound, usd_higher_bound, twoway_price_filtered_graph_volume, twoway_price_filtered_graph_mean_value, twoway_price_filtered_graph_flow, twoway_price_filtered_graph.edge_count()
    );
    print!("{}", &s);
    main_log.push_str(&s);

    let mut log_file_main= File::create("main_log.txt").unwrap();    
    write!(log_file_main, "{}", main_log).unwrap();

    let mut log_file_twoway = File::create("twoway_filtered_graph_logs.txt").unwrap();    
    let mut log_file_twoway_price = File::create("twoway_price_filtered_graph_logs.txt").unwrap();    
    let twoway_filtered_graph_logs = format!("Two-way transactions in {}-{} USD range detailed logs:\n{}", usd_lower_bound, usd_higher_bound, twoway_filtered_graph_logs);
    let twoway_price_filtered_graph_logs = format!("Two-way transactions detailed logs:\n{}", twoway_price_filtered_graph_logs);
    log_file_twoway.write_all(twoway_filtered_graph_logs.as_bytes()).unwrap();
    log_file_twoway_price.write_all(twoway_price_filtered_graph_logs.as_bytes()).unwrap();

    println!("Local + async operations took {:.3} s", timer.elapsed().as_secs_f64());
}
