use clap::Args;
use crate::utils::*;
use dateparser::parse;
use serde_json::json;
use serde_json::Value;
use std::cmp;
use std::fmt;
use std::thread;
use std::time::Duration;

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_NUMBER_OF_LINES: u32 = 10;
const MAX_BATCH_SIZE: u32 = 5000;
const LATENCY: &str = "1m";

#[derive(Args)]
pub struct CatArgs {
    /// Index name or alias to query
    #[arg(value_name = "index_or_alias")]
    pub index: String,

    /// Display entries around a specific time
    #[arg(long, value_name = "datetime")]
    #[arg(short = 'a')]
    pub around: Option<String>,

    /// Number of lines to display
    #[arg(short = 'n', value_name = "number_of_lines", default_value_t = DEFAULT_NUMBER_OF_LINES)]
    pub lines: u32,

    /// Start time for filtering results
    #[arg(long, value_name = "datetime")]
    #[arg(short = 'F')]
    pub from: Option<String>,

    /// End time for filtering results
    #[arg(long, value_name = "datetime")]
    #[arg(short = 'T')]
    pub to: Option<String>,

    /// Select specific fields (comma-separated)
    #[arg(long, value_name = "field1,field2,..")]
    #[arg(short = 's')]
    pub select: Option<String>,

    /// Filter results with a specific Elasticsearch query
    #[arg(long, value_name = "query")]
    #[arg(short = 'q')]
    pub query: Option<String>,

    /// Follow new entries in the index in real-time
    #[arg(long)]
    #[arg(short = 'f')]
    pub follow: bool,
}


#[derive(Debug, PartialEq)]
pub enum ParameterCombination {
    Around,
    To,
    From,
    FromTo,
    None,
}

impl fmt::Display for ParameterCombination {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParameterCombination::Around => write!(f, "around"),
            ParameterCombination::To => write!(f, "to"),
            ParameterCombination::From => write!(f, "from"),
            ParameterCombination::FromTo => write!(f, "from+to"),
            ParameterCombination::None => write!(f, "none"),
        }
    }
}


fn validate_parameter_combinations(
    around: &Option<String>,
    from: &Option<String>,
    to: &Option<String>,
    lines: &u32,
    follow: bool,
) -> Result<ParameterCombination, ESQError> {
    // Validate around parameter combinations
    if around.is_some() {
        // Validate incompatible parameters
        if from.is_some() || to.is_some() {
            return Err(ESQError::ValidationError(
                "The parameters --to and --from cannot be used at the same time as --around.".to_string()
            ));
        }
        if follow {
            return Err(ESQError::ValidationError(
                "The parameter --follow cannot be used at the same time as --around.".to_string()
            ));
        }

        if *lines > MAX_BATCH_SIZE  {
            return Err(ESQError::ValidationError(
                format!("In combination with --around, the -n parameter has a maximum value of {}.", MAX_BATCH_SIZE)
            ));
        }
        return Ok(ParameterCombination::Around);
    }

    // Validate to parameter combinations
    if to.is_some() {
        if follow {
            return Err(ESQError::ValidationError(
                "The parameter --follow cannot be used at the same time as --to.".to_string()
            ));
        }

        if *lines > MAX_BATCH_SIZE  {
            return Err(ESQError::ValidationError(
                format!("In combination with --to, the -n parameter has a maximum value of {}.", MAX_BATCH_SIZE)
            ));
        }
        if from.is_some() {          
            if *lines != DEFAULT_NUMBER_OF_LINES {
                return Err(ESQError::ValidationError(
                    "You cannot use -n in combination with a full time range (--from and --to).".to_string()
            ));
        }
            return Ok(ParameterCombination::FromTo)
        }
        return Ok(ParameterCombination::To);
    }

    // Validate from parameter combinations
    if from.is_some() {
        return Ok(ParameterCombination::From)
    }
    
    Ok(ParameterCombination::None)
}

struct ElasticsearchClient {
    client: reqwest::blocking::Client,
    config: Config,
    pit_id: Option<String>,
}

impl Drop for ElasticsearchClient {
    fn drop(&mut self) {
        if let Err(e) = self.delete_pit() {
            eprintln!("Erreur lors de la suppression du PIT: {}", e);
        }
    }
}

impl ElasticsearchClient {
    fn new(config: Config) -> Result<Self, ESQError> {
        let client = reqwest::blocking::Client::builder().build()?;
        Ok(Self { 
            client, 
            config,
            pit_id: None,
        })
    }

    fn create_pit(&mut self, index: &str) -> Result<(), ESQError> {
        let pit_response = add_auth(
            self.client.post(format!("{}/{}/_pit?keep_alive=1m", self.config.default.url, index)),
            &self.config
        ).send()?
        .json::<Value>()?;

        self.pit_id = Some(
            pit_response["id"].as_str()
                .ok_or_else(|| ESQError::ESError("Invalid PIT response".to_string()))?
                .to_string()
        );
        Ok(())
    }

    fn delete_pit(&mut self) -> Result<(), ESQError> {
        if let Some(pit_id) = &self.pit_id {
            add_auth(
                self.client.delete(format!("{}/_pit", self.config.default.url))
                    .json(&json!({"id": pit_id})),
                &self.config
            ).send()?;
            self.pit_id = None;
        }
        Ok(())
    }

    fn search(&self, query: &Value) -> Result<Value, ESQError> {
        let mut final_query = query.clone();
        
        // Inject PIT if available
        if let Some(pit_id) = &self.pit_id {
            final_query["pit"] = json!({
                "id": pit_id,
                "keep_alive": "1m"
            });
        }

        let response = add_auth(
            self.client.post(format!("{}/_search", self.config.default.url))
                .json(&final_query),
            &self.config
        ).send()?
        .json::<Value>()?;
        
        Ok(response)
    }
}

fn seek_origin(es_client: &ElasticsearchClient, datetime: &Option<String>, size: &u32) -> Option<Value> {
    let mut query = if let Some(dt) = datetime {
        let parsed_date = parse(dt)
            .map_err(|e| ESQError::DateParseError(e.to_string()))
            .ok()?
            .to_rfc3339();

        json!({
            "query": {
                "range": {
                    "@timestamp": {
                        "lt": parsed_date,
                    }
                }
            }
        })
    } else {
        json!({
            "query": {
                "range": {
                    "@timestamp": {
                        "lt" : format!("now-{}", LATENCY),
                    }
                }
            }
        })
    };
    query["size"] = json!(size + 1);
    query["_source"] = json!(false);
    query["sort"] = json!([{"@timestamp": {"order": "desc"}}, {"_shard_doc": {"order": "asc"}}]);

    es_client.search(&query).ok().and_then(|response| {
        response["hits"]["hits"]
            .as_array()
            .and_then(|hits| hits.last())
            .map(|last_hit| last_hit["sort"].clone())
    })
}

fn gen_query_range(from: Option<&String>, to: Option<&String>) -> Option<Value> {
    let mut range = json!({
        "@timestamp": {}
    });

    // Parse and add "from" date if present
    if let Some(from_str) = from {
        if let Ok(from_dt) = parse(from_str) {
            range["@timestamp"]["gte"] = json!(from_dt.to_rfc3339());
        } else {
            return None;
        }
    }

    // Parse and add "to" date if present
    if let Some(to_str) = to {
        if let Ok(to_dt) = parse(to_str) {
            range["@timestamp"]["lt"] = json!(to_dt.to_rfc3339());
        } else {
            return None;
        }
    } else {
        range["@timestamp"]["lt"] = json!(format!("now-{}", LATENCY));
    }

    Some(json!({
        "range": range
    }))
}

pub fn handle_cat_command(
    config: Option<Config>,
    index: &String,
    from: &Option<String>,
    to: &Option<String>,
    select: &Option<String>,
    query: &Option<String>,
    follow: bool,
    around: &Option<String>,
    mut lines: &u32,
) -> Result<(), ESQError> {

    if query.is_some() {
        return Err(ESQError::NotYetImplemented("query".to_string()));
    }

    let config = config
        .ok_or_else(|| ESQError::ConfigError("No configuration found. Please login first.".to_string()))?
        .clone();

    let extract_mode = validate_parameter_combinations(around, from, to, lines, follow)?;
    let mut search_after = None;
    let mut query_range = None;

    let mut es_client = ElasticsearchClient::new(config)?;
    es_client.create_pit(index)?;
    
    match extract_mode {
        ParameterCombination::Around => search_after = seek_origin(&es_client, around, &(lines/2)),
        ParameterCombination::To => search_after = seek_origin(&es_client, to, lines),
        ParameterCombination::From => query_range = gen_query_range(from.as_ref(), None),
        ParameterCombination::FromTo => query_range = gen_query_range(from.as_ref(), to.as_ref()),
        ParameterCombination::None => search_after = seek_origin(&es_client, &None, lines),
    }

    let mut search_query = json!({
        "sort": [{"@timestamp": {"order": "asc"}}, {"_shard_doc": {"order": "asc"}}],
        "size": json!(cmp::min(*lines as usize, DEFAULT_BATCH_SIZE))
    });
    
    // Add selected fields if specified
    if let Some(select_value) = select {
        if !select_value.is_empty() {
            let fields: Vec<&str> = select_value.split(',').collect();
            search_query["_source"] = json!(fields);
        }
    }

    let mut total_docs = *lines as i64;

    loop {
        if let Some(ref qr) = query_range {
            search_query["query"] = qr.clone();
        }
        // Fetch results in batches
        while total_docs > 0 {
            search_query["size"] = json!(cmp::min(total_docs as usize, cmp::min(*lines as usize, DEFAULT_BATCH_SIZE)));

            // Add search_after for pagination if we have it
            if let Some(ref last_sort) = search_after {
                search_query["search_after"] = last_sort.clone();
            }
            //println!("{}", serde_json::to_string_pretty(&search_query).unwrap());

            // Execute search request
            let response = es_client.search(&search_query)?;

            let hits = response["hits"]["hits"].as_array().unwrap();

            // Break if no more results
            if hits.is_empty() {
                break;
            }

            // Print results
            for hit in hits {
                println!("{}", hit["_source"]);
            }

            // Get sort values from the last document for the next iteration
            if let Some(last_hit) = hits.last() {
                search_after = last_hit["sort"].clone().into();
            } else {
                break;
            }
            total_docs -= hits.len() as i64;
        }
        if !follow {
            break;
        } else {
            es_client.delete_pit()?;
            es_client.create_pit(index)?;

            if let Some(ts) = search_after
                .as_ref()
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.get(0))
                .and_then(|num| num.as_u64()) 
            {
                query_range = gen_query_range(Some(&ts.to_string()), None);
            } 
            total_docs = MAX_BATCH_SIZE as i64;
            lines = &MAX_BATCH_SIZE;
            thread::sleep(Duration::from_secs(1));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_around_with_from() {
        let result = validate_parameter_combinations(
            &Some("2024-01-01".to_string()),
            &Some("2024-01-01".to_string()),
            &None,
            &10,
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_around_with_follow() {
        let result = validate_parameter_combinations(
            &Some("2024-01-01".to_string()),
            &None,
            &None,
            &10,
            true,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_to_with_follow() {
        let result = validate_parameter_combinations(
            &None,
            &None,
            &Some("2024-01-01".to_string()),
            &10,
            true,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_valid_params() {
        let result = validate_parameter_combinations(
            &None,
            &Some("2024-01-01".to_string()),
            &None,
            &10,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::From);
    }

    #[test]
    fn test_validate_around_only() {
        let result = validate_parameter_combinations(
            &Some("2024-01-01".to_string()),
            &None,
            &None,
            &10,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::Around);
    }

    #[test]
    fn test_validate_to_only() {
        let result = validate_parameter_combinations(
            &None,
            &None,
            &Some("2024-01-01".to_string()),
            &10,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::To);
    }
        
    #[test]
    fn test_validate_from_only() {
        let result = validate_parameter_combinations(
            &None,
            &Some("2024-01-01".to_string()),
            &None,
            &10,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::From);
    }

    #[test]
    fn test_validate_from_to() {
        let result = validate_parameter_combinations(
            &None,
            &Some("2024-01-01".to_string()),
            &Some("2024-01-02".to_string()),
            &10,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::FromTo);
    }
    #[test]
    fn test_validate_from_to_invalid_n() {
        let result = validate_parameter_combinations(
            &None,
            &Some("2024-01-01".to_string()),
            &Some("2024-01-02".to_string()),
            &20,
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_around_invalid_n() {
        let result = validate_parameter_combinations(
            &Some("2024-01-01".to_string()),
            &None,
            &None,
            &10000,
            false,
        );
        assert!(result.is_err());
    }   
    
    #[test]
    fn test_validate_to_invalid_n() {
        let result = validate_parameter_combinations(
            &None,
            &None,
            &Some("2024-01-01".to_string()),
            &10000,
            false,
        );
        assert!(result.is_err());
    }
    
    #[test]
    fn test_validate_from_n() {
        let result = validate_parameter_combinations(
            &None,
            &Some("2024-01-01".to_string()),
            &None,
            &20000,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::From);
    }

    #[test]
    fn test_validate_none(){
        let result = validate_parameter_combinations(
            &None,
            &None,
            &None,
            &20,
            false,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ParameterCombination::None);
    }
}