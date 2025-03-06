// src/commands/ls.rs
use crate::utils::*;
use serde_json::Value;

fn get_indices(client: &reqwest::blocking::Client, config: Config) -> Result<Vec<Value>, ESQError> {
    let url = format!("{}/_cat/indices?format=json", config.default.url.trim_end_matches('/'));

    let request = client.get(&url);
    
    let request = if config.default.username.is_some()  && config.default.password.is_some() {
        request.basic_auth(config.default.username.as_deref().unwrap_or(""), config.default.password.as_ref())
    } else {
        request
    };

    let response = request.send()?;

    if !response.status().is_success() {
        return Err(ESQError::NetworkError(format!(
            "Failed to list indices. Status code: {}",
            response.status()
        )));
    }

    response.json().map_err(|e| ESQError::ParseError(format!("Failed to parse indices: {}", e)))
}

fn display_indices(indices: &[Value]) {
    for index_data in indices {
        if let Some(index_name) = index_data["index"].as_str() {
            println!("- {}", index_name);
        }
    }
}

pub fn handle_ls_command(existing_config: Option<Config>) -> Result<(), ESQError> {
    let config = existing_config
        .ok_or_else(|| ESQError::ConfigError("No configuration found. Please login first.".to_string()))?
        .clone();
    
    let client = get_client()?;
    let indices = get_indices(&client, config)?;
    display_indices(&indices);
    
    Ok(())
}
