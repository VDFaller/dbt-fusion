use crate::osmosis::inherit_column_descriptions;
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode, ManifestSource};
use std::{fs, path::Path};
pub mod osmosis;

#[derive(Default, Debug)]
pub struct ModelFailures {
    pub no_descriptions: Vec<String>,
    pub no_tags: Vec<String>,
    pub column_failures: Vec<ColumnFailures>,
}

#[derive(Default, Debug)]
pub struct ColumnFailures {
    pub model: String,
    pub no_descriptions: Vec<String>,
}

#[derive(Default, Debug)]
pub struct SourceFailures {
    pub no_descriptions: Vec<String>,
}

#[derive(Default, Debug)]
pub struct Failures {
    pub model_failures: ModelFailures,
    pub source_failures: SourceFailures,
}

pub fn get_manifest(manifest_path: &Path) -> DbtManifestV12 {
    // currently doesn't work with fusion run manifest V20
    println!("Reading manifest from: {}", manifest_path.display());
    let manifest_str = fs::read_to_string(manifest_path).expect("Failed to read manifest.json");

    let manifest: DbtManifestV12 =
        serde_json::from_str(&manifest_str).expect("Failed to parse manifest.json");

    return manifest;
}

pub fn check_all(manifest: &mut DbtManifestV12) -> Failures {
    let mut failures = Failures::default();

    let model_ids: Vec<String> = manifest
        .nodes
        .iter()
        .filter_map(|(id, node)| matches!(node, DbtNode::Model(_)).then(|| id.clone()))
        .collect();

    for model_id in model_ids {
        check_model(manifest, &model_id, &mut failures.model_failures);
    }

    for source in manifest.sources.values() {
        check_source(source, &mut failures.source_failures);
    }

    failures
}

fn check_model(manifest: &mut DbtManifestV12, model_id: &str, failures: &mut ModelFailures) {
    if let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) {
        if model.__common_attr__.description.is_none() {
            failures
                .no_descriptions
                .push(model.__common_attr__.unique_id.clone());
        }

        if model.config.tags.is_none() {
            failures
                .no_tags
                .push(model.__common_attr__.unique_id.clone());
        }
    } else {
        return;
    }

    if let Some(column_failures) = check_model_columns(manifest, model_id) {
        failures.column_failures.push(column_failures);
    }
}

fn check_model_columns(manifest: &mut DbtManifestV12, model_id: &str) -> Option<ColumnFailures> {
    let missing_columns: Vec<String> = {
        let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) else {
            return None;
        };
        model
            .__base_attr__
            .columns
            .values()
            .filter(|col| col.description.is_none())
            .map(|col| col.name.clone())
            .collect()
    };

    if missing_columns.is_empty() {
        return None;
    }

    for col_name in &missing_columns {
        let _ = inherit_column_descriptions(manifest, model_id, col_name);
    }

    let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) else {
        return None;
    };

    let unresolved: Vec<String> = missing_columns
        .iter()
        .filter_map(|col_name| {
            model
                .__base_attr__
                .columns
                .get(col_name)
                .and_then(|col| col.description.is_none().then(|| col.name.clone()))
        })
        .collect();

    if unresolved.is_empty() {
        None
    } else {
        Some(ColumnFailures {
            model: model.__common_attr__.unique_id.clone(),
            no_descriptions: unresolved,
        })
    }
}

fn check_source(source: &ManifestSource, failures: &mut SourceFailures) {
    if source.__common_attr__.description.is_none() {
        failures
            .no_descriptions
            .push(source.__common_attr__.unique_id.clone());
    }
}
