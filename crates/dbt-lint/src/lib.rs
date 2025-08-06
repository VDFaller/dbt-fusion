use dbt_schemas::schemas::{
    manifest::{
        DbtManifestV12, DbtNode, ManifestSource, ManifestModel,
    }
};
use std::{fs, path::Path};

#[derive(Default, Debug)]
pub struct ModelFailures<'a> {
    pub no_descriptions: Vec<&'a str>,
    pub no_tags: Vec<&'a str>,
    pub column_failures: Vec<ColumnFailures<'a>>,
}

#[derive(Default, Debug)]
pub struct ColumnFailures<'a> {
    pub model: &'a str,
    pub no_descriptions: Vec<&'a str>,
}

#[derive(Default, Debug)]
pub struct SourceFailures<'a> {
    pub no_descriptions: Vec<&'a str>,
}

#[derive(Default, Debug)]
pub struct Failures<'a> {
    pub model_failures: ModelFailures<'a>,
    pub source_failures: SourceFailures<'a>,
}

pub fn get_manifest(manifest_path: &Path) -> DbtManifestV12 {
    let manifest_str = fs::read_to_string(manifest_path)
        .expect("Failed to read manifest.json");

    let manifest: DbtManifestV12 = serde_json::from_str(&manifest_str)
        .expect("Failed to parse manifest.json");

    return manifest
}

pub fn check_all<'a>(manifest: &'a DbtManifestV12) -> Failures<'a> {
    let mut failures = Failures::default();
    for (_, node) in &manifest.nodes {
        match node {
            DbtNode::Model(model) => {
                check_model(model, &mut failures.model_failures);
            }
            _ => {
                // do nothing for now
                continue;
            }
        }
    }
    for (_, source) in &manifest.sources {
        check_source(source, &mut failures.source_failures);
    }
    return failures
}

fn check_model<'a>(model: &'a ManifestModel, failures: &mut ModelFailures<'a>){
    // check-model-has-description
    if model.common_attr.description.is_none() {
        failures.no_descriptions.push(model.common_attr.unique_id.as_str());
    }
    // check-model-has-tags
    if model.config.tags.is_none() {
        failures.no_tags.push(model.common_attr.unique_id.as_str());
    }
    // check-model-columns-have-desc
    if let Some(column_failures) = check_model_columns(model) {
        failures.column_failures.push(column_failures);
    }
}

fn check_model_columns<'a>(model: &'a ManifestModel) -> Option<ColumnFailures<'a>> {
    let column_failures = ColumnFailures {
        model: model.common_attr.unique_id.as_str(),
        no_descriptions: check_model_columns_have_descriptions(model),
    };

    if !column_failures.no_descriptions.is_empty() {
        Some(column_failures)
    } else {
        None
    }
}

fn check_model_columns_have_descriptions<'a>(
    model: &'a ManifestModel,
) -> Vec<&'a str> {
    model.base_attr.columns.values()
        .filter(|col| col.description.is_none())
        .map(|col| col.name.as_str())
        .collect() // do I need to collect? Can I just return the iterator and iterate on reporting? 
}

fn check_source<'a>(source: &'a ManifestSource, failures: &mut SourceFailures<'a>){
    if source.common_attr.description.is_none() {
        failures.no_descriptions.push(source.common_attr.unique_id.as_str());
    }
}
