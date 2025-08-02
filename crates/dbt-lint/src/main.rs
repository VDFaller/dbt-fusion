use dbt_schemas::schemas::manifest::{
    DbtManifestV12, DbtNode, ManifestSource, ManifestModel
};
use std::{env, fs, path::Path};

#[derive(Default, Debug)]
struct ModelFailures<'a> {
    no_descriptions: Vec<&'a str>,
    no_tags: Vec<&'a str>,
    column_failures: Vec<ColumnFailures<'a>>,
}

#[derive(Default, Debug)]
struct ColumnFailures<'a> {
    model: &'a str,
    no_descriptions: Vec<&'a str>,
}

#[derive(Default, Debug)]
struct SourceFailures<'a> {
    no_descriptions: Vec<&'a str>,
}

#[derive(Default, Debug)]
struct Failures<'a> {
    model_failures: ModelFailures<'a>,
    source_failures: SourceFailures<'a>,
}

fn get_manifest(manifest_path: &Path) -> DbtManifestV12 {
    let manifest_str = fs::read_to_string(manifest_path)
        .expect("Failed to read manifest.json");

    let manifest: DbtManifestV12 = serde_json::from_str(&manifest_str)
        .expect("Failed to parse manifest.json");

    return manifest
}

fn check_all<'a>(manifest: &'a DbtManifestV12) -> Failures<'a> {
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


fn main() {
    let args: Vec<String> = env::args().collect();
    let manifest_path = Path::new(&args[1]);
    let manifest = get_manifest(&manifest_path);
    let failures = check_all(&manifest);

    println!("Nodes without description: {:?}", failures.model_failures.no_descriptions.len());
    println!("Number of models without tags: {}", failures.model_failures.no_tags.len());
    println!("Models with columns missing descriptions: {:?}", failures.model_failures.column_failures.len());

    println!("Sources without description: {:?}", failures.source_failures.no_descriptions.len());
}
