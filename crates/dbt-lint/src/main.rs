use dbt_schemas::schemas::manifest::{
    DbtManifestV12, DbtNode, ManifestSource, ManifestModel
};
use std::{fs, path::Path, env};

#[derive(Default, Debug)]
struct ModelFailures {
    no_descriptions: Vec<String>,
    no_tags: Vec<String>,
}

#[derive(Default, Debug)]
struct SourceFailures {
    no_descriptions: Vec<String>,
}

#[derive(Default, Debug)]
struct Failures {
    model_failures: ModelFailures,
    source_failures: SourceFailures,
}

fn get_manifest(manifest_path: &Path) -> DbtManifestV12 {
    let manifest_str = fs::read_to_string(manifest_path)
        .expect("Failed to read manifest.json");

    let manifest: DbtManifestV12 = serde_json::from_str(&manifest_str)
        .expect("Failed to parse manifest.json");

    manifest
}

fn check_all(manifest: &DbtManifestV12) -> Failures {
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

fn check_model(model: &ManifestModel, failures: &mut ModelFailures){
    // check-model-has-description
    if model.common_attr.description.is_none() {
        failures.no_descriptions.push(model.common_attr.unique_id.clone()); // definitely can do this better right? 
    }
    // check-model-has-tags
    // just an example, test that it has tags
    if model.config.tags.is_none() {
        failures.no_tags.push(model.common_attr.unique_id.clone());
    }
}

fn check_source(source: &ManifestSource, failures: &mut SourceFailures){
    if source.common_attr.description.is_none() {
        failures.no_descriptions.push(source.common_attr.unique_id.clone()); // definitely can do this better right? 
    }
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let manifest_path = Path::new(&args[1]);
    let manifest = get_manifest(&manifest_path);
    let failures = check_all(&manifest);

    println!("Nodes without description: {:?}", failures.model_failures.no_descriptions);
    println!("Number of models without tags: {}", failures.model_failures.no_tags.len());

    println!("Sources without description: {:?}", failures.source_failures.no_descriptions);
}
