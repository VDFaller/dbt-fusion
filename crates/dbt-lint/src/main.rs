use dbt_schemas::schemas::{
    dbt_column::DbtColumn,
    manifest::{
        DbtManifestV12, DbtNode, ManifestSource, ManifestModel,
    }
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

fn inherit_column_descriptions<'a>(manifest: &'a DbtManifestV12, model: &'a ManifestModel, col: &'a mut DbtColumn) {
    // This function will inherit column descriptions from the upstream model or source
    // todo: add sources, seeds, snapshots
    // mark unsafe if multiple upstream models have same column name
    //    or even better, know which upstream model to inherit from (SDF style)
    // fix the mutability issue
    if !col.description.is_none() {
        return;
    }
    
    let depends_on = model.base_attr.depends_on.clone();
    // check if any of the upstream models have the same column name
    for dep in &depends_on.nodes {
        if let Some(dep_model) = manifest.nodes.get(dep) {
            match dep_model {
                DbtNode::Model(dep_model) => {
                    if let Some(dep_col) = dep_model.base_attr.columns.get(&col.name) {
                        if let Some(desc) = &dep_col.description {
                            col.description = Some(desc.clone());
                        }
                    }
                }
                _ => continue, // skip if not a model
            }

        }
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


    let model = manifest.nodes.get("model.jaffle_shop.orders").unwrap();
    
    
    if let DbtNode::Model(model) = model {
        let mut col = model.base_attr.columns.get("customer_id").unwrap().clone();

        inherit_column_descriptions(&manifest, model, &mut col);
        println!("Inherited column description: {:?}", col.description);
    }
}
