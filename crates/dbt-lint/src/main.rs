use dbt_schemas::schemas::{
    dbt_column::DbtColumn,
    manifest::{
        DbtManifestV12, DbtNode, ManifestModel,
    }
};
use dbt_lint::{check_all, get_manifest};
use std::{env, path::Path};


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
