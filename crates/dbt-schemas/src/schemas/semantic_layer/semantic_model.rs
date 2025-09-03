use serde::{Deserialize, Serialize};

use crate::schemas::{
    common::Dimension,
    manifest::{
        DbtSemanticModel,
        common::SourceFileMetadata,
        semantic_model::{NodeRelation, SemanticEntity, SemanticMeasure, SemanticModelDefaults},
    },
    semantic_layer::semantic_manifest::SemanticLayerElementConfig,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestSemanticModel {
    pub name: String,
    pub defaults: Option<SemanticModelDefaults>,
    pub description: Option<String>,
    pub node_relation: Option<NodeRelation>,
    pub primary_entity: Option<String>,
    pub entities: Vec<SemanticEntity>,
    pub measures: Vec<SemanticMeasure>,
    pub dimensions: Vec<Dimension>,
    pub label: Option<String>,
    pub metadata: Option<SourceFileMetadata>,
    pub config: Option<SemanticLayerElementConfig>,
}

impl From<DbtSemanticModel> for SemanticManifestSemanticModel {
    fn from(model: DbtSemanticModel) -> Self {
        SemanticManifestSemanticModel {
            name: model.__common_attr__.name,
            description: model.__common_attr__.description,
            label: model.__semantic_model_attr__.label,
            config: Some(SemanticLayerElementConfig {
                meta: model.deprecated_config.meta,
            }),
            defaults: model.__semantic_model_attr__.defaults,
            node_relation: model.__semantic_model_attr__.node_relation,
            primary_entity: model.__semantic_model_attr__.primary_entity,
            entities: model.__semantic_model_attr__.entities,
            measures: model.__semantic_model_attr__.measures,
            dimensions: model.__semantic_model_attr__.dimensions,
            metadata: model.__semantic_model_attr__.metadata,
        }
    }
}
