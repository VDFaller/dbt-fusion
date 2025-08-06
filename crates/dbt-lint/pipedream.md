## DBT fusion pipedream for checking the project
I want a `dbtf check --fix` command that will run a series of checks on the dbt project, and fix any issues it can.
It should be able to run the following checks:

* first fix, inherit the upstream descriptions


### replacement for 
* dbt-project-evaluator
    * parity for rules
	* check only, no fixes for now
* dbt-checkpoint
	* parity for rules
	* fix through osmosis (or dif-fusion :troll:) only for now
* dbt-osmosis
    * just the propogate downstream functionality
	* options
		* fill-col-descriptions-from-upstream # default false
			* if description is empty, fill it from upstream column
			* if this isn't true, nothing else will matter
			* only if it's a passthrough/rename????
				* maybe an option to fill from transformations, --unsafe
		* propogate-docs-blocks # default true
			* when a docs block is upstream, propogate the docs block, not the rendered description
		* force-inherit-descriptions # default false
			* if true, always inherit descriptions from upstream, even if the column has a description


### Basic rules when running --fix
* keep anchors and aliases as written
* don't fight with rust or sqlfluff, let them do their thing
* unsafe rules would be opt-in with a warning:
    * remove unused columns/models from yml

## dbt-lint.toml
```toml
[project-evaluator]
[project-evaluator.modeling.rules]
fct_staging_dependent_on_staging = true
fct_source_fanout = true
fct_rejoining_of_upstream_concepts = true
fct_model_fanout = true
fct_marts_or_intermediate_dependent_on_source = true
fct_direct_join_to_source = true
fct_duplicate_sources = true
fct_hard_coded_references = true
fct_multiple_sources_joined = true
fct_root_models = true
fct_staging_dependent_on_marts_or_intermediate = true
fct_unused_sources = true
fct_too_many_joins = true

[project-evaluator.testing.rules]
fct_missing_primary_key_tests = true
fct_sources_without_freshness = true
fct_test_coverage = true

[project-evaluator.documentation.rules]
fct_undocumented_models = true
fct_documentation_coverage = true
fct_undocumented_source_tables = true
fct_undocumented_sources = true

[project-evaluator.structure.rules]
fct_test_directories = true
fct_model_naming_conventions = true
fct_source_directories = true
fct_model_directories = true

[project-evaluator.performance.rules]
fct_chained_views_dependencies = true
fct_exposure_parents_materializations = true

[project-evaluator.governance.rules]
fct_public_models_without_contracts = true
fct_exposures_dependent_on_private_models = true
fct_undocumented_public_models = true

[dbt-checkpoint]
[dbt-checkpoint.models.rules]
check-column-desc-are-same=false
check-column-name-contract=false
check-model-columns-have-desc=true
check-model-has-all-columns=true
check-model-has-contract=false
check-model-has-constraints=false
check-model-has-description=true
check-model-has-meta-keys=false
check-model-has-labels-keys=false
check-model-has-properties-file=true
check-model-has-tests-by-name=false
check-model-has-tests-by-type=false
check-model-has-tests-by-group=false # though I'd like to check grain tests
check-model-has-tests=false
check-model-name-contract=false
check-model-parents-and-childs=false
check-model-parents-database=false
check-model-parents-name-prefix=false
check-model-parents-schema=false
check-model-tags=false
check-model-materialization-by-childs=false

[dbt-checkpoint.sources.rules]
check-source-columns-have-desc=true
check-source-has-all-columns=false
check-source-table-has-description=true
check-source-has-freshness=true
check-source-has-loader=false
check-source-has-meta-keys=false
check-source-has-labels-keys=false
check-source-has-tests-by-name=false
check-source-has-tests-by-type=false
check-source-has-tests=false
check-source-has-tests-by-group=false
check-source-tags=false
check-source-childs=false
[dbt-checkpoint.macros.rules]
check-macro-has-description=false
check-macro-arguments-have-desc=false
check-macro-has-meta-keys=false

[dbt-checkpoint.exposures.rules]
check-exposure-has-meta-keys=false
[dbt-checkpoint.seeds.rules]
check-seed-has-meta-keys=false
[dbt-checkpoint.snapshots.rules]
check-snapshot-has-meta-keys=false
[dbt-checkpoint.tests.rules]
check-test-has-meta-keys=false

```