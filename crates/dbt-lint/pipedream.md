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

