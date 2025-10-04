use std::path::Path;

use crate::machinery::Span;

/// Trait for typechecking event listeners.
pub trait TypecheckingEventListener {
    /// Returns a reference to the underlying Any type.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Called when a warning is issued during typechecking.
    fn warn(&self, message: &str);

    /// Called when a span is set during typechecking.
    fn set_span(&self, span: &crate::machinery::Span);

    /// Called when a new block is encountered during typechecking.
    fn new_block(&self, block_id: usize);

    /// Called when typechecking is complete.
    fn flush(&self);

    /// Called when a variable is looked up during typechecking.
    fn on_lookup(&self, _span: &Span, _simple_name: &str, _full_name: &str, def_spans: Vec<Span>);

    /// Called when a model reference is encountered during typechecking.
    #[allow(clippy::too_many_arguments)]
    fn on_model_reference(
        &self,
        _name: &str,
        _start_line: &u32,
        _start_col: &u32,
        _start_offset: &u32,
        _end_line: &u32,
        _end_col: &u32,
        _end_offset: &u32,
    ) {
    }

    /// Called when a model source reference is encountered during typechecking.
    #[allow(clippy::too_many_arguments)]
    fn on_model_source_reference(
        &self,
        _name: &str,
        _start_line: &u32,
        _start_col: &u32,
        _start_offset: &u32,
        _end_line: &u32,
        _end_col: &u32,
        _end_offset: &u32,
    ) {
    }

    /// Called when a function call is encountered during typechecking.
    fn on_function_call(&self, _source_span: &Span, _def_span: &Span, _def_path: &Path) {}
}

/// Default implementation of the TypecheckingEventListener trait that does nothing.
#[derive(Default, Clone)]
pub struct DefaultTypecheckingEventListener {}

impl TypecheckingEventListener for DefaultTypecheckingEventListener {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn warn(&self, _message: &str) {
        //
    }

    /// Called when a span is set during typechecking.
    fn set_span(&self, _span: &crate::machinery::Span) {
        //
    }

    /// Called when a new block is encountered during typechecking.
    fn new_block(&self, _block_id: usize) {
        //
    }

    /// Called when typechecking is complete.
    fn flush(&self) {
        //
    }

    /// Called when a variable is looked up during typechecking.
    fn on_lookup(&self, _span: &Span, _simple_name: &str, _full_name: &str, _def_spans: Vec<Span>) {
        //
    }
}
