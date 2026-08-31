use crate::metadata_resolver::{
    conservative_model_metadata, resolve_model_driver_metadata_variants, wildcard_matches,
    DriverOriginMapping, ModelDriverMatchError,
};
use crate::model_types::{ApiType, ModelCapabilities, ModelMetadata, ModelPricing, ProviderType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnownProviderProfile {
    pub provider_type: String,
    pub provider_profile_id: String,
    pub display_name: String,
    pub protocol_adapter_id: String,
    pub default_endpoint: String,
    pub settings_section: String,
    pub credential_required: bool,
    #[serde(default)]
    pub metadata_drivers: Vec<String>,
}

pub fn load_known_provider_profiles() -> Vec<KnownProviderProfile> {
    serde_json::from_str(include_str!("../provider_rules/known_providers.json"))
        .expect("builtin known provider catalog must be valid")
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuleMatchSource {
    #[default]
    ProviderModelId,
    OriginModelId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPredicateOp {
    Exists,
    Equals,
    NotEquals,
    In,
    Contains,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestPredicate {
    pub path: String,
    pub op: RequestPredicateOp,
    #[serde(default)]
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestCondition {
    Predicate(RequestPredicate),
    All { all: Vec<RequestPredicate> },
}

impl RequestCondition {
    fn matches(&self, value: &Value) -> bool {
        match self {
            Self::Predicate(predicate) => predicate.matches(value),
            Self::All { all } => all.iter().all(|predicate| predicate.matches(value)),
        }
    }
}

impl RequestPredicate {
    fn matches(&self, root: &Value) -> bool {
        let current = root.pointer(self.path.as_str());
        match self.op {
            RequestPredicateOp::Exists => current.is_some(),
            RequestPredicateOp::Equals => current == Some(&self.value),
            RequestPredicateOp::NotEquals => current != Some(&self.value),
            RequestPredicateOp::In => self
                .value
                .as_array()
                .is_some_and(|items| current.is_some_and(|value| items.contains(value))),
            RequestPredicateOp::Contains => {
                current.is_some_and(|value| match (value, &self.value) {
                    (Value::String(current), Value::String(expected)) => current.contains(expected),
                    (Value::Array(current), expected) => current.contains(expected),
                    _ => false,
                })
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestRule {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub when: Option<RequestCondition>,
    #[serde(default)]
    pub defaults: Value,
    #[serde(default)]
    pub set: Value,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConditionalPricingRule {
    pub when: RequestCondition,
    pub amount: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPricing {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub input_token: Option<f64>,
    #[serde(default)]
    pub output_token: Option<f64>,
    #[serde(default)]
    pub cache_input_token: Option<f64>,
    #[serde(default)]
    pub estimated_cost: Option<f64>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub amount: Option<f64>,
    #[serde(default)]
    pub rules: Vec<ConditionalPricingRule>,
}

fn default_currency() -> String {
    "USD".to_string()
}

impl Default for ProviderPricing {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            input_token: None,
            output_token: None,
            cache_input_token: None,
            estimated_cost: None,
            unit: None,
            amount: None,
            rules: Vec::new(),
        }
    }
}

impl From<&ModelPricing> for ProviderPricing {
    fn from(value: &ModelPricing) -> Self {
        Self {
            currency: value.currency.clone(),
            input_token: value.input_token,
            output_token: value.output_token,
            cache_input_token: value.cache_input_token,
            estimated_cost: value.estimated_cost,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderModelRule {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub match_source: ProviderRuleMatchSource,
    #[serde(default)]
    pub exclude: bool,
    #[serde(default)]
    pub operations: BTreeMap<String, String>,
    #[serde(default)]
    pub provider_options: Value,
    #[serde(default)]
    pub request_rules: Vec<ProviderRequestRule>,
    #[serde(default)]
    pub pricing: Option<ProviderPricing>,
    #[serde(default)]
    pub estimated_latency_ms: Option<u64>,
    #[serde(default)]
    pub latency_class: Option<String>,
    #[serde(default)]
    pub cost_class: Option<String>,
    #[serde(default)]
    pub remove_api_types: Vec<ApiType>,
    #[serde(default)]
    pub remove_features: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRulesDocument {
    #[serde(default)]
    pub format: String,
    pub schema_version: u32,
    #[serde(default)]
    pub schema_revision: u32,
    pub revision_seq: u64,
    pub provider_profile_id: String,
    #[serde(default)]
    pub metadata_drivers: Option<Vec<String>>,
    #[serde(default)]
    pub origin_provider_aliases: HashMap<String, String>,
    #[serde(default)]
    pub origin_mappings: Vec<DriverOriginMapping>,
    #[serde(default)]
    pub discovery: ProviderDiscoveryRules,
    #[serde(default)]
    pub models: Vec<ProviderModelRule>,
    #[serde(default)]
    pub patterns: Vec<ProviderModelRule>,
    #[serde(default)]
    pub defaults: ProviderModelRule,
    #[serde(default)]
    pub variants: Vec<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDiscoveryRules {
    #[serde(default)]
    pub alias_numeric_suffix: Option<NumericSuffixPolicy>,
    #[serde(default)]
    pub deprecated_text_signals: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericSuffixPolicy {
    pub min_digits: usize,
    pub max_digits: usize,
}

#[derive(Clone, Debug, Default)]
pub struct AdapterOperationRegistry {
    supported: HashSet<String>,
    defaults: BTreeMap<ApiType, String>,
}

impl AdapterOperationRegistry {
    pub fn new(
        supported: impl IntoIterator<Item = impl Into<String>>,
        defaults: impl IntoIterator<Item = (ApiType, impl Into<String>)>,
    ) -> Self {
        Self {
            supported: supported.into_iter().map(Into::into).collect(),
            defaults: defaults
                .into_iter()
                .map(|(api_type, operation)| (api_type, operation.into()))
                .collect(),
        }
    }

    pub fn resolve_default(&self, api_type: &ApiType) -> Option<&str> {
        self.defaults.get(api_type).map(String::as_str)
    }

    pub fn supports(&self, operation: &str) -> bool {
        self.supported.contains(operation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingSource {
    ProviderDiscovery,
    ProviderInstance,
    ProviderRules,
    ModelDriver,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedPricing {
    pub definition: ProviderPricing,
    pub source: PricingSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_amount: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedProviderCall {
    pub provider_model_id: String,
    pub model_driver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_model_id: Option<String>,
    pub operation: String,
    pub options: Value,
    pub pricing: ResolvedPricing,
    pub provider_rules_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied_request_rules: Vec<String>,
}

pub struct ProviderCallResolveInput<'a> {
    pub metadata: &'a ModelMetadata,
    pub rules: &'a ProviderRulesDocument,
    pub method: &'a str,
    pub api_type: &'a ApiType,
    pub request_options: Value,
    pub adapter_operations: &'a AdapterOperationRegistry,
    pub discovery_pricing: Option<ProviderPricing>,
    pub instance_pricing: Option<ProviderPricing>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOriginIdentity {
    pub model_driver: Option<String>,
    pub origin_model_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderModelResolveError {
    InvalidRules(String),
    InvalidOriginMapping(String),
    ModelDriver(ModelDriverMatchError),
}

impl std::fmt::Display for ProviderModelResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRules(error) => f.write_str(error),
            Self::InvalidOriginMapping(error) => f.write_str(error),
            Self::ModelDriver(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ProviderModelResolveError {}

pub fn resolve_provider_origin_identity(
    provider_model_id: &str,
    rules: &ProviderRulesDocument,
) -> Result<ProviderOriginIdentity, String> {
    let mut mappings = rules.origin_mappings.iter().collect::<Vec<_>>();
    mappings.sort_by_key(|mapping| mapping.priority);
    for mapping in mappings {
        if mapping.match_rule.source != "provider_model_id" {
            return Err(format!(
                "origin mapping '{}' has unsupported match source",
                mapping.mapping_key
            ));
        }
        let regex = Regex::new(mapping.match_rule.regex.as_str()).map_err(|error| {
            format!(
                "origin mapping '{}' has invalid regex: {error}",
                mapping.mapping_key
            )
        })?;
        let Some(captures) = regex.captures(provider_model_id) else {
            continue;
        };
        let driver = captures.name("driver").ok_or_else(|| {
            format!(
                "origin mapping '{}' did not capture driver",
                mapping.mapping_key
            )
        })?;
        let model = captures.name("model").ok_or_else(|| {
            format!(
                "origin mapping '{}' did not capture model",
                mapping.mapping_key
            )
        })?;
        let model_driver = apply_origin_transforms(
            driver.as_str(),
            mapping.transforms.driver.as_slice(),
            &rules.origin_provider_aliases,
        )?;
        let origin_model_id = apply_origin_transforms(
            model.as_str(),
            mapping.transforms.model.as_slice(),
            &rules.origin_provider_aliases,
        )?;
        if model_driver.is_empty() || origin_model_id.is_empty() {
            return Err(format!(
                "origin mapping '{}' resolved an empty identity",
                mapping.mapping_key
            ));
        }
        return Ok(ProviderOriginIdentity {
            model_driver: Some(model_driver),
            origin_model_id,
        });
    }
    Ok(ProviderOriginIdentity {
        model_driver: None,
        origin_model_id: provider_model_id.to_string(),
    })
}

pub fn resolve_provider_model_metadata(
    provider_instance_name: &str,
    provider_type: ProviderType,
    provider_model_id: &str,
    rules: &ProviderRulesDocument,
    adapter_default_model_drivers: &[String],
    fallback_api_types: Vec<ApiType>,
) -> Result<Option<ModelMetadata>, ProviderModelResolveError> {
    Ok(resolve_provider_model_inventory(
        provider_instance_name,
        provider_type,
        provider_model_id,
        rules,
        adapter_default_model_drivers,
        fallback_api_types,
    )?
    .into_iter()
    .next())
}

pub fn resolve_provider_model_inventory(
    provider_instance_name: &str,
    provider_type: ProviderType,
    provider_model_id: &str,
    rules: &ProviderRulesDocument,
    adapter_default_model_drivers: &[String],
    fallback_api_types: Vec<ApiType>,
) -> Result<Vec<ModelMetadata>, ProviderModelResolveError> {
    validate_provider_rules_document(rules).map_err(ProviderModelResolveError::InvalidRules)?;
    let origin = resolve_provider_origin_identity(provider_model_id, rules)
        .map_err(ProviderModelResolveError::InvalidOriginMapping)?;
    let candidates = rules
        .metadata_drivers
        .as_deref()
        .unwrap_or(adapter_default_model_drivers);
    let probe = conservative_model_metadata(
        provider_instance_name,
        provider_type.clone(),
        provider_model_id,
        origin.origin_model_id.as_str(),
        fallback_api_types.clone(),
    );
    if find_provider_rules(&probe, rules)
        .into_iter()
        .any(|rule| rule.exclude)
    {
        return Ok(Vec::new());
    }
    let metadata = if rules.metadata_drivers.as_ref().is_some_and(Vec::is_empty) {
        Vec::new()
    } else {
        resolve_model_driver_metadata_variants(
            provider_instance_name,
            provider_type.clone(),
            provider_model_id,
            origin.origin_model_id.as_str(),
            candidates,
            origin.model_driver.as_deref(),
            fallback_api_types.clone(),
        )
        .map_err(ProviderModelResolveError::ModelDriver)?
    };
    let mut metadata = if metadata.is_empty() {
        vec![probe]
    } else {
        metadata
    };
    for model in metadata.iter_mut() {
        apply_provider_inventory_overrides(model, rules)
            .map_err(ProviderModelResolveError::InvalidRules)?;
    }
    Ok(metadata)
}

fn apply_origin_transforms(
    value: &str,
    transforms: &[crate::metadata_resolver::DriverOriginTransform],
    aliases: &HashMap<String, String>,
) -> Result<String, String> {
    let mut value = value.to_string();
    for transform in transforms {
        match transform.op.as_str() {
            "trim" => value = value.trim().to_string(),
            "lowercase" => value = value.to_ascii_lowercase(),
            "alias" => {
                if transform.table.as_deref() != Some("origin_provider_aliases") {
                    return Err("origin alias transform references an unknown table".to_string());
                }
                if let Some(alias) = aliases.get(value.as_str()) {
                    value = alias.clone();
                } else if transform.on_missing.as_deref().unwrap_or("keep") != "keep" {
                    return Err(format!("origin alias is missing for '{value}'"));
                }
            }
            operation => return Err(format!("unsupported origin transform: {operation}")),
        }
    }
    Ok(value)
}

pub fn resolve_provider_call(
    input: ProviderCallResolveInput<'_>,
) -> Result<ResolvedProviderCall, String> {
    validate_provider_rules_document(input.rules)?;
    let selected = find_provider_rules(input.metadata, input.rules);
    if selected.iter().any(|rule| rule.exclude) {
        return Err("provider model is excluded by provider rules".to_string());
    }

    let operation = resolve_operation(
        &input.rules.defaults.operations,
        selected.iter().map(|rule| &rule.operations),
        input.method,
        input.api_type,
        input.adapter_operations,
    )?;

    let mut options = Value::Object(Map::new());
    merge_json(&mut options, input.rules.defaults.provider_options.clone());
    for rule in selected.iter().rev() {
        merge_json(&mut options, rule.provider_options.clone());
    }
    merge_json(&mut options, input.request_options);
    let mut applied_request_rules = Vec::new();
    apply_request_rules_with_ids(
        &mut options,
        input.rules.defaults.request_rules.as_slice(),
        "defaults",
        &mut applied_request_rules,
    )?;
    for rule in selected.iter().rev() {
        let scope = rule
            .id
            .as_ref()
            .map(|id| format!("model:{id}"))
            .or_else(|| {
                rule.pattern
                    .as_ref()
                    .map(|pattern| format!("pattern:{pattern}"))
            })
            .unwrap_or_else(|| "provider-rule".to_string());
        apply_request_rules_with_ids(
            &mut options,
            rule.request_rules.as_slice(),
            scope.as_str(),
            &mut applied_request_rules,
        )?;
    }

    let pricing = resolve_provider_pricing(
        input.metadata,
        input.rules,
        &options,
        input.discovery_pricing,
        input.instance_pricing,
    );

    Ok(ResolvedProviderCall {
        provider_model_id: input
            .metadata
            .provider_actual_model_id
            .clone()
            .unwrap_or_else(|| input.metadata.provider_model_id.clone()),
        model_driver: input.metadata.model_driver.clone(),
        origin_model_id: input.metadata.origin_model_id.clone(),
        operation,
        options,
        pricing,
        provider_rules_revision: input.rules.revision_seq,
        applied_request_rules,
    })
}

pub fn resolve_provider_pricing(
    metadata: &ModelMetadata,
    rules: &ProviderRulesDocument,
    options: &Value,
    discovery_pricing: Option<ProviderPricing>,
    instance_pricing: Option<ProviderPricing>,
) -> ResolvedPricing {
    let provider_rule_pricing = find_provider_rules(metadata, rules)
        .into_iter()
        .find_map(|rule| rule.pricing.clone())
        .or_else(|| rules.defaults.pricing.clone());
    let (definition, source) = discovery_pricing
        .map(|pricing| (pricing, PricingSource::ProviderDiscovery))
        .or_else(|| instance_pricing.map(|pricing| (pricing, PricingSource::ProviderInstance)))
        .or_else(|| provider_rule_pricing.map(|pricing| (pricing, PricingSource::ProviderRules)))
        .unwrap_or_else(|| {
            (
                ProviderPricing::from(&metadata.pricing),
                if model_pricing_is_known(&metadata.pricing) {
                    PricingSource::ModelDriver
                } else {
                    PricingSource::Unknown
                },
            )
        });
    let matched_amount = definition
        .rules
        .iter()
        .find(|rule| rule.when.matches(options))
        .map(|rule| rule.amount)
        .or(definition.amount)
        .or(definition.estimated_cost);
    ResolvedPricing {
        definition,
        source,
        matched_amount,
    }
}

pub fn apply_resolved_provider_request_rules(
    call: &ResolvedProviderCall,
    rules: &ProviderRulesDocument,
    target: &mut Value,
) -> Result<Vec<String>, String> {
    apply_provider_request_rules_for_identity(
        call.provider_model_id.as_str(),
        call.origin_model_id.as_deref(),
        rules,
        target,
    )
}

pub fn apply_provider_request_rules_for_identity(
    provider_model_id: &str,
    origin_model_id: Option<&str>,
    rules: &ProviderRulesDocument,
    target: &mut Value,
) -> Result<Vec<String>, String> {
    validate_provider_rules_document(rules)?;
    let selected = find_provider_rules_for_identity(provider_model_id, origin_model_id, rules);
    let mut applied = apply_request_rules(target, rules.defaults.request_rules.as_slice())?;
    for rule in selected.iter().rev() {
        applied.extend(apply_request_rules(target, rule.request_rules.as_slice())?);
    }
    Ok(applied)
}

pub fn apply_provider_capability_limits(
    metadata: &mut ModelMetadata,
    rules: &ProviderRulesDocument,
) -> Result<(), String> {
    for rule in find_provider_rules(metadata, rules) {
        metadata
            .api_types
            .retain(|api_type| !rule.remove_api_types.contains(api_type));
        for feature in rule.remove_features.iter() {
            remove_capability(&mut metadata.capabilities, feature)?;
        }
    }
    Ok(())
}

pub fn apply_provider_inventory_overrides(
    metadata: &mut ModelMetadata,
    rules: &ProviderRulesDocument,
) -> Result<(), String> {
    apply_provider_capability_limits(metadata, rules)?;
    let selected = find_provider_rules(metadata, rules);
    let mut provider_options = Value::Object(Map::new());
    merge_json(
        &mut provider_options,
        rules.defaults.provider_options.clone(),
    );
    for rule in selected.iter().rev() {
        merge_json(&mut provider_options, rule.provider_options.clone());
    }
    metadata.provider_options = if provider_options
        .as_object()
        .is_some_and(|options| !options.is_empty())
    {
        Some(provider_options)
    } else {
        None
    };
    if let Some(pricing) = selected
        .iter()
        .find_map(|rule| rule.pricing.as_ref())
        .or(rules.defaults.pricing.as_ref())
    {
        metadata.pricing.currency.clone_from(&pricing.currency);
        metadata.pricing.input_token = pricing.input_token;
        metadata.pricing.output_token = pricing.output_token;
        metadata.pricing.cache_input_token = pricing.cache_input_token;
        metadata.pricing.estimated_cost = pricing.estimated_cost.or(pricing.amount);
    }
    if let Some(latency) = selected
        .iter()
        .find_map(|rule| rule.estimated_latency_ms)
        .or(rules.defaults.estimated_latency_ms)
    {
        metadata.health.p50_latency_ms = Some(latency);
    }
    Ok(())
}

pub fn apply_builtin_provider_rules_to_inventory(
    inventory: &mut crate::model_types::ProviderInventory,
    provider_profile_id: &str,
) -> Result<(), String> {
    let rules = load_builtin_provider_rules(provider_profile_id).ok_or_else(|| {
        format!("builtin provider rules are unavailable for '{provider_profile_id}'")
    })?;
    for model in inventory.models.iter_mut() {
        apply_provider_inventory_overrides(model, &rules)?;
    }
    inventory.provider_profile_id = rules.provider_profile_id;
    Ok(())
}

fn remove_capability(capabilities: &mut ModelCapabilities, feature: &str) -> Result<(), String> {
    match feature {
        "streaming" => capabilities.streaming = false,
        "tool_calling" => capabilities.tool_call = false,
        "json_output" => capabilities.json_schema = false,
        "web_search" => capabilities.web_search = false,
        "vision" => capabilities.vision = false,
        "image_generation" => capabilities.image_generation = false,
        _ => return Err(format!("unknown removable model feature: {feature}")),
    }
    Ok(())
}

pub fn validate_provider_rules_document(rules: &ProviderRulesDocument) -> Result<(), String> {
    if rules.format != "buckyos.aicc.provider-rules-catalog" {
        return Err("unsupported provider rules format".to_string());
    }
    if rules.schema_version != 1 {
        return Err("unsupported provider rules schema version".to_string());
    }
    if rules.revision_seq == 0 || rules.provider_profile_id.trim().is_empty() {
        return Err("provider rules identity and revision are required".to_string());
    }
    for (index, mapping) in rules.origin_mappings.iter().enumerate() {
        validate_origin_mapping(mapping, index)?;
    }
    if let Some(policy) = rules.discovery.alias_numeric_suffix.as_ref() {
        if policy.min_digits == 0 || policy.min_digits > policy.max_digits {
            return Err("discovery.alias_numeric_suffix range is invalid".to_string());
        }
    }
    if rules
        .discovery
        .deprecated_text_signals
        .iter()
        .any(|signal| signal.is_empty() || signal.trim() != signal)
    {
        return Err(
            "discovery.deprecated_text_signals must be non-empty trimmed strings".to_string(),
        );
    }
    for (index, rule) in rules.models.iter().enumerate() {
        if rule.id.as_deref().is_none_or(str::is_empty) || rule.pattern.is_some() {
            return Err(format!("models[{index}] must contain only a non-empty id"));
        }
        validate_provider_model_rule(rule, format!("models[{index}]").as_str())?;
    }
    for (index, rule) in rules.patterns.iter().enumerate() {
        if rule.pattern.as_deref().is_none_or(str::is_empty) || rule.id.is_some() {
            return Err(format!(
                "patterns[{index}] must contain only a non-empty pattern"
            ));
        }
        validate_provider_model_rule(rule, format!("patterns[{index}]").as_str())?;
    }
    validate_provider_model_rule(&rules.defaults, "defaults")?;
    Ok(())
}

pub fn load_builtin_provider_rules(provider_profile_id: &str) -> Option<ProviderRulesDocument> {
    let normalized = provider_profile_id.trim().to_ascii_lowercase();
    let raw = match normalized.as_str() {
        "openai" => include_str!("../provider_rules/openai.json"),
        "openrouter" => include_str!("../provider_rules/openrouter.json"),
        "claude" | "anthropic" => include_str!("../provider_rules/claude.json"),
        "google-gemini" | "gemini" | "google" => {
            include_str!("../provider_rules/google-gemini.json")
        }
        "sn-ai-provider" | "sn_router" => include_str!("../provider_rules/sn-ai-provider.json"),
        "minimax" => include_str!("../provider_rules/minimax.json"),
        "fal" => include_str!("../provider_rules/fal.json"),
        "custom-openai-compatible" => {
            include_str!("../provider_rules/custom-openai-compatible.json")
        }
        _ => return None,
    };
    serde_json::from_str::<ProviderRulesDocument>(raw)
        .ok()
        .filter(|rules| validate_provider_rules_document(rules).is_ok())
}

pub fn builtin_provider_model_ids_for_method(
    provider_profile_id: &str,
    method: &str,
) -> Vec<String> {
    load_builtin_provider_rules(provider_profile_id)
        .map(|rules| {
            rules
                .models
                .into_iter()
                .filter(|rule| rule.operations.contains_key(method))
                .filter_map(|rule| rule.id)
                .collect()
        })
        .unwrap_or_default()
}

fn validate_origin_mapping(mapping: &DriverOriginMapping, index: usize) -> Result<(), String> {
    if mapping.match_rule.source != "provider_model_id" {
        return Err(format!(
            "origin_mappings[{index}] has unsupported match source"
        ));
    }
    let regex = Regex::new(mapping.match_rule.regex.as_str())
        .map_err(|error| format!("origin_mappings[{index}] has invalid regex: {error}"))?;
    let capture_names = regex.capture_names().flatten().collect::<HashSet<_>>();
    if !capture_names.contains("driver") || !capture_names.contains("model") {
        return Err(format!(
            "origin_mappings[{index}] must define driver and model captures"
        ));
    }
    for transform in mapping
        .transforms
        .driver
        .iter()
        .chain(mapping.transforms.model.iter())
    {
        if !matches!(transform.op.as_str(), "trim" | "lowercase" | "alias") {
            return Err(format!(
                "origin_mappings[{index}] has unsupported transform"
            ));
        }
        if transform.op == "alias" && transform.table.as_deref() != Some("origin_provider_aliases")
        {
            return Err(format!(
                "origin_mappings[{index}] alias references an unknown table"
            ));
        }
    }
    Ok(())
}

fn validate_provider_model_rule(rule: &ProviderModelRule, location: &str) -> Result<(), String> {
    for request_rule in rule.request_rules.iter() {
        for path in request_rule.remove.iter() {
            if !is_valid_json_pointer(path) {
                return Err(format!(
                    "{location}.request_rules contains invalid JSON pointer"
                ));
            }
        }
        if !request_rule.defaults.is_null() && !request_rule.defaults.is_object() {
            return Err(format!(
                "{location}.request_rules.defaults must be an object"
            ));
        }
        if !request_rule.set.is_null() && !request_rule.set.is_object() {
            return Err(format!("{location}.request_rules.set must be an object"));
        }
    }
    if !rule.provider_options.is_null() && !rule.provider_options.is_object() {
        return Err(format!("{location}.provider_options must be an object"));
    }
    if let Some(pricing) = rule.pricing.as_ref() {
        validate_pricing(pricing, location)?;
    }
    if rule
        .latency_class
        .as_deref()
        .into_iter()
        .chain(rule.cost_class.as_deref())
        .any(|value| value.trim().is_empty())
    {
        return Err(format!("{location} contains an empty class"));
    }
    Ok(())
}

fn validate_pricing(pricing: &ProviderPricing, location: &str) -> Result<(), String> {
    if pricing.currency.trim().is_empty() {
        return Err(format!("{location}.pricing.currency must be non-empty"));
    }
    let values = [
        pricing.input_token,
        pricing.output_token,
        pricing.cache_input_token,
        pricing.estimated_cost,
        pricing.amount,
    ];
    if values
        .into_iter()
        .flatten()
        .any(|value| !value.is_finite() || value < 0.0)
        || pricing
            .rules
            .iter()
            .any(|rule| !rule.amount.is_finite() || rule.amount < 0.0)
    {
        return Err(format!("{location}.pricing contains invalid amount"));
    }
    Ok(())
}

fn find_provider_rules<'a>(
    metadata: &ModelMetadata,
    rules: &'a ProviderRulesDocument,
) -> Vec<&'a ProviderModelRule> {
    find_provider_rules_for_identity(
        metadata.provider_model_id.as_str(),
        metadata.origin_model_id.as_deref(),
        rules,
    )
}

fn find_provider_rules_for_identity<'a>(
    provider_model_id: &str,
    origin_model_id: Option<&str>,
    rules: &'a ProviderRulesDocument,
) -> Vec<&'a ProviderModelRule> {
    let find_for_source = |source: ProviderRuleMatchSource| {
        rules
            .models
            .iter()
            .find(|rule| {
                rule.match_source == source
                    && rule.id.as_deref().is_some_and(|id| {
                        provider_rule_identity_value(
                            provider_model_id,
                            origin_model_id,
                            &rule.match_source,
                        )
                        .eq_ignore_ascii_case(id)
                    })
            })
            .or_else(|| {
                rules.patterns.iter().find(|rule| {
                    rule.match_source == source
                        && rule.pattern.as_deref().is_some_and(|pattern| {
                            wildcard_matches(
                                pattern,
                                provider_rule_identity_value(
                                    provider_model_id,
                                    origin_model_id,
                                    &rule.match_source,
                                ),
                            )
                        })
                })
            })
    };
    [
        find_for_source(ProviderRuleMatchSource::ProviderModelId),
        find_for_source(ProviderRuleMatchSource::OriginModelId),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn provider_rule_identity_value<'a>(
    provider_model_id: &'a str,
    origin_model_id: Option<&'a str>,
    source: &ProviderRuleMatchSource,
) -> &'a str {
    match source {
        ProviderRuleMatchSource::ProviderModelId => provider_model_id,
        ProviderRuleMatchSource::OriginModelId => origin_model_id.unwrap_or(provider_model_id),
    }
}

fn resolve_operation<'a>(
    defaults: &BTreeMap<String, String>,
    selected: impl IntoIterator<Item = &'a BTreeMap<String, String>>,
    method: &str,
    api_type: &ApiType,
    registry: &AdapterOperationRegistry,
) -> Result<String, String> {
    let serialized_api_type = serde_json::to_value(api_type)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| "cannot serialize api type".to_string())?;
    let selected = selected.into_iter().collect::<Vec<_>>();
    let operation = selected
        .iter()
        .find_map(|operations| operations.get(method))
        .or_else(|| {
            selected
                .iter()
                .find_map(|operations| operations.get(serialized_api_type.as_str()))
        })
        .or_else(|| defaults.get(method))
        .or_else(|| defaults.get(serialized_api_type.as_str()))
        .map(String::as_str)
        .or_else(|| registry.resolve_default(api_type))
        .ok_or_else(|| format!("no adapter operation for method {method}"))?;
    if !registry.supports(operation) {
        return Err(format!("adapter operation is not registered: {operation}"));
    }
    Ok(operation.to_string())
}

fn apply_request_rules(
    target: &mut Value,
    rules: &[ProviderRequestRule],
) -> Result<Vec<String>, String> {
    let mut applied = Vec::new();
    for rule in rules.iter() {
        if rule
            .when
            .as_ref()
            .is_some_and(|condition| !condition.matches(target))
        {
            continue;
        }
        merge_defaults(target, &rule.defaults);
        merge_json(target, rule.set.clone());
        for pointer in rule.remove.iter() {
            remove_json_pointer(target, pointer)?;
            applied.push(pointer.clone());
        }
    }
    Ok(applied)
}

fn apply_request_rules_with_ids(
    target: &mut Value,
    rules: &[ProviderRequestRule],
    scope: &str,
    applied: &mut Vec<String>,
) -> Result<(), String> {
    for (index, rule) in rules.iter().enumerate() {
        if rule
            .when
            .as_ref()
            .is_some_and(|condition| !condition.matches(target))
        {
            continue;
        }
        merge_defaults(target, &rule.defaults);
        merge_json(target, rule.set.clone());
        for pointer in rule.remove.iter() {
            remove_json_pointer(target, pointer)?;
        }
        if !rule.defaults.is_null() || !rule.set.is_null() || !rule.remove.is_empty() {
            applied.push(
                rule.id
                    .clone()
                    .unwrap_or_else(|| format!("{scope}.request_rules[{index}]")),
            );
        }
    }
    Ok(())
}

fn merge_defaults(target: &mut Value, defaults: &Value) {
    if let (Some(target), Some(defaults)) = (target.as_object_mut(), defaults.as_object()) {
        for (key, value) in defaults.iter() {
            if let Some(existing) = target.get_mut(key) {
                merge_defaults(existing, value);
            } else {
                target.insert(key.clone(), value.clone());
            }
        }
    } else if target.is_null() && !defaults.is_null() {
        *target = defaults.clone();
    }
}

fn merge_json(target: &mut Value, overlay: Value) {
    if overlay.is_null() {
        return;
    }
    match (target, overlay) {
        (Value::Object(target), Value::Object(overlay)) => {
            for (key, value) in overlay {
                if let Some(existing) = target.get_mut(key.as_str()) {
                    merge_json(existing, value);
                } else {
                    target.insert(key, value);
                }
            }
        }
        (target, overlay) => *target = overlay,
    }
}

fn is_valid_json_pointer(pointer: &str) -> bool {
    pointer.starts_with('/')
        && !pointer.split('/').skip(1).any(|part| {
            let mut chars = part.chars();
            while let Some(ch) = chars.next() {
                if ch == '~' && !matches!(chars.next(), Some('0' | '1')) {
                    return true;
                }
            }
            false
        })
}

fn remove_json_pointer(target: &mut Value, pointer: &str) -> Result<(), String> {
    if !is_valid_json_pointer(pointer) {
        return Err(format!("invalid JSON pointer: {pointer}"));
    }
    let mut segments = pointer
        .split('/')
        .skip(1)
        .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
        .collect::<Vec<_>>();
    let Some(last) = segments.pop() else {
        return Ok(());
    };
    let mut parent = target;
    for segment in segments {
        match parent {
            Value::Object(object) => {
                let Some(next) = object.get_mut(segment.as_str()) else {
                    return Ok(());
                };
                parent = next;
            }
            Value::Array(array) => {
                let Ok(index) = segment.parse::<usize>() else {
                    return Ok(());
                };
                let Some(next) = array.get_mut(index) else {
                    return Ok(());
                };
                parent = next;
            }
            _ => return Ok(()),
        }
    }
    match parent {
        Value::Object(object) => {
            object.remove(last.as_str());
        }
        Value::Array(array) => {
            if let Ok(index) = last.parse::<usize>() {
                if index < array.len() {
                    array.remove(index);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn model_pricing_is_known(pricing: &ModelPricing) -> bool {
    pricing.input_token.is_some()
        || pricing.output_token.is_some()
        || pricing.cache_input_token.is_some()
        || pricing.estimated_cost.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_resolver::{
        DriverOriginMatch, DriverOriginTransform, DriverOriginTransforms,
    };
    use crate::model_types::{ModelAttributes, ModelHealth};
    use serde_json::json;

    fn metadata() -> ModelMetadata {
        ModelMetadata {
            provider_model_id: "vendor/renamed-gpt".to_string(),
            exact_model: "vendor/renamed-gpt@example".to_string(),
            model_driver: "openai".to_string(),
            origin_model_id: Some("gpt-5-nano".to_string()),
            provider_actual_model_id: None,
            provider_options: None,
            parameter_scale: None,
            api_types: vec![ApiType::Llm, ApiType::Embedding],
            logical_mounts: vec!["llm.openai.gpt-5".to_string()],
            capabilities: ModelCapabilities {
                streaming: true,
                tool_call: true,
                json_schema: true,
                web_search: true,
                vision: true,
                image_generation: false,
                max_context_tokens: Some(128_000),
                max_output_tokens: Some(32_000),
                unsupported_feature_combinations: Vec::new(),
            },
            attributes: ModelAttributes::default(),
            pricing: ModelPricing {
                estimated_cost: Some(9.0),
                ..ModelPricing::default()
            },
            health: ModelHealth::default(),
        }
    }

    fn rules() -> ProviderRulesDocument {
        ProviderRulesDocument {
            format: "buckyos.aicc.provider-rules-catalog".to_string(),
            schema_version: 1,
            revision_seq: 7,
            provider_profile_id: "example-router".to_string(),
            patterns: vec![ProviderModelRule {
                pattern: Some("gpt-5-*".to_string()),
                match_source: ProviderRuleMatchSource::OriginModelId,
                operations: BTreeMap::from([(
                    "llm.chat".to_string(),
                    "chat.completions.create".to_string(),
                )]),
                request_rules: vec![ProviderRequestRule {
                    defaults: json!({"reasoning": {"effort": "minimal"}}),
                    when: Some(RequestCondition::Predicate(RequestPredicate {
                        path: "/reasoning/effort".to_string(),
                        op: RequestPredicateOp::NotEquals,
                        value: json!("none"),
                    })),
                    remove: vec!["/temperature".to_string()],
                    ..ProviderRequestRule::default()
                }],
                pricing: Some(ProviderPricing {
                    estimated_cost: Some(2.0),
                    ..ProviderPricing::default()
                }),
                remove_features: vec!["web_search".to_string()],
                ..ProviderModelRule::default()
            }],
            ..ProviderRulesDocument::default()
        }
    }

    fn registry() -> AdapterOperationRegistry {
        AdapterOperationRegistry::new(
            ["chat.completions.create"],
            [(ApiType::Llm, "chat.completions.create")],
        )
    }

    fn legacy_gpt_request_rules(model: &str, mut value: Value) -> Value {
        let target = value.as_object_mut().unwrap();
        let model = model.trim().to_ascii_lowercase();
        if model.starts_with("gpt-5-nano") {
            target
                .entry("reasoning".to_string())
                .or_insert_with(|| json!({"effort": "minimal"}));
            if !target.contains_key("verbosity") {
                target
                    .entry("text".to_string())
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .unwrap()
                    .entry("verbosity".to_string())
                    .or_insert_with(|| json!("low"));
            }
        }
        if model.starts_with("gpt-5") {
            let old_model = model == "gpt-5"
                || model.starts_with("gpt-5-")
                || model.starts_with("gpt-5-mini")
                || model.starts_with("gpt-5-nano");
            let codex = model.contains("codex");
            let none_effort = target
                .get("reasoning")
                .and_then(Value::as_object)
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str)
                == Some("none");
            if old_model || codex || !none_effort {
                for key in ["temperature", "top_p", "logprobs", "top_logprobs"] {
                    target.remove(key);
                }
            }
        }
        value
    }

    #[test]
    fn builtin_gpt_request_rules_are_equivalent_to_legacy_branches() {
        let rules = load_builtin_provider_rules("openai").unwrap();
        for (model, request) in [
            ("gpt-5", json!({"temperature": 0.2, "top_p": 0.9})),
            ("gpt-5-nano-2025-08-07", json!({"temperature": 0.2})),
            (
                "gpt-5-nano",
                json!({"reasoning": {"effort": "high"}, "text": {"verbosity": "high"}}),
            ),
            (
                "gpt-5.2-codex",
                json!({"reasoning": {"effort": "none"}, "temperature": 0.2}),
            ),
            (
                "gpt-5.4",
                json!({"reasoning": {"effort": "none"}, "temperature": 0.2}),
            ),
            ("gpt-5.4", json!({"temperature": 0.2, "logprobs": true})),
            ("gpt-4.1", json!({"temperature": 0.2})),
        ] {
            let mut actual = request.clone();
            let call = ResolvedProviderCall {
                provider_model_id: model.to_string(),
                model_driver: "openai".to_string(),
                origin_model_id: Some(model.to_string()),
                operation: "responses.create".to_string(),
                options: json!({}),
                pricing: ResolvedPricing {
                    definition: ProviderPricing::default(),
                    source: PricingSource::Unknown,
                    matched_amount: None,
                },
                provider_rules_revision: rules.revision_seq,
                applied_request_rules: Vec::new(),
            };
            apply_resolved_provider_request_rules(&call, &rules, &mut actual).unwrap();
            assert_eq!(actual, legacy_gpt_request_rules(model, request), "{model}");
        }
    }

    #[test]
    fn openrouter_renamed_gpt_uses_origin_request_rules_and_channel_operation() {
        let rules = load_builtin_provider_rules("openrouter").unwrap();
        let mut model = metadata();
        model.provider_model_id = "openai/gpt-5-nano".to_string();
        model.origin_model_id = Some("gpt-5-nano".to_string());
        let call = resolve_provider_call(ProviderCallResolveInput {
            metadata: &model,
            rules: &rules,
            method: "llm.chat",
            api_type: &ApiType::Llm,
            request_options: json!({"temperature": 0.2}),
            adapter_operations: &registry(),
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(call.provider_model_id, "openai/gpt-5-nano");
        assert_eq!(call.operation, "chat.completions.create");
        assert_eq!(
            call.options.pointer("/reasoning/effort"),
            Some(&json!("minimal"))
        );
        assert!(call.options.get("temperature").is_none());
        assert!(call
            .applied_request_rules
            .iter()
            .any(|rule| rule.starts_with("pattern:gpt-5-nano")));
    }

    #[test]
    fn openai_compatible_claude_keeps_claude_semantics_without_gpt_rules() {
        let rules = load_builtin_provider_rules("openrouter").unwrap();
        let model = resolve_provider_model_metadata(
            "openrouter-main",
            ProviderType::CloudApi,
            "anthropic/claude-sonnet-4-5",
            &rules,
            &[],
            vec![ApiType::Llm],
        )
        .unwrap()
        .unwrap();
        let call = resolve_provider_call(ProviderCallResolveInput {
            metadata: &model,
            rules: &rules,
            method: "llm.chat",
            api_type: &ApiType::Llm,
            request_options: json!({"temperature": 0.2}),
            adapter_operations: &registry(),
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(call.model_driver, "claude");
        assert_eq!(call.provider_model_id, "anthropic/claude-sonnet-4-5");
        assert_eq!(call.operation, "chat.completions.create");
        assert_eq!(call.options.get("temperature"), Some(&json!(0.2)));
    }

    #[test]
    fn openrouter_veo_uses_openai_compatible_video_operation() {
        let rules = load_builtin_provider_rules("openrouter").unwrap();
        let model = resolve_provider_model_metadata(
            "openrouter-main",
            ProviderType::CloudApi,
            "google/veo-3.1-generate-preview",
            &rules,
            &[],
            vec![ApiType::VideoTextToVideo],
        )
        .unwrap()
        .unwrap();
        let operations = AdapterOperationRegistry::new(
            ["videos.create"],
            [(ApiType::VideoTextToVideo, "videos.create")],
        );
        let call = resolve_provider_call(ProviderCallResolveInput {
            metadata: &model,
            rules: &rules,
            method: "video.txt2video",
            api_type: &ApiType::VideoTextToVideo,
            request_options: json!({}),
            adapter_operations: &operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(call.model_driver, "google-gemini");
        assert_eq!(call.operation, "videos.create");
        assert_ne!(call.operation, "google.models.predictLongRunning");
    }

    #[test]
    fn origin_rule_preserves_provider_model_and_resolves_operation() {
        let resolved = resolve_provider_call(ProviderCallResolveInput {
            metadata: &metadata(),
            rules: &rules(),
            method: "llm.chat",
            api_type: &ApiType::Llm,
            request_options: json!({"temperature": 0.2}),
            adapter_operations: &registry(),
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();

        assert_eq!(resolved.provider_model_id, "vendor/renamed-gpt");
        assert_eq!(resolved.model_driver, "openai");
        assert_eq!(resolved.operation, "chat.completions.create");
        assert_eq!(
            resolved.options.pointer("/reasoning/effort"),
            Some(&json!("minimal"))
        );
        assert!(resolved.options.get("temperature").is_none());
        assert_eq!(resolved.pricing.source, PricingSource::ProviderRules);
    }

    #[test]
    fn pricing_priority_is_discovery_instance_provider_model() {
        let rule_set = rules();
        let model = metadata();
        let adapter = registry();
        let resolve = |discovery, instance| {
            resolve_provider_call(ProviderCallResolveInput {
                metadata: &model,
                rules: &rule_set,
                method: "llm.chat",
                api_type: &ApiType::Llm,
                request_options: json!({}),
                adapter_operations: &adapter,
                discovery_pricing: discovery,
                instance_pricing: instance,
            })
            .unwrap()
            .pricing
        };
        assert_eq!(
            resolve(
                Some(ProviderPricing {
                    estimated_cost: Some(0.5),
                    ..ProviderPricing::default()
                }),
                Some(ProviderPricing {
                    estimated_cost: Some(1.0),
                    ..ProviderPricing::default()
                })
            )
            .source,
            PricingSource::ProviderDiscovery
        );
        assert_eq!(
            resolve(
                None,
                Some(ProviderPricing {
                    estimated_cost: Some(1.0),
                    ..ProviderPricing::default()
                })
            )
            .source,
            PricingSource::ProviderInstance
        );
        assert_eq!(resolve(None, None).source, PricingSource::ProviderRules);
    }

    #[test]
    fn provider_rules_only_remove_capabilities() {
        let mut model = metadata();
        apply_provider_capability_limits(&mut model, &rules()).unwrap();
        assert!(!model.capabilities.web_search);
        assert!(model.capabilities.tool_call);
        assert_eq!(model.api_types, vec![ApiType::Llm, ApiType::Embedding]);
    }

    #[test]
    fn unknown_operation_is_rejected() {
        let mut rule_set = rules();
        rule_set.patterns[0].operations.insert(
            "llm.chat".to_string(),
            "https://attacker.example/run".to_string(),
        );
        let err = resolve_provider_call(ProviderCallResolveInput {
            metadata: &metadata(),
            rules: &rule_set,
            method: "llm.chat",
            api_type: &ApiType::Llm,
            request_options: json!({}),
            adapter_operations: &registry(),
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap_err();
        assert!(err.contains("not registered"));
    }

    #[test]
    fn empty_provider_override_deserializes() {
        let value: ProviderRulesDocument = serde_json::from_value(json!({
            "format": "buckyos.aicc.provider-rules-catalog",
            "schema_version": 1,
            "revision_seq": 1,
            "provider_profile_id": "custom"
        }))
        .unwrap();
        validate_provider_rules_document(&value).unwrap();
        assert!(value.metadata_drivers.is_none());
        let operations =
            AdapterOperationRegistry::new(["custom.default"], [(ApiType::Llm, "custom.default")]);
        let call = resolve_provider_call(ProviderCallResolveInput {
            metadata: &metadata(),
            rules: &value,
            method: "llm.chat",
            api_type: &ApiType::Llm,
            request_options: json!({}),
            adapter_operations: &operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(call.operation, "custom.default");
        assert_eq!(call.options, json!({}));
    }

    #[test]
    fn official_gemini_video_operations_differ_from_openrouter() {
        let google_rules = load_builtin_provider_rules("google-gemini").unwrap();
        let google_operations = AdapterOperationRegistry::new(
            [
                "google.models.generateContent",
                "google.models.predictLongRunning",
                "google.interactions.create",
            ],
            [
                (
                    ApiType::VideoTextToVideo,
                    "google.models.predictLongRunning",
                ),
                (ApiType::VideoToVideo, "google.interactions.create"),
                (ApiType::AudioAsr, "google.models.generateContent"),
            ],
        );
        let veo = resolve_provider_model_metadata(
            "google-main",
            ProviderType::CloudApi,
            "veo-3.1-generate-preview",
            &google_rules,
            &[],
            vec![ApiType::VideoTextToVideo],
        )
        .unwrap()
        .unwrap();
        let official_veo = resolve_provider_call(ProviderCallResolveInput {
            metadata: &veo,
            rules: &google_rules,
            method: "video.txt2video",
            api_type: &ApiType::VideoTextToVideo,
            request_options: json!({}),
            adapter_operations: &google_operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(official_veo.operation, "google.models.predictLongRunning");

        let omni = resolve_provider_model_metadata(
            "google-main",
            ProviderType::CloudApi,
            "gemini-omni-flash-preview",
            &google_rules,
            &[],
            vec![ApiType::VideoToVideo],
        )
        .unwrap()
        .unwrap();
        let official_omni = resolve_provider_call(ProviderCallResolveInput {
            metadata: &omni,
            rules: &google_rules,
            method: "video.video2video",
            api_type: &ApiType::VideoToVideo,
            request_options: json!({}),
            adapter_operations: &google_operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(official_omni.operation, "google.interactions.create");

        let transcribe = resolve_provider_model_metadata(
            "google-main",
            ProviderType::CloudApi,
            "gemini-3.5-transcribe",
            &google_rules,
            &[],
            vec![ApiType::AudioAsr],
        )
        .unwrap()
        .unwrap();
        let official_transcribe = resolve_provider_call(ProviderCallResolveInput {
            metadata: &transcribe,
            rules: &google_rules,
            method: "audio.asr",
            api_type: &ApiType::AudioAsr,
            request_options: json!({}),
            adapter_operations: &google_operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(official_transcribe.operation, "google.interactions.create");

        let openrouter_rules = load_builtin_provider_rules("openrouter").unwrap();
        let aggregate_veo = resolve_provider_model_metadata(
            "openrouter-main",
            ProviderType::CloudApi,
            "google/veo-3.1-generate-preview",
            &openrouter_rules,
            &[],
            vec![ApiType::VideoTextToVideo],
        )
        .unwrap()
        .unwrap();
        let aggregate_operations = AdapterOperationRegistry::new(
            ["videos.create"],
            [(ApiType::VideoTextToVideo, "videos.create")],
        );
        let aggregate_call = resolve_provider_call(ProviderCallResolveInput {
            metadata: &aggregate_veo,
            rules: &openrouter_rules,
            method: "video.txt2video",
            api_type: &ApiType::VideoTextToVideo,
            request_options: json!({}),
            adapter_operations: &aggregate_operations,
            discovery_pricing: None,
            instance_pricing: None,
        })
        .unwrap();
        assert_eq!(aggregate_call.operation, "videos.create");
        assert_ne!(aggregate_call.operation, official_veo.operation);
    }

    #[test]
    fn all_builtin_provider_rules_are_valid() {
        for provider in [
            "openai",
            "openrouter",
            "claude",
            "google-gemini",
            "sn-ai-provider",
            "minimax",
            "fal",
            "custom-openai-compatible",
        ] {
            let rules = load_builtin_provider_rules(provider)
                .unwrap_or_else(|| panic!("missing provider rules for {provider}"));
            validate_provider_rules_document(&rules)
                .unwrap_or_else(|error| panic!("invalid provider rules for {provider}: {error}"));
        }
    }

    #[test]
    fn provider_mapping_resolves_origin_before_model_driver_matching() {
        let rules = ProviderRulesDocument {
            format: "buckyos.aicc.provider-rules-catalog".to_string(),
            schema_version: 1,
            revision_seq: 1,
            provider_profile_id: "example-router".to_string(),
            metadata_drivers: Some(vec!["openai".to_string(), "claude".to_string()]),
            origin_provider_aliases: HashMap::from([(
                "anthropic".to_string(),
                "claude".to_string(),
            )]),
            origin_mappings: vec![DriverOriginMapping {
                mapping_key: "vendor-model".to_string(),
                priority: 0,
                match_rule: DriverOriginMatch {
                    source: "provider_model_id".to_string(),
                    regex: "^(?<driver>[^/]+)/(?<model>.+)$".to_string(),
                },
                transforms: DriverOriginTransforms {
                    driver: vec![
                        DriverOriginTransform {
                            op: "lowercase".to_string(),
                            table: None,
                            on_missing: None,
                        },
                        DriverOriginTransform {
                            op: "alias".to_string(),
                            table: Some("origin_provider_aliases".to_string()),
                            on_missing: Some("keep".to_string()),
                        },
                    ],
                    model: vec![DriverOriginTransform {
                        op: "trim".to_string(),
                        table: None,
                        on_missing: None,
                    }],
                },
            }],
            ..ProviderRulesDocument::default()
        };
        let metadata = resolve_provider_model_metadata(
            "router-main",
            ProviderType::CloudApi,
            "anthropic/claude-sonnet-4-5",
            &rules,
            &[],
            vec![ApiType::Llm],
        )
        .unwrap()
        .unwrap();
        assert_eq!(metadata.model_driver, "claude");
        assert_eq!(
            metadata.origin_model_id.as_deref(),
            Some("claude-sonnet-4-5")
        );
        assert_eq!(metadata.provider_model_id, "anthropic/claude-sonnet-4-5");
    }

    #[test]
    fn explicit_empty_metadata_drivers_forces_conservative_fallback() {
        let rules = ProviderRulesDocument {
            format: "buckyos.aicc.provider-rules-catalog".to_string(),
            schema_version: 1,
            revision_seq: 1,
            provider_profile_id: "custom".to_string(),
            metadata_drivers: Some(Vec::new()),
            ..ProviderRulesDocument::default()
        };
        let metadata = resolve_provider_model_metadata(
            "custom-main",
            ProviderType::ProxyUnknown,
            "gpt-5.4",
            &rules,
            &["openai".to_string()],
            vec![ApiType::Llm],
        )
        .unwrap()
        .unwrap();
        assert_eq!(metadata.model_driver, "conservative");
    }

    #[test]
    fn openai_image_pricing_uses_request_conditions() {
        let rules = load_builtin_provider_rules("openai").unwrap();
        let mut model = metadata();
        model.provider_model_id = "gpt-image-1".to_string();
        model.origin_model_id = Some("gpt-image-1".to_string());
        let pricing = resolve_provider_pricing(
            &model,
            &rules,
            &json!({"quality": "high", "size": "1536x1024"}),
            None,
            None,
        );
        assert_eq!(pricing.source, PricingSource::ProviderRules);
        assert_eq!(pricing.matched_amount, Some(0.25));
    }

    #[test]
    fn provider_runtime_has_no_model_family_dispatch_fragments() {
        let sources = [
            ("openai", include_str!("openai.rs")),
            ("claude", include_str!("claude.rs")),
            ("gemini", include_str!("gemini.rs")),
            ("minimax", include_str!("minimax.rs")),
            ("fal", include_str!("fal.rs")),
            ("sn", include_str!("sn_ai_provider.rs")),
        ];
        let forbidden = [
            "starts_with(\"gpt-",
            "starts_with(\"dall-e",
            "starts_with(\"sora-",
            "starts_with(\"claude-\")",
            "starts_with(\"MiniMax-M2",
            "contains(\"highspeed\")",
            "contains(\"opus\")",
            "contains(\"haiku\")",
            "contains(\"lyria\")",
            "contains(\"veo\")",
        ];
        for (provider, source) in sources {
            for fragment in forbidden {
                assert!(
                    !source.contains(fragment),
                    "{provider} reintroduced model-name dispatch: {fragment}"
                );
            }
        }
    }
}
