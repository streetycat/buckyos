use crate::aicc::exact_model_name;
use crate::model_types::{
    is_model_feature_name, ApiType, CostClass, HealthStatus, LatencyClass, ModelAttributes,
    ModelCapabilities, ModelHealth, ModelMetadata, ModelPricing, PrivacyClass, ProviderInventory,
    ProviderOrigin, ProviderType, ProviderTypeTrustedSource, QuotaState,
};
use buckyos_kit::get_buckyos_system_etc_dir;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

const DRIVER_METADATA_SCHEMA_VERSION: u32 = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelDriverMatchError {
    ExplicitDriverUnavailable(String),
    Ambiguous {
        provider_model_id: String,
        model_drivers: Vec<String>,
    },
}

impl std::fmt::Display for ModelDriverMatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExplicitDriverUnavailable(driver) => {
                write!(f, "explicit model driver is unavailable: {driver}")
            }
            Self::Ambiguous {
                provider_model_id,
                model_drivers,
            } => write!(
                f,
                "model '{}' matches multiple model drivers: {}",
                provider_model_id,
                model_drivers.join(", ")
            ),
        }
    }
}

impl std::error::Error for ModelDriverMatchError {}

#[derive(Clone, Debug, Default)]
pub struct DriverModelResolveRequest {
    pub provider_model_id: String,
    pub fallback_api_types: Vec<ApiType>,
    pub fallback_logical_mounts: Vec<String>,
    pub fallback_estimated_cost_usd: Option<f64>,
    pub fallback_estimated_latency_ms: Option<u64>,
}

impl DriverModelResolveRequest {
    pub fn new(provider_model_id: impl Into<String>, fallback_api_types: Vec<ApiType>) -> Self {
        Self {
            provider_model_id: provider_model_id.into(),
            fallback_api_types,
            fallback_logical_mounts: Vec::new(),
            fallback_estimated_cost_usd: None,
            fallback_estimated_latency_ms: None,
        }
    }

    pub fn with_cost(mut self, estimated_cost_usd: Option<f64>) -> Self {
        self.fallback_estimated_cost_usd = estimated_cost_usd;
        self
    }

    pub fn with_latency(mut self, estimated_latency_ms: Option<u64>) -> Self {
        self.fallback_estimated_latency_ms = estimated_latency_ms;
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverMetadataDocument {
    #[serde(default)]
    pub format: String,
    pub schema_version: u32,
    #[serde(default)]
    pub schema_revision: u32,
    pub model_driver_id: String,
    pub revision_seq: u64,
    #[serde(default)]
    pub required_features: Vec<String>,
    #[serde(default)]
    pub models: Vec<DriverModelRule>,
    #[serde(default)]
    pub patterns: Vec<DriverModelRule>,
    #[serde(default)]
    pub defaults: DriverModelRule,
    #[serde(default)]
    pub variants: Vec<DriverModelVariant>,
    #[serde(default)]
    pub version_rules: Vec<DriverVersionRule>,
    #[serde(default)]
    pub signature: Option<DriverMetadataSignature>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverOriginMapping {
    #[serde(default)]
    pub mapping_key: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default, rename = "match")]
    pub match_rule: DriverOriginMatch,
    #[serde(default)]
    pub transforms: DriverOriginTransforms,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverOriginMatch {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub regex: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverOriginTransforms {
    #[serde(default)]
    pub driver: Vec<DriverOriginTransform>,
    #[serde(default)]
    pub model: Vec<DriverOriginTransform>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverOriginTransform {
    #[serde(default)]
    pub op: String,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub on_missing: Option<String>,
}

#[derive(Clone, Debug)]
struct DriverOriginIdentity {
    driver: String,
    model: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverModelRule {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub model_driver: Option<String>,
    #[serde(default)]
    pub exclude: bool,
    #[serde(default)]
    pub parameter_scale: Option<String>,
    #[serde(default)]
    pub api_types: Option<Vec<ApiType>>,
    #[serde(default)]
    pub logical_mounts: Option<Vec<String>>,
    #[serde(default)]
    pub capabilities: DriverCapabilitiesPatch,
    #[serde(default)]
    pub pricing: Option<ModelPricing>,
    #[serde(default)]
    pub estimated_latency_ms: Option<u64>,
    #[serde(default)]
    pub quality_score: Option<f64>,
    #[serde(default)]
    pub latency_class: Option<LatencyClass>,
    #[serde(default)]
    pub cost_class: Option<CostClass>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverVersionRule {
    #[serde(default)]
    pub family: String,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub model_pattern: Option<String>,
    #[serde(default)]
    pub tier_tokens: Vec<String>,
    #[serde(default)]
    pub tier_patterns: Vec<String>,
    #[serde(default)]
    pub exclude_tier_tokens: Vec<String>,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    #[serde(default)]
    pub version_rank: DriverVersionRankRule,
    #[serde(default)]
    pub stability: DriverVersionStabilityRule,
    #[serde(default)]
    pub current_mount: Option<String>,
    #[serde(default)]
    pub version_mount: Option<String>,
    #[serde(default)]
    pub auto_mounts: Vec<String>,
    #[serde(default)]
    pub exclude_snapshot_date_suffix: bool,
    #[serde(default)]
    pub capabilities: DriverCapabilitiesPatch,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverVersionRankRule {
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverVersionStabilityRule {
    #[serde(default)]
    pub unstable_tokens: Vec<String>,
    #[serde(default)]
    pub current_requires_stable: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverCapabilitiesPatch {
    #[serde(default)]
    pub streaming: Option<bool>,
    #[serde(default)]
    pub tool_call: Option<bool>,
    #[serde(default)]
    pub json_schema: Option<bool>,
    #[serde(default)]
    pub web_search: Option<bool>,
    #[serde(default)]
    pub unsupported_feature_combinations: Option<Vec<Vec<String>>>,
    #[serde(default)]
    pub vision: Option<bool>,
    #[serde(default)]
    pub image_generation: Option<bool>,
    #[serde(default)]
    pub max_context_tokens: Option<u64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverModelVariant {
    pub name: String,
    #[serde(default)]
    pub model_pattern: Option<String>,
    #[serde(default)]
    pub mount_suffix: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverMetadataSignature {
    #[serde(default)]
    pub algorithm: String,
    #[serde(default)]
    pub key_id: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Clone, Debug)]
struct DriverMetadataSource {
    name: String,
    document: DriverMetadataDocument,
    exact_model_index: HashMap<String, usize>,
}

impl DriverMetadataSource {
    fn new(name: String, document: DriverMetadataDocument) -> Self {
        let exact_model_index = document
            .models
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| {
                rule.id
                    .as_deref()
                    .map(|id| (id.to_ascii_lowercase(), index))
            })
            .collect();
        Self {
            name,
            document,
            exact_model_index,
        }
    }
}

pub fn resolve_driver_inventory(
    provider_instance_name: &str,
    provider_type: ProviderType,
    model_driver_id: &str,
    requests: &[DriverModelResolveRequest],
    inventory_revision: Option<String>,
) -> ProviderInventory {
    let (sources, driver_metadata_generation) = load_driver_metadata_sources(model_driver_id);
    let mut models = Vec::new();
    for request in requests.iter() {
        if let Some(metadata) = resolve_driver_model(
            provider_instance_name,
            provider_type.clone(),
            model_driver_id,
            request,
            sources.as_slice(),
        ) {
            models.push(metadata);
        }
    }
    apply_driver_post_rules(model_driver_id, &mut models, sources.as_slice());
    models = models
        .into_iter()
        .flat_map(|metadata| expand_model_variants(metadata, sources.as_slice()))
        .collect();

    ProviderInventory {
        provider_instance_name: provider_instance_name.to_string(),
        provider_type,
        provider_profile_id: model_driver_id.to_string(),
        protocol_adapter_id: model_driver_id.to_string(),
        provider_origin: ProviderOrigin::SystemConfig,
        provider_type_trusted_source: ProviderTypeTrustedSource::SystemConfig,
        provider_type_revision: None,
        version: None,
        inventory_revision,
        driver_metadata_generation,
        models,
    }
}

pub fn match_model_driver(
    origin_model_id: &str,
    candidate_drivers: &[String],
    explicit_model_driver: Option<&str>,
) -> Result<Option<String>, ModelDriverMatchError> {
    let origin_model_id = origin_model_id.trim();
    if origin_model_id.is_empty() {
        return Ok(None);
    }

    let mut normalized_candidates = Vec::<String>::new();
    for candidate in candidate_drivers {
        let candidate = normalize_driver(candidate);
        if !candidate.is_empty()
            && !normalized_candidates
                .iter()
                .any(|current| current.eq_ignore_ascii_case(candidate.as_str()))
        {
            normalized_candidates.push(candidate);
        }
    }

    if let Some(explicit) = explicit_model_driver {
        let explicit = normalize_driver(explicit);
        let allowed = normalized_candidates.is_empty()
            || normalized_candidates
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(explicit.as_str()));
        let (sources, _) = load_driver_metadata_sources(explicit.as_str());
        if allowed && !sources.is_empty() {
            return Ok(sources
                .last()
                .map(|source| normalize_driver(source.document.model_driver_id.as_str())));
        }
        return Err(ModelDriverMatchError::ExplicitDriverUnavailable(explicit));
    }

    let mut matches = Vec::<String>::new();
    for candidate in normalized_candidates {
        let (sources, _) = load_driver_metadata_sources(candidate.as_str());
        if sources.is_empty() {
            continue;
        }
        let matched = find_exact_rule(origin_model_id, sources.as_slice())
            .or_else(|| find_pattern_rule(origin_model_id, sources.as_slice()))
            .is_some_and(|rule| !rule.exclude);
        if matched {
            let model_driver = sources
                .last()
                .map(|source| normalize_driver(source.document.model_driver_id.as_str()))
                .unwrap_or(candidate);
            if !matches
                .iter()
                .any(|current| current.eq_ignore_ascii_case(model_driver.as_str()))
            {
                matches.push(model_driver);
            }
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => Err(ModelDriverMatchError::Ambiguous {
            provider_model_id: origin_model_id.to_string(),
            model_drivers: matches,
        }),
    }
}

pub fn resolve_model_driver_metadata(
    provider_instance_name: &str,
    provider_type: ProviderType,
    provider_model_id: &str,
    origin_model_id: &str,
    candidate_drivers: &[String],
    explicit_model_driver: Option<&str>,
    fallback_api_types: Vec<ApiType>,
) -> Result<Option<ModelMetadata>, ModelDriverMatchError> {
    Ok(resolve_model_driver_metadata_variants(
        provider_instance_name,
        provider_type,
        provider_model_id,
        origin_model_id,
        candidate_drivers,
        explicit_model_driver,
        fallback_api_types,
    )?
    .into_iter()
    .next())
}

pub fn resolve_model_driver_metadata_variants(
    provider_instance_name: &str,
    provider_type: ProviderType,
    provider_model_id: &str,
    origin_model_id: &str,
    candidate_drivers: &[String],
    explicit_model_driver: Option<&str>,
    fallback_api_types: Vec<ApiType>,
) -> Result<Vec<ModelMetadata>, ModelDriverMatchError> {
    let Some(model_driver) =
        match_model_driver(origin_model_id, candidate_drivers, explicit_model_driver)?
    else {
        return Ok(Vec::new());
    };
    let (sources, _) = load_driver_metadata_sources(model_driver.as_str());
    let request = DriverModelResolveRequest::new(origin_model_id, fallback_api_types);
    let Some(metadata) = resolve_driver_model(
        provider_instance_name,
        provider_type,
        model_driver.as_str(),
        &request,
        sources.as_slice(),
    ) else {
        return Ok(Vec::new());
    };
    let variants = expand_model_variants(metadata, sources.as_slice());
    Ok(variants
        .into_iter()
        .map(|mut metadata| {
            let suffix = metadata
                .provider_model_id
                .strip_prefix(origin_model_id)
                .filter(|suffix| suffix.starts_with(':'))
                .map(str::to_string);
            metadata.provider_model_id = suffix
                .as_deref()
                .map(|suffix| format!("{provider_model_id}{suffix}"))
                .unwrap_or_else(|| provider_model_id.to_string());
            metadata.exact_model =
                exact_model_name(metadata.provider_model_id.as_str(), provider_instance_name);
            metadata.model_driver = model_driver.clone();
            metadata.origin_model_id = Some(origin_model_id.to_string());
            metadata.provider_actual_model_id = suffix.map(|_| provider_model_id.to_string());
            metadata
        })
        .collect())
}

pub fn conservative_model_metadata(
    provider_instance_name: &str,
    provider_type: ProviderType,
    provider_model_id: &str,
    origin_model_id: &str,
    fallback_api_types: Vec<ApiType>,
) -> ModelMetadata {
    let request = DriverModelResolveRequest::new(origin_model_id, fallback_api_types);
    let mut metadata = resolve_driver_model(
        provider_instance_name,
        provider_type,
        "conservative",
        &request,
        &[],
    )
    .expect("non-empty provider model must produce conservative metadata");
    metadata.provider_model_id = provider_model_id.to_string();
    metadata.exact_model = exact_model_name(provider_model_id, provider_instance_name);
    metadata.model_driver = "conservative".to_string();
    metadata.origin_model_id = Some(origin_model_id.to_string());
    metadata.provider_actual_model_id = None;
    metadata
}

pub fn model_driver_metadata_generation(model_drivers: &[String]) -> u64 {
    model_drivers
        .iter()
        .map(|model_driver| load_driver_metadata_sources(model_driver).1)
        .max()
        .unwrap_or(0)
}

pub(crate) fn driver_model_has_specific_metadata(
    model_driver_id: &str,
    provider_model_id: &str,
) -> bool {
    let (sources, _) = load_driver_metadata_sources(model_driver_id);
    find_exact_rule(provider_model_id, sources.as_slice()).is_some()
        || find_pattern_rule(provider_model_id, sources.as_slice()).is_some()
}

pub(crate) fn driver_metadata_model_ids(model_driver_id: &str, api_type: &ApiType) -> Vec<String> {
    let (sources, _) = load_driver_metadata_sources(model_driver_id);
    let mut models = Vec::<String>::new();
    for source in sources {
        for rule in source.document.models {
            let Some(model_id) = rule.id.map(|value| value.trim().to_string()) else {
                continue;
            };
            if model_id.is_empty() {
                continue;
            }
            models.retain(|current| !current.eq_ignore_ascii_case(model_id.as_str()));
            if !rule.exclude
                && rule
                    .api_types
                    .as_ref()
                    .map(|api_types| api_types.contains(api_type))
                    .unwrap_or(false)
            {
                models.push(model_id);
            }
        }
    }
    models
}

pub(crate) fn max_driver_metadata_cost(
    model_driver_id: &str,
    api_type: &ApiType,
    input_tokens: u64,
    output_tokens: u64,
) -> Option<(f64, String)> {
    let (sources, _) = load_driver_metadata_sources(model_driver_id);
    sources
        .iter()
        .flat_map(|source| {
            source
                .document
                .models
                .iter()
                .chain(source.document.patterns.iter())
                .chain(std::iter::once(&source.document.defaults))
        })
        .filter(|rule| {
            rule.api_types
                .as_ref()
                .map(|api_types| api_types.contains(api_type))
                .unwrap_or(false)
        })
        .filter_map(|rule| rule.pricing.as_ref())
        .filter(|pricing| pricing.currency.eq_ignore_ascii_case("USD"))
        .filter_map(|pricing| {
            let amount = match (pricing.input_token, pricing.output_token) {
                (Some(input_price), Some(output_price)) => {
                    (input_tokens as f64 * input_price) + (output_tokens as f64 * output_price)
                }
                _ => pricing.estimated_cost?,
            };
            amount
                .is_finite()
                .then(|| (amount, pricing.currency.clone()))
        })
        .max_by(|left, right| left.0.total_cmp(&right.0))
}

fn resolve_driver_model(
    provider_instance_name: &str,
    provider_type: ProviderType,
    model_driver_id: &str,
    request: &DriverModelResolveRequest,
    sources: &[DriverMetadataSource],
) -> Option<ModelMetadata> {
    let provider_model_id = request.provider_model_id.trim();
    if provider_model_id.is_empty() {
        return None;
    }
    let origin = resolve_origin_identity(model_driver_id, provider_model_id, sources);

    let exact_rule = find_exact_rule(provider_model_id, sources);
    let pattern_rule = if exact_rule.is_none() {
        find_pattern_rule(provider_model_id, sources)
    } else {
        None
    };
    let default_rule = if exact_rule.is_none() && pattern_rule.is_none() {
        find_default_rule(sources)
    } else {
        None
    };
    let rule = exact_rule.or(pattern_rule).or(default_rule);
    let driver_rule_found = rule.is_some();

    if rule.map(|rule| rule.exclude).unwrap_or(false) {
        return None;
    }

    let mut api_types = request.fallback_api_types.clone();
    if api_types.is_empty() {
        api_types.push(ApiType::Llm);
    }
    let mut logical_mounts = Vec::new();

    let mut capabilities = conservative_capabilities();
    let mut parameter_scale = None;
    let mut pricing = ModelPricing {
        estimated_cost: request.fallback_estimated_cost_usd,
        ..Default::default()
    };
    let mut estimated_latency_ms = request.fallback_estimated_latency_ms;
    let mut quality_score = Some(0.75);
    let mut latency_class = LatencyClass::Normal;
    let mut cost_class = CostClass::Medium;
    let mut model_driver = origin.driver.clone();
    if let Some(rule) = rule {
        if let Some(next) = rule.model_driver.as_ref() {
            model_driver = next.clone();
        }
        if let Some(next_api_types) = rule.api_types.as_ref() {
            api_types = next_api_types.clone();
        }
        if let Some(next_mounts) = rule.logical_mounts.as_ref() {
            logical_mounts = next_mounts
                .iter()
                .map(|mount| {
                    expand_mount_template(mount, model_driver_id, provider_model_id, &origin)
                })
                .collect();
        }
        apply_capabilities_patch(&mut capabilities, &rule.capabilities);
        if rule.parameter_scale.is_some() {
            parameter_scale = rule.parameter_scale.clone();
        }
        if let Some(rule_pricing) = rule.pricing.as_ref() {
            pricing.currency.clone_from(&rule_pricing.currency);
            if rule_pricing.estimated_cost.is_some() {
                pricing.estimated_cost = rule_pricing.estimated_cost;
            }
            pricing.input_token = rule_pricing.input_token;
            pricing.output_token = rule_pricing.output_token;
            pricing.cache_input_token = rule_pricing.cache_input_token;
        }
        if rule.estimated_latency_ms.is_some() {
            estimated_latency_ms = rule.estimated_latency_ms;
        }
        if rule.quality_score.is_some() {
            quality_score = rule.quality_score;
        }
        if let Some(next) = rule.latency_class.clone() {
            latency_class = next;
        }
        if let Some(next) = rule.cost_class.clone() {
            cost_class = next;
        }
    }
    if logical_mounts.is_empty() && !driver_rule_found {
        logical_mounts = provider_fallback_mounts(request.fallback_logical_mounts.as_slice());
    }
    if logical_mounts.is_empty() {
        logical_mounts = generic_mounts(&origin, api_types.as_slice());
    }
    if api_types
        .iter()
        .any(|api_type| matches!(api_type, ApiType::Llm))
    {
        for mount in semantic_llm_family_mounts(origin.model.as_str()) {
            add_unique(&mut logical_mounts, mount);
        }
    }

    logical_mounts = dedupe_strings(logical_mounts);
    Some(ModelMetadata {
        provider_model_id: provider_model_id.to_string(),
        exact_model: exact_model_name(provider_model_id, provider_instance_name),
        model_driver,
        origin_model_id: Some(origin.model),
        provider_actual_model_id: None,
        provider_options: None,
        parameter_scale,
        api_types,
        logical_mounts,
        capabilities,
        attributes: ModelAttributes {
            provider_type: provider_type.clone(),
            local: provider_type == ProviderType::LocalInference,
            privacy: if provider_type == ProviderType::LocalInference {
                PrivacyClass::Local
            } else {
                PrivacyClass::Cloud
            },
            quality_score,
            latency_class,
            cost_class,
        },
        pricing,
        health: ModelHealth {
            status: HealthStatus::Available,
            p95_latency_ms: estimated_latency_ms,
            quota_state: QuotaState::Normal,
            ..Default::default()
        },
    })
}

fn load_driver_metadata_sources(model_driver_id: &str) -> (Vec<DriverMetadataSource>, u64) {
    let mut sources = Vec::new();
    if let Some(document) = load_builtin_driver_metadata(model_driver_id) {
        sources.push(DriverMetadataSource::new("builtin".to_string(), document));
    }
    let normalized = normalize_driver(model_driver_id);
    let (remote_document, driver_metadata_generation) =
        crate::metadata_updater::load_active_remote_metadata(&normalized);
    if let Some(document) = remote_document {
        sources.push(DriverMetadataSource::new(
            "remote_cache_v1".to_string(),
            document,
        ));
    }
    for (name, path) in driver_metadata_override_paths(model_driver_id) {
        match std::fs::read_to_string(path.as_path()) {
            Ok(content) => match parse_and_validate_driver_metadata(
                content.as_str(),
                Some(normalized.as_str()),
            ) {
                Ok(document) => sources.push(DriverMetadataSource::new(name, document)),
                Err(err) => warn!(
                    "aicc.metadata_resolver.skip_invalid_metadata path={} err={}",
                    path.display(),
                    err
                ),
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warn!(
                "aicc.metadata_resolver.skip_unreadable_metadata path={} err={}",
                path.display(),
                err
            ),
        }
    }
    (sources, driver_metadata_generation)
}

fn parse_driver_metadata(content: &str) -> Result<DriverMetadataDocument, serde_json::Error> {
    serde_json::from_str::<DriverMetadataDocument>(content)
}

fn parse_and_validate_driver_metadata(
    content: &str,
    expected_model_driver_id: Option<&str>,
) -> Result<DriverMetadataDocument, String> {
    let document = parse_driver_metadata(content).map_err(|err| err.to_string())?;
    validate_driver_metadata_document(&document)?;
    if let Some(expected) = expected_model_driver_id {
        if document.model_driver_id != expected {
            return Err(format!(
                "model_driver_id '{}' does not match expected '{}'",
                document.model_driver_id, expected
            ));
        }
    }
    Ok(document)
}

pub(crate) fn validate_driver_metadata_document(
    document: &DriverMetadataDocument,
) -> Result<(), String> {
    if document.format != "buckyos.aicc.provider-driver-metadata" {
        return Err("unsupported provider metadata format".to_string());
    }
    if document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
        return Err(format!(
            "unsupported provider metadata schema_version {}",
            document.schema_version
        ));
    }
    if document.model_driver_id.is_empty()
        || document.model_driver_id.trim() != document.model_driver_id
    {
        return Err("model_driver_id must be a non-empty trimmed string".to_string());
    }
    if !document.required_features.is_empty() {
        return Err("provider metadata requires unsupported features".to_string());
    }

    let mut model_ids = HashSet::new();
    for (index, rule) in document.models.iter().enumerate() {
        let id = rule
            .id
            .as_deref()
            .ok_or_else(|| format!("models[{}].id is required", index))?;
        validate_trimmed_value(id, format!("models[{}].id", index).as_str())?;
        if rule.pattern.is_some() {
            return Err(format!("models[{}].pattern is not allowed", index));
        }
        if !model_ids.insert(id.to_ascii_lowercase()) {
            return Err(format!("duplicate model id '{}'", id));
        }
        validate_driver_model_rule(rule, format!("models[{}]", index).as_str())?;
    }

    let mut patterns = HashSet::new();
    for (index, rule) in document.patterns.iter().enumerate() {
        let pattern = rule
            .pattern
            .as_deref()
            .ok_or_else(|| format!("patterns[{}].pattern is required", index))?;
        validate_trimmed_value(pattern, format!("patterns[{}].pattern", index).as_str())?;
        if rule.id.is_some() {
            return Err(format!("patterns[{}].id is not allowed", index));
        }
        if !patterns.insert(pattern.to_ascii_lowercase()) {
            return Err(format!("duplicate model pattern '{}'", pattern));
        }
        validate_driver_model_rule(rule, format!("patterns[{}]", index).as_str())?;
    }

    if document.defaults.id.is_some() || document.defaults.pattern.is_some() {
        return Err("defaults cannot declare id or pattern".to_string());
    }
    validate_driver_model_rule(&document.defaults, "defaults")?;

    let mut variant_names = HashSet::new();
    let mut variant_scopes = HashSet::new();
    for (index, variant) in document.variants.iter().enumerate() {
        validate_trimmed_value(
            variant.name.as_str(),
            format!("variants[{}].name", index).as_str(),
        )?;
        if !variant_names.insert(variant.name.to_ascii_lowercase()) {
            return Err(format!("duplicate variant name '{}'", variant.name));
        }
        let suffix = variant
            .mount_suffix
            .as_deref()
            .ok_or_else(|| format!("variants[{}].mount_suffix is required", index))?;
        if !is_valid_variant_suffix(suffix) || suffix.trim() != suffix || suffix.is_empty() {
            return Err(format!("variants[{}].mount_suffix is invalid", index));
        }
        let scope = (
            suffix.to_ascii_lowercase(),
            variant
                .model_pattern
                .as_deref()
                .unwrap_or("*")
                .to_ascii_lowercase(),
        );
        if !variant_scopes.insert(scope) {
            return Err(format!(
                "duplicate variant mount_suffix '{}' for the same model pattern",
                suffix
            ));
        }
    }

    for (index, rule) in document.version_rules.iter().enumerate() {
        validate_trimmed_value(
            rule.family.as_str(),
            format!("version_rules[{}].family", index).as_str(),
        )?;
        validate_optional_trimmed_value(
            rule.tier.as_deref(),
            format!("version_rules[{}].tier", index).as_str(),
        )?;
        validate_optional_trimmed_value(
            rule.model_pattern.as_deref(),
            format!("version_rules[{}].model_pattern", index).as_str(),
        )?;
        validate_optional_trimmed_value(
            rule.current_mount.as_deref(),
            format!("version_rules[{}].current_mount", index).as_str(),
        )?;
        validate_optional_trimmed_value(
            rule.version_mount.as_deref(),
            format!("version_rules[{}].version_mount", index).as_str(),
        )?;
        validate_string_list(
            rule.tier_tokens.as_slice(),
            format!("version_rules[{}].tier_tokens", index).as_str(),
        )?;
        validate_string_list(
            rule.tier_patterns.as_slice(),
            format!("version_rules[{}].tier_patterns", index).as_str(),
        )?;
        validate_string_list(
            rule.exclude_tier_tokens.as_slice(),
            format!("version_rules[{}].exclude_tier_tokens", index).as_str(),
        )?;
        validate_string_list(
            rule.exclude_patterns.as_slice(),
            format!("version_rules[{}].exclude_patterns", index).as_str(),
        )?;
        validate_string_list(
            rule.auto_mounts.as_slice(),
            format!("version_rules[{}].auto_mounts", index).as_str(),
        )?;
        validate_capabilities_patch(
            &rule.capabilities,
            format!("version_rules[{}]", index).as_str(),
        )?;
    }
    Ok(())
}

fn validate_driver_model_rule(rule: &DriverModelRule, location: &str) -> Result<(), String> {
    validate_optional_trimmed_value(
        rule.model_driver.as_deref(),
        &format!("{}.model_driver", location),
    )?;
    validate_optional_trimmed_value(
        rule.parameter_scale.as_deref(),
        &format!("{}.parameter_scale", location),
    )?;
    if rule.api_types.as_ref().is_some_and(Vec::is_empty) {
        return Err(format!("{}.api_types cannot be empty", location));
    }
    if let Some(mounts) = rule.logical_mounts.as_ref() {
        validate_string_list(mounts.as_slice(), &format!("{}.logical_mounts", location))?;
    }
    if let Some(pricing) = rule.pricing.as_ref() {
        let pricing_location = format!("{}.pricing", location);
        if pricing.currency.trim().is_empty() {
            return Err(format!("{}.currency cannot be empty", pricing_location));
        }
        validate_non_negative_number(
            pricing.estimated_cost,
            "estimated_cost",
            pricing_location.as_str(),
        )?;
        validate_non_negative_number(
            pricing.input_token,
            "input_token",
            pricing_location.as_str(),
        )?;
        validate_non_negative_number(
            pricing.output_token,
            "output_token",
            pricing_location.as_str(),
        )?;
        validate_non_negative_number(
            pricing.cache_input_token,
            "cache_input_token",
            pricing_location.as_str(),
        )?;
    }
    if rule
        .quality_score
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err(format!("{}.quality_score is invalid", location));
    }
    validate_capabilities_patch(&rule.capabilities, location)
}

fn validate_non_negative_number(
    value: Option<f64>,
    field: &str,
    location: &str,
) -> Result<(), String> {
    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(format!("{}.{} is invalid", location, field));
    }
    Ok(())
}

fn validate_capabilities_patch(
    capabilities: &DriverCapabilitiesPatch,
    location: &str,
) -> Result<(), String> {
    if capabilities.max_context_tokens == Some(0) || capabilities.max_output_tokens == Some(0) {
        return Err(format!(
            "{}.capability token limits must be positive",
            location
        ));
    }
    if let Some(combinations) = capabilities.unsupported_feature_combinations.as_ref() {
        let mut unique_combinations = HashSet::new();
        for (index, combination) in combinations.iter().enumerate() {
            let combination_location = format!(
                "{}.capabilities.unsupported_feature_combinations[{}]",
                location, index
            );
            if combination.len() < 2 {
                return Err(format!(
                    "{} must contain at least two features",
                    combination_location
                ));
            }
            validate_string_list(combination, combination_location.as_str())?;
            if let Some(feature) = combination
                .iter()
                .find(|feature| !is_model_feature_name(feature))
            {
                return Err(format!(
                    "{} contains unsupported feature '{}'",
                    combination_location, feature
                ));
            }
            let mut canonical = combination.clone();
            canonical.sort();
            if !unique_combinations.insert(canonical.join("\0")) {
                return Err(format!(
                    "{}.capabilities.unsupported_feature_combinations contains duplicate combination",
                    location
                ));
            }
        }
    }
    Ok(())
}

fn validate_optional_trimmed_value(value: Option<&str>, location: &str) -> Result<(), String> {
    if let Some(value) = value {
        validate_trimmed_value(value, location)?;
    }
    Ok(())
}

fn validate_trimmed_value(value: &str, location: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!("{} must be a non-empty trimmed string", location));
    }
    Ok(())
}

fn validate_string_list(values: &[String], location: &str) -> Result<(), String> {
    let mut unique = HashSet::new();
    for value in values {
        validate_trimmed_value(value, location)?;
        if !unique.insert(value.to_ascii_lowercase()) {
            return Err(format!("{} contains duplicate value '{}'", location, value));
        }
    }
    Ok(())
}

fn resolve_origin_identity(
    model_driver_id: &str,
    provider_model_id: &str,
    _sources: &[DriverMetadataSource],
) -> DriverOriginIdentity {
    DriverOriginIdentity {
        driver: model_driver_id.to_string(),
        model: provider_model_id.to_string(),
    }
}

fn load_builtin_driver_metadata(model_driver_id: &str) -> Option<DriverMetadataDocument> {
    let normalized = normalize_driver(model_driver_id);
    let raw = match normalized.as_str() {
        "openai" => include_str!("../driver_metadata/openai.json"),
        "sn-ai-provider" => include_str!("../driver_metadata/openai.json"),
        "claude" | "anthropic" => include_str!("../driver_metadata/claude.json"),
        "google-gemini" | "gemini" => include_str!("../driver_metadata/gemini.json"),
        "fal" => include_str!("../driver_metadata/fal.json"),
        "minimax" => include_str!("../driver_metadata/minimax.json"),
        _ => return None,
    };
    parse_and_validate_driver_metadata(raw, None)
        .map_err(|err| {
            warn!(
                "aicc.metadata_resolver.invalid_builtin model_driver_id={} err={}",
                model_driver_id, err
            );
            err
        })
        .ok()
}

fn driver_metadata_override_paths(model_driver_id: &str) -> Vec<(String, PathBuf)> {
    let etc = get_buckyos_system_etc_dir()
        .join("aicc")
        .join("driver_metadata");
    let driver = normalize_driver(model_driver_id);
    vec![
        (
            "local_override".to_string(),
            etc.join("local").join(format!("{}.json", driver)),
        ),
        (
            "system_config_override".to_string(),
            etc.join("system-config").join(format!("{}.json", driver)),
        ),
    ]
}

fn find_exact_rule<'a>(
    provider_model_id: &str,
    sources: &'a [DriverMetadataSource],
) -> Option<&'a DriverModelRule> {
    let key = provider_model_id.to_ascii_lowercase();
    for source in sources.iter().rev() {
        if source.document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
            warn!(
                "aicc.metadata_resolver.skip_schema_version source={} schema_version={}",
                source.name, source.document.schema_version
            );
            continue;
        }
        if let Some(index) = source.exact_model_index.get(key.as_str()) {
            return source.document.models.get(*index);
        }
    }
    None
}

fn find_pattern_rule<'a>(
    provider_model_id: &str,
    sources: &'a [DriverMetadataSource],
) -> Option<&'a DriverModelRule> {
    for source in sources.iter().rev() {
        if source.document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
            continue;
        }
        for rule in source.document.patterns.iter() {
            if rule
                .pattern
                .as_deref()
                .map(|pattern| wildcard_matches(pattern, provider_model_id))
                .unwrap_or(false)
            {
                return Some(rule);
            }
        }
    }
    None
}

fn find_default_rule(sources: &[DriverMetadataSource]) -> Option<&DriverModelRule> {
    for source in sources.iter().rev() {
        if source.document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
            continue;
        }
        if source.document.defaults.api_types.is_some()
            || source.document.defaults.logical_mounts.is_some()
            || source.document.defaults.capabilities.has_any()
            || source.document.defaults.pricing.is_some()
            || source.document.defaults.estimated_latency_ms.is_some()
        {
            return Some(&source.document.defaults);
        }
    }
    None
}

fn driver_variants(sources: &[DriverMetadataSource]) -> &[DriverModelVariant] {
    for source in sources.iter().rev() {
        if source.document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
            continue;
        }
        if !source.document.variants.is_empty() {
            return source.document.variants.as_slice();
        }
    }
    &[]
}

fn expand_model_variants(
    model: ModelMetadata,
    sources: &[DriverMetadataSource],
) -> Vec<ModelMetadata> {
    let variants = driver_variants(sources);
    if variants.is_empty() || !model_supports_variants(&model) {
        return vec![model];
    }

    let mut models = vec![model.clone()];
    for variant in variants.iter() {
        if variant
            .model_pattern
            .as_deref()
            .is_some_and(|pattern| !wildcard_matches(pattern, model.provider_model_id.as_str()))
        {
            continue;
        }
        let Some(suffix) = variant.mount_suffix.as_deref() else {
            continue;
        };
        let suffix = suffix.trim();
        if suffix.is_empty() || !is_valid_variant_suffix(suffix) {
            continue;
        }

        let variant_provider_model_id = format!("{}:{}", model.provider_model_id, suffix);
        let mut variant_model = model.clone();
        variant_model.provider_model_id = variant_provider_model_id.clone();
        let Ok(exact_name) = model.exact_name() else {
            continue;
        };
        variant_model.exact_model = exact_model_name(
            variant_provider_model_id.as_str(),
            exact_name.provider_instance_name.as_str(),
        );
        variant_model.provider_actual_model_id = Some(model.provider_model_id.clone());
        variant_model.provider_options = None;
        variant_model.logical_mounts =
            variant_logical_mounts(model.logical_mounts.as_slice(), suffix);
        models.push(variant_model);
    }
    models
}

fn model_supports_variants(model: &ModelMetadata) -> bool {
    model
        .api_types
        .iter()
        .any(|api_type| matches!(api_type, ApiType::Llm))
        && (model.capabilities.streaming
            || model.capabilities.tool_call
            || model.capabilities.json_schema
            || model.capabilities.web_search
            || model.capabilities.vision
            || model.capabilities.max_context_tokens.is_some()
            || model.capabilities.max_output_tokens.is_some())
}

fn is_valid_variant_suffix(value: &str) -> bool {
    value
        .bytes()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'-' | b'_'))
}

fn variant_logical_mounts(base_mounts: &[String], suffix: &str) -> Vec<String> {
    dedupe_strings(
        base_mounts
            .iter()
            .map(|mount| format!("{}.{}", mount, suffix))
            .collect(),
    )
}

pub(crate) fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let value = value.to_ascii_lowercase();
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index) = (0usize, 0usize);
    let (mut star_index, mut star_value_index) = (None, 0usize);

    while value_index < value.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == value[value_index] {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            star_value_index += 1;
            value_index = star_value_index;
            pattern_index = star + 1;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn conservative_capabilities() -> ModelCapabilities {
    ModelCapabilities {
        streaming: false,
        tool_call: false,
        json_schema: false,
        web_search: false,
        unsupported_feature_combinations: vec![],
        vision: false,
        image_generation: false,
        max_context_tokens: None,
        max_output_tokens: None,
    }
}

impl DriverCapabilitiesPatch {
    fn has_any(&self) -> bool {
        self.streaming.is_some()
            || self.tool_call.is_some()
            || self.json_schema.is_some()
            || self.web_search.is_some()
            || self.unsupported_feature_combinations.is_some()
            || self.vision.is_some()
            || self.image_generation.is_some()
            || self.max_context_tokens.is_some()
            || self.max_output_tokens.is_some()
    }
}

fn apply_capabilities_patch(capabilities: &mut ModelCapabilities, patch: &DriverCapabilitiesPatch) {
    if let Some(value) = patch.streaming {
        capabilities.streaming = value;
    }
    if let Some(value) = patch.tool_call {
        capabilities.tool_call = value;
    }
    if let Some(value) = patch.json_schema {
        capabilities.json_schema = value;
    }
    if let Some(value) = patch.web_search {
        capabilities.web_search = value;
    }
    if let Some(value) = patch.unsupported_feature_combinations.as_ref() {
        capabilities.unsupported_feature_combinations = value.clone();
    }
    if let Some(value) = patch.vision {
        capabilities.vision = value;
    }
    if let Some(value) = patch.image_generation {
        capabilities.image_generation = value;
    }
    if patch.max_context_tokens.is_some() {
        capabilities.max_context_tokens = patch.max_context_tokens;
    }
    if patch.max_output_tokens.is_some() {
        capabilities.max_output_tokens = patch.max_output_tokens;
    }
}

fn generic_mounts(origin: &DriverOriginIdentity, api_types: &[ApiType]) -> Vec<String> {
    let mut mounts = Vec::new();
    for api_type in api_types.iter() {
        let base = api_mount_base(api_type);
        add_unique(&mut mounts, base.to_string());
        add_unique(
            &mut mounts,
            format!("{}.{}", base, logical_mount_segment(origin.driver.as_str())),
        );
        add_unique(
            &mut mounts,
            format!("{}.{}", base, logical_mount_segment(origin.model.as_str())),
        );
        if matches!(api_type, ApiType::Llm) {
            add_unique(&mut mounts, "llm".to_string());
            add_unique(
                &mut mounts,
                format!("llm.{}", logical_mount_segment(origin.driver.as_str())),
            );
        }
    }
    mounts
}

pub(crate) fn semantic_llm_family_mounts(provider_model_id: &str) -> Vec<String> {
    let normalized = logical_mount_segment(provider_model_id);
    let mut mounts = Vec::new();

    if normalized.contains("qwen") {
        if normalized.contains("coder") {
            add_unique(&mut mounts, "llm.qwen-coder".to_string());
        } else if normalized.contains("max") {
            add_unique(&mut mounts, "llm.qwen-max".to_string());
        } else if normalized.contains("small")
            || normalized.contains("mini")
            || normalized.contains("flash")
            || normalized.contains("turbo")
        {
            add_unique(&mut mounts, "llm.qwen-small".to_string());
        }
    }

    if normalized.contains("deepseek") {
        if normalized.contains("reasoner") || normalized.contains("r1") {
            add_unique(&mut mounts, "llm.deepseek-reasoner".to_string());
        } else if normalized.contains("pro")
            || normalized.contains("chat")
            || normalized.contains("v3")
        {
            add_unique(&mut mounts, "llm.deepseek-pro".to_string());
        }
    }

    if normalized.contains("kimi") || normalized.contains("moonshot") {
        if normalized.contains("thinking")
            || normalized.contains("think")
            || normalized.contains("k1")
        {
            add_unique(&mut mounts, "llm.kimi-thinking".to_string());
        } else {
            add_unique(&mut mounts, "llm.kimi".to_string());
        }
    }

    if normalized.contains("glm") {
        if normalized.contains("flash") || normalized.contains("air") {
            add_unique(&mut mounts, "llm.glm-flash".to_string());
        } else {
            add_unique(&mut mounts, "llm.glm".to_string());
        }
    }

    if normalized.contains("grok") {
        if normalized.contains("fast")
            || normalized.contains("mini")
            || normalized.contains("small")
        {
            add_unique(&mut mounts, "llm.grok-fast".to_string());
        } else if normalized.contains("heavy")
            || normalized.contains("reason")
            || normalized.contains("think")
        {
            add_unique(&mut mounts, "llm.grok-heavy".to_string());
        }
    }

    mounts
}

fn api_mount_base(api_type: &ApiType) -> &'static str {
    match api_type {
        ApiType::Llm => "llm",
        ApiType::Embedding => "embedding.text",
        ApiType::EmbeddingMultimodal => "embedding.multimodal",
        ApiType::Rerank => "rerank",
        ApiType::ImageTextToImage => "image.txt2img",
        ApiType::ImageToImage => "image.img2img",
        ApiType::ImageInpaint => "image.inpaint",
        ApiType::ImageUpscale => "image.upscale",
        ApiType::ImageBgRemove => "image.bg_remove",
        ApiType::VisionOcr => "vision.ocr",
        ApiType::VisionCaption => "vision.caption",
        ApiType::VisionDetect => "vision.detect",
        ApiType::VisionSegment => "vision.segment",
        ApiType::AudioTts => "audio.tts",
        ApiType::AudioAsr => "audio.asr",
        ApiType::AudioMusic => "audio.music",
        ApiType::AudioEnhance => "audio.enhance",
        ApiType::VideoTextToVideo => "video.txt2video",
        ApiType::VideoImageToVideo => "video.img2video",
        ApiType::VideoToVideo => "video.video2video",
        ApiType::VideoExtend => "video.extend",
        ApiType::VideoUpscale => "video.upscale",
        ApiType::AgentComputerUse => "agent.computer_use",
    }
}

fn provider_fallback_mounts(mounts: &[String]) -> Vec<String> {
    mounts
        .iter()
        .filter(|mount| !is_task_role_mount(mount.as_str()))
        .cloned()
        .collect()
}

fn is_task_role_mount(mount: &str) -> bool {
    const ROLE_MOUNTS: &[&str] = &[
        "llm",
        "llm.plan",
        "llm.code",
        "llm.reason",
        "llm.summarize",
        "llm.swift",
        "llm.vision",
        "llm.long",
        "llm.fallback",
    ];
    ROLE_MOUNTS.iter().any(|role| {
        mount == *role
            || (*role != "llm"
                && mount
                    .strip_prefix(*role)
                    .is_some_and(|tail| tail.starts_with('.')))
    })
}

fn expand_mount_template(
    template: &str,
    model_driver_id: &str,
    provider_model_id: &str,
    origin: &DriverOriginIdentity,
) -> String {
    template
        .replace(
            "{model_driver_id}",
            logical_mount_segment(model_driver_id).as_str(),
        )
        .replace(
            "{provider_model_id}",
            logical_mount_segment(provider_model_id).as_str(),
        )
        .replace(
            "{driver}",
            logical_mount_segment(origin.driver.as_str()).as_str(),
        )
        .replace(
            "{model}",
            logical_mount_segment(origin.model.as_str()).as_str(),
        )
}

fn logical_mount_segment(value: &str) -> String {
    let normalized = value
        .trim()
        .trim_start_matches('/')
        .replace('/', "-")
        .replace('_', "-")
        .replace('.', "-")
        .to_ascii_lowercase();
    normalized
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn normalize_driver(model_driver_id: &str) -> String {
    model_driver_id
        .trim()
        .replace('_', "-")
        .to_ascii_lowercase()
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::<String>::new();
    let mut result = Vec::new();
    for value in values.into_iter() {
        if !value.is_empty() && seen.insert(value.clone()) {
            result.push(value);
        }
    }
    result
}

fn add_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|item| item == &value) {
        values.push(value);
    }
}

fn apply_driver_post_rules(
    model_driver_id: &str,
    models: &mut [ModelMetadata],
    sources: &[DriverMetadataSource],
) {
    for rule in driver_version_rules(sources) {
        apply_driver_version_rule(model_driver_id, models, rule, sources);
    }
}

fn driver_version_rules(sources: &[DriverMetadataSource]) -> &[DriverVersionRule] {
    for source in sources.iter().rev() {
        if source.document.schema_version != DRIVER_METADATA_SCHEMA_VERSION {
            continue;
        }
        if !source.document.version_rules.is_empty() {
            return source.document.version_rules.as_slice();
        }
    }
    &[]
}

fn apply_driver_version_rule(
    model_driver_id: &str,
    models: &mut [ModelMetadata],
    rule: &DriverVersionRule,
    sources: &[DriverMetadataSource],
) {
    use std::cmp::Ordering;
    let mut latest: Option<(usize, DriverModelRank)> = None;
    for (index, model) in models.iter_mut().enumerate() {
        if !model
            .api_types
            .iter()
            .any(|api_type| matches!(api_type, ApiType::Llm))
        {
            continue;
        }
        let origin =
            resolve_origin_identity(model_driver_id, model.provider_model_id.as_str(), sources);
        let Some(rank) = rank_model_for_version_rule(model.provider_model_id.as_str(), rule) else {
            continue;
        };
        remove_driver_auto_mounts(&mut model.logical_mounts, rule);
        if let Some(version_mount) = rule.version_mount.as_deref() {
            add_unique(
                &mut model.logical_mounts,
                expand_mount_template(
                    version_mount,
                    model_driver_id,
                    model.provider_model_id.as_str(),
                    &origin,
                ),
            );
        }
        if rule.stability.current_requires_stable && !rank.stable {
            continue;
        }
        let replace = latest
            .as_ref()
            .map(|entry| compare_gpt_rank(&rank, &entry.1) == Ordering::Greater)
            .unwrap_or(true);
        if replace {
            latest = Some((index, rank));
        }
    }

    if let Some((index, _)) = latest {
        let model = &mut models[index];
        let origin =
            resolve_origin_identity(model_driver_id, model.provider_model_id.as_str(), sources);
        if let Some(current_mount) = rule.current_mount.as_deref() {
            add_unique(
                &mut model.logical_mounts,
                expand_mount_template(
                    current_mount,
                    model_driver_id,
                    model.provider_model_id.as_str(),
                    &origin,
                ),
            );
        }
        apply_capabilities_patch(&mut model.capabilities, &rule.capabilities);
    }
}

#[derive(Clone, Debug)]
struct DriverModelRank {
    version: Vec<u64>,
    stable: bool,
    model_id: String,
}

fn rank_model_for_version_rule(
    provider_model_id: &str,
    rule: &DriverVersionRule,
) -> Option<DriverModelRank> {
    let normalized = provider_model_id
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-");
    if rule
        .model_pattern
        .as_deref()
        .is_some_and(|pattern| !wildcard_matches(pattern, normalized.as_str()))
    {
        return None;
    }
    if rule.model_pattern.is_none()
        && !rule.family.is_empty()
        && !normalized.contains(rule.family.to_ascii_lowercase().as_str())
    {
        return None;
    }
    if rule.exclude_snapshot_date_suffix && has_snapshot_date_suffix(normalized.as_str()) {
        return None;
    }

    let tokens = normalized
        .split(|ch: char| ch == '-' || ch == '.' || ch == '/')
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
        .collect::<HashSet<_>>();
    let tier_token_matches = rule
        .tier_tokens
        .iter()
        .map(|token| token.to_ascii_lowercase())
        .any(|token| tokens.contains(token.as_str()));
    let tier_pattern_matches = rule
        .tier_patterns
        .iter()
        .any(|pattern| wildcard_matches(pattern, normalized.as_str()));
    if (!rule.tier_tokens.is_empty() || !rule.tier_patterns.is_empty())
        && !tier_token_matches
        && !tier_pattern_matches
    {
        return None;
    }
    if rule
        .exclude_tier_tokens
        .iter()
        .map(|token| token.to_ascii_lowercase())
        .any(|token| tokens.contains(token.as_str()))
    {
        return None;
    }
    if rule
        .exclude_patterns
        .iter()
        .any(|pattern| wildcard_matches(pattern, normalized.as_str()))
    {
        return None;
    }
    let stable = !rule
        .stability
        .unstable_tokens
        .iter()
        .map(|token| token.to_ascii_lowercase())
        .any(|token| tokens.contains(token.as_str()));
    Some(DriverModelRank {
        version: parse_driver_version(normalized.as_str(), rule.version_rank.prefix.as_deref()),
        stable,
        model_id: normalized,
    })
}

fn has_snapshot_date_suffix(normalized_model_id: &str) -> bool {
    let mut parts = normalized_model_id.rsplitn(4, '-');
    let Some(day) = parts.next() else {
        return false;
    };
    let Some(month) = parts.next() else {
        return false;
    };
    let Some(year) = parts.next() else {
        return false;
    };
    let Some(prefix) = parts.next() else {
        return false;
    };
    !prefix.is_empty()
        && year.len() == 4
        && month.len() == 2
        && day.len() == 2
        && year.chars().all(|ch| ch.is_ascii_digit())
        && month.chars().all(|ch| ch.is_ascii_digit())
        && day.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_driver_version(normalized_model_id: &str, prefix: Option<&str>) -> Vec<u64> {
    let offset = prefix
        .and_then(|prefix| {
            normalized_model_id
                .find(prefix)
                .map(|pos| pos + prefix.len())
        })
        .unwrap_or(0);
    let mut chars = normalized_model_id[offset..]
        .trim_start_matches('-')
        .chars()
        .peekable();
    let mut version = Vec::new();
    loop {
        let mut value = String::new();
        while let Some(ch) = chars.peek().copied() {
            if ch.is_ascii_digit() {
                value.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        if value.is_empty() {
            break;
        }
        if let Ok(parsed) = value.parse::<u64>() {
            version.push(parsed);
        }
        if chars.peek().copied() == Some('.') {
            chars.next();
            continue;
        }
        break;
    }
    version
}

fn compare_gpt_rank(left: &DriverModelRank, right: &DriverModelRank) -> std::cmp::Ordering {
    let max_len = left.version.len().max(right.version.len());
    for index in 0..max_len {
        let left_value = left.version.get(index).copied().unwrap_or(0);
        let right_value = right.version.get(index).copied().unwrap_or(0);
        match left_value.cmp(&right_value) {
            std::cmp::Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    left.stable
        .cmp(&right.stable)
        .then_with(|| left.model_id.cmp(&right.model_id))
}

fn remove_driver_auto_mounts(mounts: &mut Vec<String>, rule: &DriverVersionRule) {
    mounts.retain(|mount| {
        !rule
            .auto_mounts
            .iter()
            .any(|auto_mount| mount == auto_mount)
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_matching_supports_internal_and_multiple_stars() {
        assert!(wildcard_matches("gemini-*-latest", "gemini-pro-latest"));
        assert!(wildcard_matches("gemini-3*flash*", "gemini-3.5-flash-lite"));
        assert!(wildcard_matches("openai/*:*", "openai/gpt-5:fast"));
        assert!(!wildcard_matches("gemini-3*pro*", "gemini-3.5-flash"));
        assert!(!wildcard_matches("gemini-*-latest", "gemini-pro-preview"));
    }

    #[test]
    fn builtin_driver_metadata_passes_semantic_validation() {
        for driver in [
            "openai",
            "sn-ai-provider",
            "claude",
            "google-gemini",
            "fal",
            "minimax",
        ] {
            let document = load_builtin_driver_metadata(driver).unwrap();
            validate_driver_metadata_document(&document)
                .unwrap_or_else(|err| panic!("{} metadata is invalid: {}", driver, err));
        }
    }

    #[test]
    fn sn_metadata_reuses_openai_and_maximum_unknown_cost() {
        let document = load_builtin_driver_metadata("sn-ai-provider").expect("sn metadata");
        assert_eq!(document.model_driver_id, "openai");
        assert_eq!(
            driver_metadata_model_ids("sn-ai-provider", &ApiType::Llm),
            driver_metadata_model_ids("openai", &ApiType::Llm)
        );
        assert!(driver_model_has_specific_metadata(
            "sn-ai-provider",
            "gpt-5.4"
        ));
        assert!(!driver_model_has_specific_metadata(
            "sn-ai-provider",
            "vendor-new-model"
        ));
        assert_eq!(
            max_driver_metadata_cost("sn-ai-provider", &ApiType::Llm, 1_000, 1_000),
            max_driver_metadata_cost("openai", &ApiType::Llm, 1_000, 1_000)
        );
    }

    #[test]
    fn capability_combination_validation_is_generic_and_fail_closed() {
        let mut patch = DriverCapabilitiesPatch {
            unsupported_feature_combinations: Some(vec![vec![
                "web_search".to_string(),
                "tool_calling".to_string(),
            ]]),
            ..Default::default()
        };
        assert!(validate_capabilities_patch(&patch, "models[0]").is_ok());

        patch.unsupported_feature_combinations = Some(vec![vec!["web_search".to_string()]]);
        assert!(validate_capabilities_patch(&patch, "models[0]").is_err());

        patch.unsupported_feature_combinations = Some(vec![vec![
            "web_search".to_string(),
            "future_feature".to_string(),
        ]]);
        assert!(validate_capabilities_patch(&patch, "models[0]").is_err());

        patch.unsupported_feature_combinations = Some(vec![
            vec!["web_search".to_string(), "tool_calling".to_string()],
            vec!["tool_calling".to_string(), "web_search".to_string()],
        ]);
        assert!(validate_capabilities_patch(&patch, "models[0]").is_err());
    }

    #[test]
    fn override_metadata_rejects_schema_and_provider_identity_mismatch() {
        let valid = serde_json::json!({
            "format": "buckyos.aicc.provider-driver-metadata",
            "schema_version": DRIVER_METADATA_SCHEMA_VERSION,
            "schema_revision": 0,
            "model_driver_id": "openai",
            "revision_seq": 1,
            "required_features": [],
            "models": [],
            "patterns": [],
            "defaults": {},
            "variants": [],
            "version_rules": []
        });
        assert!(parse_and_validate_driver_metadata(&valid.to_string(), Some("openai")).is_ok());

        let mut removed_capability_overrides = valid.clone();
        removed_capability_overrides["capability_overrides"] = serde_json::json!([]);
        assert!(parse_and_validate_driver_metadata(
            &removed_capability_overrides.to_string(),
            Some("openai")
        )
        .is_err());

        let mut wrong_schema = valid.clone();
        wrong_schema["schema_version"] = serde_json::json!(1);
        assert!(
            parse_and_validate_driver_metadata(&wrong_schema.to_string(), Some("openai")).is_err()
        );

        let mut unsupported_features = valid.clone();
        unsupported_features["required_features"] = serde_json::json!(["future-semantics"]);
        assert!(parse_and_validate_driver_metadata(
            &unsupported_features.to_string(),
            Some("openai")
        )
        .is_err());

        assert!(parse_and_validate_driver_metadata(&valid.to_string(), Some("claude")).is_err());
    }

    #[test]
    fn driver_metadata_rejects_provider_origin_mapping_fields() {
        let document = serde_json::json!({
            "format": "buckyos.aicc.provider-driver-metadata",
            "schema_version": DRIVER_METADATA_SCHEMA_VERSION,
            "model_driver_id": "openai",
            "revision_seq": 1,
            "origin_provider_aliases": { "vendor": "openai" },
            "origin_mappings": []
        });
        assert!(parse_and_validate_driver_metadata(&document.to_string(), Some("openai")).is_err());
    }

    #[test]
    fn mount_templates_keep_origin_and_channel_names_separate() {
        let origin = DriverOriginIdentity {
            driver: "openai".to_string(),
            model: "gpt-5.5".to_string(),
        };
        assert_eq!(
            expand_mount_template(
                "llm.{driver}.{model}.{model_driver_id}.{provider_model_id}",
                "openrouter",
                "openai/gpt-5.5",
                &origin,
            ),
            "llm.openai.gpt-5-5.openrouter.openai-gpt-5-5"
        );
    }

    #[test]
    fn openai_unknown_fallback_is_conservative() {
        let request = DriverModelResolveRequest::new("future-model", vec![]);
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[request],
            None,
        );
        assert_eq!(inventory.models.len(), 1);
        let model = &inventory.models[0];
        assert_eq!(model.api_types, vec![ApiType::Llm]);
        assert!(!model.capabilities.tool_call);
        assert!(!model.capabilities.web_search);
        assert!(!model.capabilities.vision);
        assert!(!model.capabilities.json_schema);
    }

    #[test]
    fn known_driver_ignores_provider_fallback_mounts() {
        let mut request = DriverModelResolveRequest::new("MiniMax-M2.5", vec![ApiType::Llm]);
        request.fallback_logical_mounts = vec![
            "llm.plan".to_string(),
            "llm.provider-hint".to_string(),
            "llm.minimax-provider-hint".to_string(),
        ];
        let inventory = resolve_driver_inventory(
            "minimax-test",
            ProviderType::CloudApi,
            "minimax",
            &[request],
            None,
        );
        let model = &inventory.models[0];
        assert!(model
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.minimax"));
        assert!(!model.logical_mounts.iter().any(|mount| mount == "llm.plan"));
        assert!(!model
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.provider-hint"));
        assert!(!model
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.minimax-provider-hint"));
    }

    #[test]
    fn unknown_driver_fallback_mounts_drop_role_paths() {
        let mut request = DriverModelResolveRequest::new("future-model", vec![ApiType::Llm]);
        request.fallback_logical_mounts = vec![
            "llm.plan".to_string(),
            "llm".to_string(),
            "llm.future-family".to_string(),
        ];
        let inventory = resolve_driver_inventory(
            "future-test",
            ProviderType::CloudApi,
            "future-driver",
            &[request],
            None,
        );
        let model = &inventory.models[0];
        assert_eq!(model.logical_mounts, vec!["llm.future-family".to_string()]);
    }

    #[test]
    fn exact_model_wins_before_pattern() {
        let request = DriverModelResolveRequest::new("gpt-image-1", vec![]);
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[request],
            None,
        );
        let model = &inventory.models[0];
        assert!(model.api_types.contains(&ApiType::ImageTextToImage));
        assert!(model.api_types.contains(&ApiType::ImageToImage));
        assert!(!model.api_types.contains(&ApiType::Llm));
    }

    #[test]
    fn openai_gpt_image_pattern_stays_on_image_api() {
        let request = DriverModelResolveRequest::new("gpt-image-2", vec![]);
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[request],
            None,
        );
        let model = &inventory.models[0];
        assert!(model.api_types.contains(&ApiType::ImageTextToImage));
        assert!(model.api_types.contains(&ApiType::ImageToImage));
        assert!(model.api_types.contains(&ApiType::ImageInpaint));
        assert!(!model.api_types.contains(&ApiType::Llm));
    }

    #[test]
    fn openai_latest_gpt_mounts_family_only() {
        let requests = vec![
            DriverModelResolveRequest::new("gpt-5.4", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.5", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.5-pro", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.4-mini", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.4-nano", vec![ApiType::Llm]),
        ];
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            requests.as_slice(),
            None,
        );
        let by_id = |id: &str| {
            inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("model should exist")
        };
        let gpt = by_id("gpt-5.5");
        assert!(gpt
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-standard"));
        assert!(gpt
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.openai.gpt-5-5"));
        assert!(!gpt.logical_mounts.iter().any(|mount| mount == "llm"));
        assert!(!gpt.logical_mounts.iter().any(|mount| mount == "llm.code"));
        assert!(!gpt.logical_mounts.iter().any(|mount| mount == "llm.plan"));

        let pro = by_id("gpt-5.5-pro");
        assert!(pro
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-pro"));
        assert!(!pro.logical_mounts.iter().any(|mount| mount == "llm.plan"));
        assert!(!pro.logical_mounts.iter().any(|mount| mount == "llm.reason"));

        let mini = by_id("gpt-5.4-mini");
        assert!(mini
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-mini"));
        assert!(!mini
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.summarize"));

        let nano = by_id("gpt-5.4-nano");
        assert!(nano
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-nano"));
        assert!(!nano.logical_mounts.iter().any(|mount| mount == "llm.swift"));
    }

    #[test]
    fn openai_version_rule_prefers_stable_current_mounts() {
        let requests = vec![
            DriverModelResolveRequest::new("gpt-5.4", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.5-preview", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.6-beta", vec![ApiType::Llm]),
        ];
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            requests.as_slice(),
            None,
        );
        let by_id = |id: &str| {
            inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("model should exist")
        };
        assert!(by_id("gpt-5.4")
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-standard"));
        for id in ["gpt-5.5-preview", "gpt-5.6-beta"] {
            let model = by_id(id);
            assert!(!model
                .logical_mounts
                .iter()
                .any(|mount| mount == "llm.gpt-standard"));
            assert!(model
                .logical_mounts
                .iter()
                .any(|mount| mount.starts_with("llm.openai.gpt-")));
        }
    }

    #[test]
    fn openai_gpt5_image_generation_is_metadata_driven() {
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[
                DriverModelResolveRequest::new("gpt-5.6-sol", vec![ApiType::Llm]),
                DriverModelResolveRequest::new("gpt-5.4", vec![ApiType::Llm]),
                DriverModelResolveRequest::new(
                    "gpt-image-2",
                    vec![ApiType::ImageTextToImage, ApiType::ImageToImage],
                ),
            ],
            None,
        );
        for id in ["gpt-5.6-sol", "gpt-5.4"] {
            let model = inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("GPT-5 model");
            assert!(model.capabilities.image_generation);
            assert!(model.api_types.contains(&ApiType::ImageTextToImage));
            assert!(model.api_types.contains(&ApiType::ImageToImage));
            assert!(!model.api_types.contains(&ApiType::ImageInpaint));
        }
        let image_model = inventory
            .models
            .iter()
            .find(|model| model.provider_model_id == "gpt-image-2")
            .expect("GPT Image model");
        assert!(!image_model.capabilities.image_generation);
    }

    #[test]
    fn openai_variants_expand_after_current_mount_selection() {
        let requests = [
            DriverModelResolveRequest::new("gpt-5.5", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.2-pro", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.4-pro", vec![ApiType::Llm]),
            DriverModelResolveRequest::new("gpt-5.5-pro", vec![ApiType::Llm]),
        ];
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &requests,
            None,
        );
        let variant = inventory
            .models
            .iter()
            .find(|model| model.provider_model_id == "gpt-5.5:reasoning-high")
            .expect("reasoning variant should exist");
        assert_eq!(variant.exact_model, "gpt-5.5:reasoning-high@openai-test");
        assert_eq!(variant.provider_actual_model_id.as_deref(), Some("gpt-5.5"));
        assert!(variant
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gpt-standard.reasoning-high"));
        for id in ["gpt-5.2-pro", "gpt-5.4-pro", "gpt-5.5-pro"] {
            assert!(inventory
                .models
                .iter()
                .any(|model| model.provider_model_id == format!("{id}:reasoning-high")));
            assert!(!inventory
                .models
                .iter()
                .any(|model| model.provider_model_id == format!("{id}:reasoning-low")));
        }
        for id in ["gpt-5.2-pro", "gpt-5.4-pro"] {
            let model = inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("pro model");
            assert!(!model.capabilities.json_schema);
        }
        let gpt_55_pro = inventory
            .models
            .iter()
            .find(|model| model.provider_model_id == "gpt-5.5-pro")
            .expect("GPT-5.5 Pro model");
        assert!(!gpt_55_pro.capabilities.streaming);
        assert!(gpt_55_pro.capabilities.json_schema);
    }

    #[test]
    fn claude_haiku_vision_is_not_assumed() {
        let request = DriverModelResolveRequest::new("claude-3-5-haiku-20241022", vec![]);
        let inventory = resolve_driver_inventory(
            "claude-test",
            ProviderType::CloudApi,
            "claude",
            &[request],
            None,
        );
        let model = &inventory.models[0];
        assert!(model.capabilities.tool_call);
        assert!(model.capabilities.web_search);
        assert!(!model.capabilities.vision);
        assert!(!model.api_types.contains(&ApiType::VisionCaption));
        assert!(model
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.haiku"));
        assert!(model
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.anthropic.claude-3-5-haiku-20241022"));
        assert!(!model.logical_mounts.iter().any(|mount| mount == "llm"));
    }

    #[test]
    fn claude_family_mounts_do_not_include_role_paths() {
        let requests = vec![
            DriverModelResolveRequest::new("claude-opus-4-7", vec![]),
            DriverModelResolveRequest::new("claude-sonnet-4-6", vec![]),
            DriverModelResolveRequest::new("claude-haiku-4-5", vec![]),
        ];
        let inventory = resolve_driver_inventory(
            "claude-test",
            ProviderType::CloudApi,
            "claude",
            requests.as_slice(),
            None,
        );
        let by_id = |id: &str| {
            inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("model should exist")
        };
        let opus = by_id("claude-opus-4-7");
        assert!(opus.logical_mounts.iter().any(|mount| mount == "llm.opus"));
        assert!(opus
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.anthropic.claude-opus-4-7"));
        let opus_reasoning = by_id("claude-opus-4-7:reasoning-medium");
        assert!(opus_reasoning
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.opus.reasoning-medium"));
        assert!(opus_reasoning.provider_options.is_none());

        let sonnet = by_id("claude-sonnet-4-6");
        assert!(sonnet
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.sonnet"));

        let haiku = by_id("claude-haiku-4-5");
        assert!(haiku
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.haiku"));

        for model in inventory.models.iter() {
            assert!(!model.logical_mounts.iter().any(|mount| mount == "llm"));
            assert!(!model.logical_mounts.iter().any(|mount| mount == "llm.code"));
            assert!(!model
                .logical_mounts
                .iter()
                .any(|mount| mount == "llm.reason"));
        }
    }

    #[test]
    fn gemini_family_mounts_do_not_include_chat_role() {
        let requests = vec![
            DriverModelResolveRequest::new("gemini-2.5-pro", vec![]),
            DriverModelResolveRequest::new("gemini-2.5-flash", vec![]),
            DriverModelResolveRequest::new("gemini-2.5-flash-lite", vec![]),
            DriverModelResolveRequest::new("gemini-2.5-deepthink", vec![]),
            DriverModelResolveRequest::new("gemini-3.7-flash", vec![]),
        ];
        let inventory = resolve_driver_inventory(
            "gemini-test",
            ProviderType::CloudApi,
            "google-gemini",
            requests.as_slice(),
            None,
        );
        let by_id = |id: &str| {
            inventory
                .models
                .iter()
                .find(|model| model.provider_model_id == id)
                .expect("model should exist")
        };
        assert!(by_id("gemini-2.5-pro")
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gemini-pro"));
        assert!(by_id("gemini-2.5-flash")
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gemini-flash"));
        assert!(by_id("gemini-2.5-flash-lite")
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gemini-flash-lite"));
        assert!(by_id("gemini-2.5-deepthink")
            .logical_mounts
            .iter()
            .any(|mount| mount == "llm.gemini-deepthink"));
        for model in inventory.models.iter() {
            assert!(!model.logical_mounts.iter().any(|mount| mount == "llm"));
        }
        let gemini_25_flash = by_id("gemini-2.5-flash");
        assert!(gemini_25_flash.api_types.contains(&ApiType::AudioAsr));
        assert!(gemini_25_flash
            .logical_mounts
            .iter()
            .any(|mount| mount == "audio.asr"));
        assert_eq!(gemini_25_flash.capabilities.max_output_tokens, Some(65_536));
        assert!(!gemini_25_flash
            .capabilities
            .supports_feature_combination(&["tool_calling", "web_search"]));
        let gemini_25_deepthink = by_id("gemini-2.5-deepthink");
        assert!(!gemini_25_deepthink.api_types.contains(&ApiType::AudioAsr));
        assert_eq!(
            gemini_25_deepthink.capabilities.max_output_tokens,
            Some(65_536)
        );
        let gemini_37_flash = by_id("gemini-3.7-flash");
        assert!(gemini_37_flash.api_types.contains(&ApiType::AudioAsr));
        assert_eq!(gemini_37_flash.capabilities.max_output_tokens, Some(8_192));
        assert!(gemini_37_flash
            .capabilities
            .supports_feature_combination(&["tool_calling", "web_search"]));
    }

    #[test]
    fn semantic_family_mounts_cover_domestic_and_other_models() {
        let cases = [
            ("qwen2.5-coder-32b", "llm.qwen-coder"),
            ("qwen-max", "llm.qwen-max"),
            ("qwen-turbo", "llm.qwen-small"),
            ("deepseek-r1", "llm.deepseek-reasoner"),
            ("deepseek-v3", "llm.deepseek-pro"),
            ("kimi-k1-thinking", "llm.kimi-thinking"),
            ("kimi-latest", "llm.kimi"),
            ("glm-4-flash", "llm.glm-flash"),
            ("glm-4-plus", "llm.glm"),
            ("grok-mini", "llm.grok-fast"),
            ("grok-4-heavy", "llm.grok-heavy"),
        ];
        for (model, expected_mount) in cases {
            assert!(
                semantic_llm_family_mounts(model)
                    .iter()
                    .any(|mount| mount == expected_mount),
                "{} should mount to {}",
                model,
                expected_mount
            );
        }
    }

    #[test]
    fn pattern_exclude_drops_unsupported_openai_audio_realtime() {
        let request = DriverModelResolveRequest::new("gpt-4o-realtime-preview", vec![]);
        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[request],
            None,
        );
        assert!(inventory.models.is_empty());
    }

    #[test]
    fn pattern_exclude_drops_deprecated_openai_model_families() {
        for model in [
            "gpt-3.5-turbo",
            "gpt-4",
            "gpt-4-turbo",
            "gpt-4.1-nano",
            "gpt-4o-search-preview",
            "gpt-5.1-codex-mini",
            "o1-pro",
            "o3-mini",
            "o3-deep-research",
            "o4-mini",
        ] {
            let inventory = resolve_driver_inventory(
                "openai-test",
                ProviderType::CloudApi,
                "openai",
                &[DriverModelResolveRequest::new(model, vec![])],
                None,
            );
            assert!(inventory.models.is_empty(), "{model} should be excluded");
        }

        let inventory = resolve_driver_inventory(
            "openai-test",
            ProviderType::CloudApi,
            "openai",
            &[DriverModelResolveRequest::new("gpt-4.1", vec![])],
            None,
        );
        assert_eq!(inventory.models.len(), 1);
    }

    #[test]
    fn model_driver_matching_uses_exact_and_pattern_rules_not_defaults() {
        let candidates = vec!["openai".to_string(), "claude".to_string()];
        assert_eq!(
            match_model_driver("gpt-5.4", candidates.as_slice(), None).unwrap(),
            Some("openai".to_string())
        );
        assert_eq!(
            match_model_driver("claude-sonnet-4-5", candidates.as_slice(), None).unwrap(),
            Some("claude".to_string())
        );
        assert_eq!(
            match_model_driver("unknown-private-model", candidates.as_slice(), None).unwrap(),
            None
        );
    }

    #[test]
    fn explicit_mapping_selects_driver_and_preserves_provider_model_id() {
        let candidates = vec!["openai".to_string(), "claude".to_string()];
        let metadata = resolve_model_driver_metadata(
            "router-main",
            ProviderType::CloudApi,
            "anthropic/renamed-sonnet",
            "claude-sonnet-4-5",
            candidates.as_slice(),
            Some("claude"),
            vec![ApiType::Llm],
        )
        .unwrap()
        .unwrap();
        assert_eq!(metadata.provider_model_id, "anthropic/renamed-sonnet");
        assert_eq!(
            metadata.origin_model_id.as_deref(),
            Some("claude-sonnet-4-5")
        );
        assert_eq!(metadata.model_driver, "claude");
        assert_eq!(metadata.exact_model, "anthropic/renamed-sonnet@router-main");
    }
}
